const CHANNEL_BUFFER_SIZE: usize = 10_000_000;
// an unbounded channel gives slightly better performance

use crate::{Error as CribError, file::FileLocation, object_store::presigned_urls};
use bigtools::{utils::remote_file::RemoteFile, BBIReadError, BigWigReadOpenError, Value};
use futures_util::{FutureExt, Stream, StreamExt, stream::FuturesOrdered};
use gannot::genome::{GenomicRange, SeqId};
use std::{
    pin::Pin, task::{Context, Poll}
};
use tracing::warn;

impl From<BBIReadError> for CribError {
    fn from(value: BBIReadError) -> Self {
        CribError::IoError(value.to_string())
    }
}

impl From<BigWigReadOpenError> for CribError {
    fn from(_: BigWigReadOpenError) -> Self {
        CribError::IoError("could not open remote file".to_string())
    }
}

trait BbiFile: Send + 'static {
    fn get_intervals(
        self,
        seqid: &SeqId,
        coord_start: u32,
        coord_end: u32,
    ) -> Result<Box<dyn Iterator<Item = Result<Value, BBIReadError>>>, CribError>;
}

trait BbiFileLocation {
    async fn open(self) -> Result<Vec<impl BbiFile>, CribError>;
}

impl BbiFileLocation for FileLocation {
    async fn open(self) -> Result<Vec<impl BbiFile>, CribError> {
        match self {
            FileLocation::Local(input_file) => {
                let local_file = std::fs::File::open(input_file)?;
                Ok::<_, CribError>(vec![BigWigFile::Local(local_file)])
            }
            FileLocation::Http(url) => {
                // bigtools RemoteFile::new(url) does not access the network
                let remote_file = RemoteFile::new(url.as_str());
                Ok(vec![BigWigFile::Remote(remote_file)])
            }
            FileLocation::S3(s3_url) => {
                let presigned_urls = presigned_urls(&s3_url).await?;
                let readers: Vec<_> = presigned_urls
                    .iter()
                    // bigtools RemoteFile::new(url) does not access the network
                    .map(|url| BigWigFile::Remote(RemoteFile::new(url.as_str())))
                    .collect();
                Ok(readers)
            }
    }
}
}

#[non_exhaustive]
enum BigWigFile {
    Local(std::fs::File),
    Remote(RemoteFile),
}

impl BbiFile for BigWigFile {
    fn get_intervals(
        self,
        seqid: &SeqId,
        coord_start: u32,
        coord_end: u32,
    ) -> Result<Box<dyn Iterator<Item = Result<Value, BBIReadError>>>, CribError>
    {
        match self {
            BigWigFile::Local(file) => {
                let bw_file = bigtools::BigWigRead::open(file)?;
                let iter = Box::new(bw_file.get_interval_move(seqid.as_str(), coord_start, coord_end)?);
                Ok(iter)
            },
            BigWigFile::Remote(remote_file) => {
                let bw_file = bigtools::BigWigRead::open(remote_file)?;
                let iter = Box::new(bw_file.get_interval_move(seqid.as_str(), coord_start, coord_end)?);
                Ok(iter)
            }
        }
    }
}

struct BbiThread {
    rx: tokio::sync::mpsc::Receiver<Result<Value, CribError>>,
}

impl BbiThread {
    fn send_intervals(
        tx: tokio::sync::mpsc::Sender<Result<Value, CribError>>,
        maybe_data_interval: 
        Result<Box<dyn Iterator<Item = Result<Value, BBIReadError>>>, CribError>
    ) {
        match maybe_data_interval {
            Ok(data_interval) => {
                for value in data_interval {
                    let result = tx.blocking_send(value.map_err(|err| err.into()));
                    if result.is_err() {
                        warn!("Message channel for a bigWigThread ended early.");
                        break;
                    }
                }
            },
            Err(err) => {
                let result = tx.blocking_send(Err(err));
                if result.is_err() {
                    warn!("Message channel for a bigWigThread ended early.");
                }
            },
        }
    }

    fn new(file: impl BbiFile + 'static, seqid: &SeqId, coord_start: u32, coord_end: u32) -> BbiThread {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Value, CribError>>(CHANNEL_BUFFER_SIZE);
        let seqid = seqid.clone();

        tokio::task::spawn_blocking(move || {
            let maybe_data_interval = file.get_intervals(&seqid, coord_start, coord_end);
            Self::send_intervals(tx, maybe_data_interval);
        });

        BbiThread { rx }
    }

}

struct BbiDataSlider {
    thread: BbiThread,
    latest_value: Option<Value>,
    finished: bool,
}

impl Stream for BbiDataSlider {
    type Item = Result<Value, CribError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        match self.thread.rx.poll_recv(cx) {
            Poll::Ready(None) => {
                self.finished = true;
                self.latest_value = None;
                Poll::Ready(None)
            }
            Poll::Pending => {
                // poll_recv has arranged for the waker to be notified
                Poll::Pending
            }
            Poll::Ready(Some(maybe_value)) => {
                let value = match maybe_value {
                    Ok(value) => {
                        if value.start > value.end {
                            Err(CribError::InvalidDataFormat(format!(
                                "start > end ({} > {}) in bigWig",
                                value.start, value.end
                            )))
                        } else if let Some(prev_value) = self.latest_value
                            && prev_value.end > value.start
                        {
                            Err(CribError::InvalidDataFormat(format!(
                                "overlapping intervals in bigWig ({}, {}) and ({}, {})",
                                prev_value.start, prev_value.end, value.start, value.end
                            )))
                        } else {
                            Ok(value)
                        }
                    }
                    Err(err) => Err(err),
                };
                if let Ok(value) = value {
                    self.latest_value = Some(value);
                } else {
                    // if there is an error, this will be the last value
                    self.latest_value = None;
                    self.finished = true;
                }
                Poll::Ready(Some(value))
            }
        }
    }
}

impl BbiDataSlider {
    async fn new(thread: BbiThread) -> Result<BbiDataSlider, CribError> {
        let mut data_slide = BbiDataSlider {
            thread,
            latest_value: None,
            finished: false,
        };
        if let Some(Err(err)) = data_slide.next().await {
            Err(err)
        } else {
            Ok(data_slide)
        }
    }

    fn next_start(&self, coord: Option<u32>) -> Option<u32> {
        match coord {
            None => self.latest_value.map(|v| v.start),
            Some(coord) => self.latest_value.and_then(|v| {
                let coord = coord + 1;
                if coord < v.start {
                    Some(v.start)
                } else if coord < v.end {
                    Some(coord)
                } else {
                    None
                }
            }),
        }
    }

    fn next_end(&self, start: u32) -> Result<Option<u32>, CribError> {
        match self.latest_value {
            None => Ok(None),
            Some(v) => {
                if start < v.start {
                    Ok(Some(v.start))
                } else if start < v.end {
                    Ok(Some(v.end))
                } else {
                    Err(CribError::InternalError(format!(
                        "looking in bigWig for next end from start ({}), but got {}",
                        start, v.end
                    )))
                }
            }
        }
    }

    fn value_at(&self, at: u32) -> Option<f32> {
        if let Some(v) = self.latest_value {
            if v.start <= at && v.end > at {
                Some(v.value)
            } else {
                None
            }
        } else {
            None
        }
    }

    async fn advance_towards(&mut self, upto: u32) -> Result<(), CribError> {
        match self.latest_value {
            None => Ok(()),
            Some(current_value) => {
                if upto >= current_value.end {
                    match self.next().await {
                        None => Ok(()),
                        Some(Ok(_)) => Ok(()),
                        Some(Err(err)) => Err(err),
                    }
                } else {
                    Ok(())
                }
            }
        }
    }
}

enum BbiMuxState {
    Start,
    Advancing(usize, u32), // bigtools uses u32 for coordinates
}

/// Multiplexes iteration over BBI files within a single genomic range (single seqid)
struct BbiMux {
    data_sliders: Vec<BbiDataSlider>,
    start: Option<u32>, // start must be None if data_sliders has length of 0
    state: BbiMuxState,
}

impl BbiMux {
    async fn try_open(
        input_files: Vec<impl BbiFileLocation>,
        location: GenomicRange,
    ) -> Result<BbiMux, CribError> {
        let range = location.range_0halfopen();
        let coord_start: u32 = range.start.try_into().map_err(|_| {
            CribError::NotSupported(
                "range values greater than u32 not supported for bigWig".to_string(),
            )
        })?;
        let coord_end: u32 = range.end.try_into().map_err(|_| {
            CribError::NotSupported(
                "range values greater than u32 not supported for bigWig".to_string(),
            )
        })?;

        let mut futures: FuturesOrdered<_> = input_files
            .into_iter()
            .map(|file| file.open())
            .collect();

        let mut next_futures = FuturesOrdered::new();
        let seqid = location.seqid();
        while let Some(file_result) = futures.next().await {
            let file_result = file_result?;
            let new_sliders: Vec<_> = file_result
                .into_iter()
                .map(|file| {
                    let seqid = seqid.clone();
                    tokio::task::spawn_blocking(move || {
                        let thread = BbiThread::new(file, &seqid, coord_start, coord_end);
                        BbiDataSlider::new(thread)
                    })
                })
                .collect();
            next_futures.extend(new_sliders);
        }

        let mut data_sliders = Vec::new();
        while let Some(data_slider) = next_futures.next().await {
            let data_slider = data_slider.unwrap().await?;
            data_sliders.push(data_slider);
        }

        let start = data_sliders
            .iter()
            .map(|ds| ds.next_start(None))
            .filter(|start| start.is_some())
            .min()
            .flatten();

        Ok(BbiMux {
            data_sliders,
            start,
            state: BbiMuxState::Start,
        })
    }
}

impl Stream for BbiMux {
    type Item = Result<(u32, u32, Vec<Option<f32>>), CribError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // assumes start is None if data_sliders has length of 0
        while let Some(start) = self.start {
            match self.state {
                BbiMuxState::Start => {
                    let end: Result<Vec<_>, _> = self
                        .data_sliders
                        .iter()
                        .map(|ds| ds.next_end(start))
                        .collect();
                    let end = end?.into_iter().filter(|end| end.is_some()).min().flatten();
                    let end = end.ok_or(CribError::InvalidDataFormat(
                        "unexpected missing end coordinate".to_string(),
                    ))?;

                    let values: Vec<_> = self
                        .data_sliders
                        .iter_mut()
                        .map(|ds| ds.value_at(start))
                        .collect();
                    self.state = BbiMuxState::Advancing(0, end);
                    return Poll::Ready(Some(Ok((start, end, values))));
                }
                BbiMuxState::Advancing(index, end) => {
                    let mut i = index;
                    for ds in self.data_sliders[index..].iter_mut() {
                        let mut future = Box::pin(ds.advance_towards(end));
                        match future.poll_unpin(cx) {
                            Poll::Ready(Ok(_)) => i += 1,
                            Poll::Pending => break,
                            Poll::Ready(Err(err)) => return Poll::Ready(Some(Err(err))),
                        }
                    }
                    if i < self.data_sliders.len() {
                        self.state = BbiMuxState::Advancing(i, end);
                        // waker will be notified within data_slider that returned Poll::Pending
                        return Poll::Pending;
                    } else {
                        self.start = self
                            .data_sliders
                            .iter()
                            .map(|ds| ds.next_start(Some(end - 1)))
                            .filter(|start| start.is_some())
                            .min()
                            .flatten();
                        self.state = BbiMuxState::Start;
                    }
                }
            }
        }
        std::task::Poll::Ready(None)
    }
}

/// Provides streaming access to one or more BBI files
pub struct DataStream {
    seqid: SeqId,
    bbi_mux: BbiMux,
}

impl DataStream {
    pub async fn try_open(
        input_files: Vec<FileLocation>,
        location: GenomicRange,
    ) -> Result<DataStream, CribError> {
        let seqid = location.seqid().clone();
        let bbi_mux = BbiMux::try_open(input_files, location).await?;
        Ok(DataStream { seqid, bbi_mux })
    }

    pub async fn bg_write<W: std::io::Write>(mut self, mut writer: W) -> Result<(), CribError> {
        while let Some(data_interval) = self.bbi_mux.next().await {
            let (start, end, data_interval) = data_interval?;
            write!(writer, "{}\t{}\t{}", self.seqid, start, end)?;
            for value in data_interval {
                if let Some(value) = value {
                    write!(writer, "\t{}", value)?;
                } else {
                    write!(writer, "\t.")?;
                }
            }
            writeln!(writer)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;