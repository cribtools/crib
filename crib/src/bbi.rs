const CHANNEL_BUFFER_SIZE: usize = 100_000;

use crate::{Error as CribError, file::FileLocation, object_store::presigned_urls};
use bigtools::{BBIReadError, BigWigReadOpenError, Value, utils::remote_file::RemoteFile};
use futures_util::{Stream, StreamExt, stream::FuturesOrdered};
use gannot::genome::{GenomicRange, SeqId};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tracing::warn;

type IntervalData = (u32, u32, Vec<Option<f32>>);

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

// helper function for BbiFile
fn send_intervals(
    tx: tokio::sync::mpsc::Sender<Result<Value, CribError>>,
    maybe_data_interval: Result<Box<dyn Iterator<Item = Result<Value, BBIReadError>>>, CribError>,
) {
    match maybe_data_interval {
        Ok(data_interval) => {
            for value in data_interval {
                let value = value.map_err(|err| err.into());
                if let Err(tokio::sync::mpsc::error::SendError(err)) = tx.blocking_send(value) {
                    if let Err(crib_err) = err {
                        warn!(
                            "Message channel for a bigWigThread ended early. {}",
                            crib_err
                        );
                    } else {
                        warn!("Message channel for a bigWigThread ended early.");
                    }
                    break;
                }
            }
        }
        Err(err) => {
            if let Err(tokio::sync::mpsc::error::SendError(err)) = tx.blocking_send(Err(err)) {
                if let Err(crib_err) = err {
                    warn!(
                        "Message channel for a bigWigThread ended early. {}",
                        crib_err
                    );
                } else {
                    warn!("Message channel for a bigWigThread ended early.");
                }
            }
        }
    }
}

trait BbiFile: Sized + Send + 'static {
    fn spawn_into_receiver(
        self,
        seqid: SeqId,
        coord_start: u32,
        coord_end: u32,
    ) -> tokio::sync::mpsc::Receiver<Result<Value, CribError>> {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Value, CribError>>(CHANNEL_BUFFER_SIZE);
        let seqid = seqid.clone();

        tokio::task::spawn_blocking(move || {
            let maybe_data_interval = self.get_intervals(&seqid, coord_start, coord_end);
            send_intervals(tx, maybe_data_interval);
        });
        rx
    }

    fn get_intervals(
        self,
        seqid: &SeqId,
        coord_start: u32,
        coord_end: u32,
    ) -> Result<Box<dyn Iterator<Item = Result<Value, BBIReadError>>>, CribError>;
}

trait BbiFileLocation {
    async fn open(self) -> Result<impl IntoIterator<Item = impl BbiFile>, CribError>;
}

impl BbiFileLocation for FileLocation {
    async fn open(self) -> Result<impl IntoIterator<Item = impl BbiFile>, CribError> {
        match self {
            FileLocation::Local(input_file) => {
                // blocking and not spawning a thread as opening a local file is presumably quite fast
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
    // this is blocking and is meant to be called by spawn_into_stream
    fn get_intervals(
        self,
        seqid: &SeqId,
        coord_start: u32,
        coord_end: u32,
    ) -> Result<Box<dyn Iterator<Item = Result<Value, BBIReadError>>>, CribError> {
        match self {
            BigWigFile::Local(file) => {
                let bw_file = bigtools::BigWigRead::open(file)?;
                let iter =
                    Box::new(bw_file.get_interval_move(seqid.as_str(), coord_start, coord_end)?);
                Ok(iter)
            }
            BigWigFile::Remote(remote_file) => {
                let bw_file = bigtools::BigWigRead::open(remote_file)?;
                let iter =
                    Box::new(bw_file.get_interval_move(seqid.as_str(), coord_start, coord_end)?);
                Ok(iter)
            }
        }
    }
}

struct BbiDataSlider {
    rx: tokio::sync::mpsc::Receiver<Result<Value, CribError>>,
    _name: String,
    latest_value: Option<Value>,
}

impl Stream for BbiDataSlider {
    type Item = Result<Value, CribError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(None) => {
                self.latest_value = None;
                Poll::Ready(None)
            }
            Poll::Pending => {
                // the underlying receiver has arranged for the waker to be notified
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
                    self.latest_value = None;
                }
                Poll::Ready(Some(value))
            }
        }
    }
}

impl BbiDataSlider {
    async fn new<T: BbiFile>(
        bw_file: T,
        name: String,
        seqid: SeqId,
        coord_start: u32,
        coord_end: u32,
    ) -> Result<BbiDataSlider, CribError> {
        let rx = bw_file.spawn_into_receiver(seqid, coord_start, coord_end);

        let mut data_slide = BbiDataSlider {
            rx,
            _name: name,
            latest_value: None,
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

    // this can return None when there is nothing to fetch less than upto
    fn gated_poll(
        &mut self,
        upto: u32,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Value, CribError>>> {
        if self
            .latest_value
            .is_some_and(|latest_value| upto >= latest_value.end)
        {
            self.poll_next_unpin(cx)
        } else {
            Poll::Ready(None)
        }
    }
}

enum BbiMuxState {
    Init,
    GetStart,
    AdvanceInStep(usize, u32), // bigtools uses u32 for coordinates
    Finished,
}

/// Multiplexes iteration over BBI files within a single genomic range (single seqid)
struct BbiMux {
    data_sliders: Vec<BbiDataSlider>,
    start: Option<u32>, // start must be None if data_sliders has length of 0
    state: BbiMuxState,
}

impl BbiMux {
    async fn try_open(
        input_files: impl IntoIterator<Item = impl BbiFileLocation>,
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

        let mut futures: FuturesOrdered<_> =
            input_files.into_iter().map(|file| file.open()).collect();

        let mut next_futures = FuturesOrdered::new();
        let seqid = location.seqid();
        let mut file_count = 1;
        while let Some(file_result) = futures.next().await {
            let file_result = file_result?;
            let new_sliders: Vec<_> = file_result
                .into_iter()
                .map(|file| {
                    let name = file_count.to_string();
                    file_count += 1;
                    let seqid = seqid.clone();
                    BbiDataSlider::new(file, name, seqid, coord_start, coord_end)
                })
                .collect();
            next_futures.extend(new_sliders);
        }

        let mut data_sliders = Vec::new();
        while let Some(data_slider) = next_futures.next().await {
            data_sliders.push(data_slider?);
        }

        Ok(BbiMux {
            data_sliders,
            start: None,
            state: BbiMuxState::Init,
        })
    }
}

impl Stream for BbiMux {
    type Item = Result<IntervalData, CribError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.state {
                BbiMuxState::Init => {
                    let start = self
                        .data_sliders
                        .iter()
                        .map(|ds| ds.next_start(None))
                        .filter(|start| start.is_some())
                        .min()
                        .flatten();
                    self.start = start;
                    self.state = BbiMuxState::GetStart;
                }
                BbiMuxState::GetStart => {
                    if let Some(start) = self.start {
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
                        self.state = BbiMuxState::AdvanceInStep(0, end);
                        return Poll::Ready(Some(Ok((start, end, values))));
                    } else {
                        self.state = BbiMuxState::Finished;
                        break;
                    }
                }
                BbiMuxState::AdvanceInStep(index, end) => {
                    let mut i = index;

                    for ds in self.data_sliders[index..].iter_mut() {
                        match ds.gated_poll(end, cx) {
                            Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                            Poll::Ready(_) => i += 1,
                            Poll::Pending => break,
                        }
                    }
                    if i < self.data_sliders.len() {
                        self.state = BbiMuxState::AdvanceInStep(i, end);
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
                        self.state = BbiMuxState::GetStart;
                    }
                }
                BbiMuxState::Finished => break,
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
        input_files: impl IntoIterator<Item = FileLocation>,
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
