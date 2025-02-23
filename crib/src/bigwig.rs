const CHANNEL_BUFFER_SIZE: usize = 100_000_000;

use crate::Error as CribError;
use bigtools::{BBIReadError, Value, utils::remote_file::RemoteFile};
use futures_util::{Stream, StreamExt, TryFutureExt, stream::FuturesOrdered};
use gannot::{
    format::DataInterval,
    genome::{GenomicRange, SeqId},
};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tracing::{trace, warn};

#[non_exhaustive]
pub enum BigWigFile {
    Local(std::fs::File),
    Remote(RemoteFile),
    Iter(BBIIter),
}

pub struct BBIIter {
    inner: std::vec::IntoIter<Value>,
}

impl BBIIter {
    fn new(inner: std::vec::IntoIter<Value>) -> BBIIter {
        BBIIter { inner }
    }
}

impl Iterator for BBIIter {
    type Item = Result<Value, BBIReadError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(Ok)
    }
}

impl From<Vec<(u32, u32, f32)>> for BBIIter {
    fn from(val: Vec<(u32, u32, f32)>) -> Self {
        let data: Vec<_> = val
            .into_iter()
            .map(|(start, end, value)| Value { start, end, value })
            .collect();
        BBIIter::new(data.into_iter())
    }
}

struct BigWigSpawn {
    rx: tokio::sync::mpsc::Receiver<Result<Value, CribError>>,
}

impl BigWigSpawn {
    fn send_intervals(
        tx: tokio::sync::mpsc::Sender<Result<Value, CribError>>,
        maybe_data_interval: Result<
            Result<impl Iterator<Item = Result<Value, BBIReadError>>, BBIReadError>,
            bigtools::BigWigReadOpenError,
        >,
    ) {
        if let Ok(Ok(data_interval)) = maybe_data_interval {
            for value in data_interval {
                let result = tx.blocking_send(value.map_err(|err| err.into()));
                if result.is_err() {
                    warn!("Message channel for a bigWigThread ended early.");
                    break;
                }
            }
        } else {
            let error: CribError = match maybe_data_interval {
                Ok(Err(err)) => err.into(),
                Err(err) => err.into(),
                _ => CribError::InternalError("unexpected result".to_string()),
            };
            let result = tx.blocking_send(Err(error));
            if result.is_err() {
                warn!("Message channel for a bigWigThread ended early.");
            }
        }
    }

    async fn new(
        file: BigWigFile,
        seqid: &SeqId,
        coord_start: u32,
        coord_end: u32,
    ) -> BigWigSpawn {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Value, CribError>>(CHANNEL_BUFFER_SIZE);
        let seqid = seqid.clone();

        tokio::task::spawn_blocking(move || {
            match file {
                BigWigFile::Local(file) => {
                    let maybe_data_interval = bigtools::BigWigRead::open(file).map(|reader| {
                        reader.get_interval_move(&seqid.to_string(), coord_start, coord_end)
                    });
                    Self::send_intervals(tx, maybe_data_interval);
                }
                BigWigFile::Remote(remote_file) => {
                    let maybe_data_interval =
                        bigtools::BigWigRead::open(remote_file).map(|reader| {
                            reader.get_interval_move(&seqid.to_string(), coord_start, coord_end)
                        });
                    Self::send_intervals(tx, maybe_data_interval);
                }
                BigWigFile::Iter(iterator) => {
                    for value in iterator {
                        let result = tx.blocking_send(value.map_err(|err| err.into()));
                        if result.is_err() {
                            warn!("Message channel for a bigWigThread ended early.");
                            break;
                        }
                    }
                }
            };
        });
        BigWigSpawn { rx }
    }
}

struct DataSlider {
    thread: BigWigSpawn,
    latest_value: Option<Value>,
    finished: bool,
}

impl Stream for DataSlider {
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
                    trace!("Got value {:?}", value);
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

impl DataSlider {
    async fn new(thread: BigWigSpawn) -> Result<DataSlider, CribError> {
        let mut data_slide = DataSlider {
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

enum DataSliderMuxState {
    Start,
    Advancing(usize, u32), // bigtools only supports u32 for coordinates
}

struct DataSliderMux<'ds> {
    seqid: &'ds SeqId, // assumes all DataSliders are from the same seqid
    data_sliders: &'ds mut [DataSlider],
    start: Option<u32>, // start must be None if data_sliders has length of 0
    state: DataSliderMuxState,
}

impl<'ds> DataSliderMux<'ds> {
    fn new(seqid: &'ds SeqId, data_sliders: &'ds mut [DataSlider]) -> DataSliderMux<'ds> {
        let start = data_sliders
            .iter()
            .map(|ds| ds.next_start(None))
            .filter(|start| start.is_some())
            .min()
            .flatten();

        DataSliderMux {
            seqid,
            data_sliders,
            start,
            state: DataSliderMuxState::Start,
        }
    }
}

impl Stream for DataSliderMux<'_> {
    type Item = Result<DataInterval<f32>, CribError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        while let Some(start) = self.start {
            match self.state {
                DataSliderMuxState::Start => {
                    let end: Result<Vec<_>, _> = self
                        .data_sliders
                        .iter()
                        .map(|ds| ds.next_end(start))
                        .collect();
                    let end = end?.into_iter().filter(|end| end.is_some()).min().flatten();
                    let end = end.ok_or(CribError::InvalidDataFormat(
                        "unexpected missing end coordinate".to_string(),
                    ))?;
                    let range = start as u64..end as u64;
                    let genomic_range =
                        GenomicRange::from_0halfopen(self.seqid.to_owned(), range).unwrap();
                    let mut values = Vec::new();
                    for ds in self.data_sliders.iter_mut() {
                        if let Some(value) = ds.value_at(start) {
                            values.push(Some(value));
                        } else {
                            values.push(None);
                        }
                    }
                    assert!(!self.data_sliders.is_empty());
                    self.state = DataSliderMuxState::Advancing(0, end);
                    let data_interval = DataInterval::new(genomic_range, values);
                    return Poll::Ready(Some(Ok(data_interval)));
                }
                DataSliderMuxState::Advancing(index, end) => {
                    let mut i = index;
                    for ds in self.data_sliders[index..].iter_mut() {
                        let mut future = Box::pin(ds.advance_towards(end));
                        match future.try_poll_unpin(cx) {
                            Poll::Ready(Err(err)) => return Poll::Ready(Some(Err(err))),
                            Poll::Ready(Ok(_)) => i += 1,
                            Poll::Pending => break,
                        }
                    }
                    if i < self.data_sliders.len() {
                        self.state = DataSliderMuxState::Advancing(i, end);
                        // waker will be notified within each DataslideStream that returned Poll::Pending
                        return Poll::Pending;
                    } else {
                        self.start = self
                            .data_sliders
                            .iter()
                            .map(|ds| ds.next_start(Some(end - 1)))
                            .filter(|start| start.is_some())
                            .min()
                            .flatten();
                        self.state = DataSliderMuxState::Start;
                    }
                }
            }
        }
        std::task::Poll::Ready(None)
    }
}

pub async fn bigwig_print_stream<W: std::io::Write>(
    mut writer: W,
    files: Vec<BigWigFile>,
    seqid: &SeqId,
    coord_start: u32,
    coord_end: u32,
) -> Result<(), CribError> {
    let mut futures: FuturesOrdered<_> = files
        .into_iter()
        .map(|file| {
            let seqid = seqid.clone();
            tokio::spawn(async move {
                let thread = BigWigSpawn::new(file, &seqid, coord_start, coord_end).await;
                DataSlider::new(thread).await
            })
        })
        .collect();

    let mut data_sliders = Vec::new();
    while let Some(data_slider) = futures.next().await {
        let data_slider = data_slider.unwrap()?;
        data_sliders.push(data_slider);
    }

    let mut data_stream = DataSliderMux::new(seqid, &mut data_sliders);
    while let Some(data_interval) = data_stream.next().await {
        let data_interval = data_interval?;
        let genomic_range = data_interval.range();
        let range = genomic_range.range_0halfopen();
        write!(
            writer,
            "{}\t{}\t{}",
            genomic_range.seqid(),
            range.start,
            range.end
        )?;
        for value in data_interval.values() {
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

#[cfg(test)]
mod tests;
