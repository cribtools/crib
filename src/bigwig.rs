use crate::Error as CribError;
use bigtools::{BBIReadError, Value, utils::remote_file::RemoteFile};
use futures_util::{StreamExt, stream::FuturesOrdered};
use gannot::genome::SeqId;
use log::warn;
use std::sync::mpsc;

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
    rx: mpsc::Receiver<Result<Value, CribError>>,
}

impl BigWigSpawn {
    fn send_intervals(
        tx: mpsc::Sender<Result<Value, CribError>>,
        maybe_data_interval: Result<
            Result<impl Iterator<Item = Result<Value, BBIReadError>>, BBIReadError>,
            bigtools::BigWigReadOpenError,
        >,
    ) {
        if let Ok(Ok(data_interval)) = maybe_data_interval {
            for value in data_interval {
                let result = tx.send(value.map_err(|err| err.into()));
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
            let result = tx.send(Err(error));
            if result.is_err() {
                warn!("Message channel for a bigWigThread ended early.");
            }
        }
    }

    fn new(file: BigWigFile, seqid: &SeqId, coord_start: u32, coord_end: u32) -> BigWigSpawn {
        let (tx, rx) = mpsc::channel::<Result<Value, CribError>>();
        let seqid = seqid.clone();
        std::thread::spawn(move || {
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
                        let result = tx.send(value.map_err(|err| err.into()));
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
struct DataSlide {
    thread: BigWigSpawn,
    latest_value: Option<Value>,
}

impl Iterator for DataSlide {
    type Item = Result<Value, CribError>;

    fn next(&mut self) -> Option<Self::Item> {
        let next_value = self.thread.rx.iter().next().map(|value| match value {
            Ok(value) => {
                if value.start > value.end
                    || self
                        .latest_value
                        .is_some_and(|prev_value| prev_value.start >= value.start)
                {
                    Err(CribError::InvalidDataFormat(format!(
                        "start > end ({} > {}) in bigWig",
                        value.start, value.end
                    )))
                } else {
                    Ok(value)
                }
            }
            Err(err) => Err(err),
        });
        if let Some(Ok(value)) = next_value {
            self.latest_value = Some(value);
        } else {
            self.latest_value = None;
        }
        next_value
    }
}

impl DataSlide {
    fn new(thread: BigWigSpawn) -> Result<DataSlide, CribError> {
        let mut data_slide = DataSlide {
            thread,
            latest_value: None,
        };
        if let Some(Err(err)) = data_slide.next() {
            Err(err)
        } else {
            Ok(data_slide)
        }
    }

    fn next_start(&self, coord: Option<u32>) -> Option<u32> {
        match coord {
            None => self.latest_value.map(|v| v.start),
            Some(coord) => self.latest_value.and_then(|v| {
                if coord < v.start {
                    Some(v.start)
                } else if coord < v.end {
                    Some(coord + 1)
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
                    Err(CribError::InternalError(
                        "unexpected order traversing bigWig".to_string(),
                    ))
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

    fn advance_towards(&mut self, upto: u32) -> Result<(), CribError> {
        match self.latest_value {
            None => Ok(()),
            Some(current_value) => {
                if upto >= current_value.end {
                    match self.next() {
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

async fn data_slider_print<W: std::io::Write>(
    mut writer: W,
    seqid: &SeqId,
    data_sliders: &mut [DataSlide],
) -> Result<(), CribError> {
    let mut maybe_start = data_sliders
        .iter()
        .map(|ds| ds.next_start(None))
        .filter(|start| start.is_some())
        .min()
        .flatten();

    while let Some(start) = maybe_start {
        let end: Result<Vec<_>, _> = data_sliders.iter().map(|ds| ds.next_end(start)).collect();
        let end = end?.into_iter().filter(|end| end.is_some()).min().flatten();

        let end = end.ok_or(CribError::InvalidDataFormat(
            "unexpected missing end coordinate".to_string(),
        ))?;
        write!(writer, "{}\t{}\t{}", seqid, start, end)?;
        for ds in data_sliders.iter_mut() {
            if let Some(value) = ds.value_at(start) {
                write!(writer, "\t{}", value)?;
            } else {
                write!(writer, "\t.")?;
            }
            ds.advance_towards(end)?;
        }
        writeln!(writer)?;
        maybe_start = data_sliders
            .iter()
            .map(|ds| ds.next_start(Some(end - 1)))
            .filter(|start| start.is_some())
            .min()
            .flatten();
    }
    Ok(())
}

pub async fn bigwig_print(
    files: Vec<BigWigFile>,
    seqid: &SeqId,
    coord_start: u32,
    coord_end: u32,
) -> Result<(), CribError> {
    let mut futures: FuturesOrdered<_> = files
        .into_iter()
        .map(|file| {
            let seqid = seqid.clone();
            tokio::task::spawn_blocking(move || {
                let thread = BigWigSpawn::new(file, &seqid, coord_start, coord_end);
                DataSlide::new(thread)
            })
        })
        .collect();

    let mut data_sliders = Vec::new();
    while let Some(data_slider) = futures.next().await {
        let data_slider = data_slider.unwrap();
        data_sliders.push(data_slider);
    }
    if data_sliders.iter().all(|ds| ds.is_ok()) {
        let mut data_sliders: Vec<_> = data_sliders.into_iter().map(|ds| ds.unwrap()).collect();
        let stdout = std::io::stdout();
        let lock = stdout.lock();
        let mut writer = std::io::BufWriter::new(lock);
        data_slider_print(&mut writer, seqid, &mut data_sliders).await?;
        Ok(())
    } else {
        
        Err(CribError::IoError("errors accessing file(s)".to_string()))
    }

}

#[cfg(test)]
mod tests;
