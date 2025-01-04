use std::io::BufWriter;
use bigtools::BigWigRead;

pub fn bigwig_print<R: std::io::Read + std::io::Seek>(reader: &mut BigWigRead<R>, seqid: String, coord_start: u32, coord_end: u32) {
    use std::io::Write;
    let stdout = std::io::stdout();
    let lock = stdout.lock();
    let mut writer = BufWriter::new(lock);
    let region = reader.get_interval(&seqid, coord_start, coord_end).unwrap();
    for interval in region {
        let interval = interval.expect("interval to contain a value");
        if interval.end - interval.start > 0 {
            writeln!(writer, "{}\t{}\t{}\t{}", seqid, interval.start, interval.end, interval.value).unwrap();
        }
    }
} 