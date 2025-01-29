use bigtools::utils::remote_file::RemoteFile;
use crib::{bigwig::bigwig_print, file::{FileLocation, presigned_url}, Error as CribError};
use gannot::genome::{Error as GenomeError, GenomicRange};
use std::fs::File;

use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    View(ViewArgs),
}

fn genomic_range(param: &str) -> Result<GenomicRange, GenomeError> {
    param.try_into()
}

fn file_location(param: &str) -> Result<FileLocation, GenomeError> {
    Ok(param.into())
}

#[derive(Args)]
struct ViewArgs {
    #[arg(value_parser = genomic_range)]
    location: GenomicRange,
    #[clap(value_parser = file_location)]
    input_file: FileLocation,
}

fn view(select_args: &ViewArgs) -> anyhow::Result<()> {
    match &select_args.input_file {
        FileLocation::Local(input_file) => {
            let file = File::open(input_file)?;
            let mut reader = bigtools::BigWigRead::open(file)?;
            let range = select_args.location.range_0halfopen();
            let coord_start = range.start.try_into()?;
            let coord_end = range.end.try_into()?;
            bigwig_print(
                &mut reader,
                select_args.location.seqid().to_string(),
                coord_start,
                coord_end,
            );
            Ok(())
        }
        FileLocation::Http(url) => {
            let remote_file = RemoteFile::new(url.as_str());
            let mut reader = bigtools::BigWigRead::open(remote_file)?;
            let range = select_args.location.range_0halfopen();
            let coord_start = range.start.try_into()?;
            let coord_end = range.end.try_into()?;
            bigwig_print(
                &mut reader,
                select_args.location.seqid().to_string(),
                coord_start,
                coord_end,
            );
            Ok(())
        }
        FileLocation::S3(s3_url) => {
            let url = presigned_url(s3_url)?;

            let remote_file = RemoteFile::new(url.as_str());
            let mut reader = bigtools::BigWigRead::open(remote_file)
                .map_err(|_| CribError::IoError("could not open remote file".to_string()))?;
            let range = select_args.location.range_0halfopen();
            let coord_start = range.start.try_into()?;
            let coord_end = range.end.try_into()?;
            bigwig_print(
                &mut reader,
                select_args.location.seqid().to_string(),
                coord_start,
                coord_end,
            );
            Ok(())
        }
    }
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match &cli.command {
        Commands::View(view_args) => view(view_args),
    }?;
    Ok(())
}
