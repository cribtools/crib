use crib::{
    Error as CribError,
    bigwig::BigWigFile,
    file::FileLocation,
    object_store::presigned_urls,
};
use gannot::genome::{Error as GenomeError, GenomicRange};
use tracing_subscriber::EnvFilter;

use std::fs::File;
use futures_util::{StreamExt, stream::FuturesOrdered};
use clap::{Args, Parser, Subcommand};
use bigtools::utils::remote_file::RemoteFile;

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
    input_files: Vec<FileLocation>,
}

async fn view(select_args: ViewArgs) -> anyhow::Result<()> {
    let mut futures: FuturesOrdered<_> = select_args
        .input_files
        .into_iter()
        .map(|file| async {
            match file {
                FileLocation::Local(input_file) => {
                    let local_file = File::open(input_file)?;
                    Ok::<_, CribError>(vec![BigWigFile::Local(local_file)])
                }
                FileLocation::Http(url) => {
                    let remote_file = RemoteFile::new(url.as_str());
                    Ok(vec![BigWigFile::Remote(remote_file)])
                }
                FileLocation::S3(s3_url) => {
                    let presigned_urls = presigned_urls(&s3_url).await?;
                    let readers: Vec<_> = presigned_urls
                        .iter()
                        .map(|url| {
                            crib::bigwig::BigWigFile::Remote(RemoteFile::new(url.as_str()))
                        })
                        .collect();
                    Ok(readers)
                }
            }
        })
        .collect();

    let mut bw_files = Vec::new();
    while let Some(file_result) = futures.next().await {
        let file_result = file_result?;
        bw_files.extend(file_result);
    }

    let range = select_args.location.range_0halfopen();
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

    let stdout = std::io::stdout();
    let lock = stdout.lock();
    let writer = std::io::BufWriter::new(lock);

    crib::bigwig::bigwig_print_stream(writer, bw_files, select_args.location.seqid(), coord_start, coord_end).await?;

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (non_blocking, _guard) = tracing_appender::non_blocking(std::io::stderr());
    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::View(view_args) => view(view_args).await,
    }?;
    Ok(())
}
