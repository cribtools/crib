use crib::file::FileLocation;

use gannot::genome::{Error as GenomeError, GenomicRange};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

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
    input_files: Vec<FileLocation>,
}

async fn view(view_args: ViewArgs) -> anyhow::Result<()> {
    let stdout = std::io::stdout();
    let lock = stdout.lock();
    let writer = std::io::BufWriter::new(lock);
    let data_stream = crib::bbi::DataStream::try_open(view_args.input_files, view_args.location).await?;
    data_stream.bg_write(writer).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::WARN.into())
        .from_env_lossy();
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::View(view_args) => view(view_args).await,
    }?;
    Ok(())
}
