use std::ffi::OsString;
use clap::{Parser};
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::process::exit;
use tokio::fs::{File, create_dir};
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, BufReader, BufWriter, AsyncWriteExt, AsyncReadExt};
use tokio::task::{JoinSet};

type MyResult = Result<(), std::io::Error>;

const BUF_SIZE: usize = 1024;

const NEWLINE_LEN: usize = 1;

const BOM: [u8; 3] = [0xEF, 0xBB, 0xBF];

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct CsvUtil {
    /// Input csv file path
    input_path: String,

    /// Each file size
    #[arg(short, long)]
    #[clap(default_value_t = 100000)]
    file_lines: usize,

    /// Output csv file path
    #[arg(short, long)]
    #[clap(default_value = "./output")]
    output_path: String,

    /// How many lines for header
    #[arg(long)]
    #[clap(default_value_t = 1)]
    header: u8,

    /// Turn debugging info on
    #[arg(short, long, action = clap::ArgAction::Count)]
    debug: u8,
}

#[tokio::main]
async fn main() -> MyResult {
    let csv_util = CsvUtil::parse();
    let input_path = Path::new(&csv_util.input_path);
    let output_path = Path::new(&csv_util.output_path);
    let file_lines = csv_util.file_lines;
    let mut header = csv_util.header;

    if !input_path.exists() {
        println!("Cannot find input file!");
        exit(2);
    }

    match create_dir(output_path).await {
        Ok(_) => {}
        Err(_) => {
            println!("output-path already exists!");
            exit(2);
        }
    }

    let input_file = File::open(input_path).await?;
    let buf_reader = BufReader::new(input_file);
    let mut lines = buf_reader.lines();
    let mut line_counter = 0;
    let mut bytes_size: usize = 0;
    let mut seek_from: u64 = 0;
    let mut file_suffix: usize = 0;
    let mut header_line: String = String::from("");

    let mut set = JoinSet::new();

    while let Some(line) = lines.next_line().await? {
        if header > 0 {
            header -= 1;
            header_line.push_str(&format!("{}\n", &line));
            seek_from += u64::try_from(line.len() + NEWLINE_LEN).unwrap();
            continue;
        }

        line_counter += 1;
        bytes_size += line.len() + NEWLINE_LEN;
        if line_counter % file_lines == 0 {
            set.spawn(process_parts(PathBuf::from(csv_util.input_path.clone()), PathBuf::from(csv_util.output_path.clone()), file_suffix.to_string(), SeekFrom::Start(seek_from), bytes_size, file_suffix > 0, header_line.clone()));
            seek_from += u64::try_from(bytes_size).unwrap();
            file_suffix += 1;
            bytes_size = 0;
        }
    }

    while let Some(_) = set.join_next().await {}
    Ok(())
}

async fn process_parts(input_path: PathBuf, mut output_path: PathBuf, file_suffix: String, seek_from: SeekFrom, bytes_size: usize, need_bom: bool, header_line: String) -> MyResult {
    let input_file = File::open(&input_path).await?;
    let mut buf_reader = BufReader::new(input_file);
    output_path.push(input_path.file_stem().map(|n| format!("{}_{}.csv", OsString::from(n).to_str().unwrap(), file_suffix)).unwrap());

    buf_reader.seek(seek_from).await?;

    let output_file = File::create(output_path).await?;
    let mut buf_writer = BufWriter::new(output_file);

    let mut buffer = vec![0u8; BUF_SIZE];
    let mut read_size: usize = 0;

    if need_bom {
        buf_writer.write_all(&BOM).await?;
    }
    buf_writer.write_all(header_line.as_bytes()).await?;

    while let Ok(size) = buf_reader.read(&mut buffer).await {
        if size == 0usize {
            break;
        }
        if read_size + size > bytes_size {
            buf_writer.write(&buffer[..(bytes_size - read_size)]).await?;
            break;
        } else {
            buf_writer.write(&buffer[..size]).await?;
            read_size += size;
        }
    }
    buf_writer.flush().await?;
    Ok(())
}
