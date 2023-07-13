mod logger;
mod reits;
mod crsp;
use crsp::csv_crsp_reader;
use reits::reit_data::output_reit_tickers;
use reits::reit_data;
use surrealdb::engine::any;
use surrealdb::engine::remote::ws::Client;
use surrealdb::engine::remote::ws::Ws;
use surrealdb::kvs::Val;
use surrealdb::opt::auth::Root;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb::engine::any::connect;
use tokio;
use tokio::fs::{self,DirEntry};
use std::env;
use std::path::{Path,PathBuf};
use tokio_stream::StreamExt;
use polars::prelude::*;
use tracing::{info,Level};
use tracing_subscriber::fmt;
use tracing_appender;

static DB: Surreal<Any> = Surreal::init();

pub async fn visit(
    paths: &str,
) -> Result<Vec<DirEntry>, std::io::Error>{
    let paths = Path::new(paths);
    print!("{:?}", paths);
    let mut entries = fs::read_dir(paths).await.unwrap();
    let count = 0;
    let mut pths: Vec<DirEntry> = Vec::new();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        pths.push(entry);
    }
    Ok(pths)
}

#[tokio::main]
async fn main() {
    logger::setup_logger().unwrap();
    let args:Vec<String> = env::args().collect();
    let ext: &str = &args[1];//"/root/data/dbz/";
    let cplt: &str = &args[2];
    println!("{:?}",ext);
    println!("{:?}", cplt);
    let mut completed_paths: Vec<String> = Vec::new();
    let url: &str = "209.127.152.40:21";
    let completed_file = std::path::Path::new(cplt);//"/root/data/complete.txt"
    let path_vec = visit(&ext).await.unwrap();
    let db = Surreal::new::<Ws>("127.0.0.1:8000").await.unwrap();
    // Signin as a namespace, database, or root user
    db.signin(Root {
        username: "root",
        password: "root",
    })
    .await.unwrap();
    // Select a specific namespace / database
    //let ses = Session::for_db("commodities","soy.futures");
    println!("connected");
    //let company: Obvs = DB.create("company-year").content(row_json).await.unwrap();
    let mut counter = 0;
    let mut reit_ticks = output_reit_tickers().await.unwrap();
    let mut reit_ticks = reit_data::reit_ticker_vec_split(&reit_ticks).await.unwrap();
    let crsp_path: PathBuf = PathBuf::from("y5bmhefpwmpjmfcw.csv");
    let mut df = CsvReader::from_path(crsp_path).unwrap()
        .has_header(true)
        .with_schema(Arc::new(csv_crsp_reader::get_test_crsp_schema().await.unwrap().clone()))
        .finish().unwrap();
    logger::log_polars_object(&df
        .groupby(["TICKER"])
        .unwrap()
        .select(&["VOL","SHROUT"])
        .mean()
        .unwrap()).await;
        df.rename("TICKER", "ticker");
        println!("{:?}",df.clone().lazy().select([count()]).collect().unwrap());
        let df_vec = &reit_data::query_ticker(reit_ticks, df.clone()).await.unwrap();
        println!("{:?}",df_vec.len());
        for reit_df in df_vec.into_iter() {
            if reit_df.height() == 0 {continue;}
            println!("{:?}",&reit_df.clone().lazy().select([count()]).collect().unwrap());
            logger::log_polars_object(&reit_df.clone().head(Some(15))).await;
            println!("{:?}", &reit_data::prepare_ticker(&reit_df, "RET").await);
        }
}
