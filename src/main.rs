mod logger;
use surrealdb::engine::any;
use surrealdb::engine::remote::ws::Client;
use surrealdb::engine::remote::ws::Ws;
use surrealdb::kvs::Val;
use surrealdb::opt::auth::Root;
use surrealdb::Surreal;
use surrealdb::{self, dbs::Session, kvs::Datastore};
use tokio;
use tokio::fs::{self,DirEntry};
use std::env;
use std::path::Path;
use tokio::spawn;
use tokio_stream::StreamExt;

static DB: Surreal<Client> = Surreal::init();

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
    DB.connect::<Ws>("10.162.0.2:8080").await.unwrap();
    // Log into the database
    DB.signin(Root {
        username: "root",
        password: "root",
    })
    .await
    .unwrap();
    println!("Connected");
    DB.use_ns("commodities").use_db("soy.futures").await.unwrap();
    //let company: Obvs = DB.create("company-year").content(row_json).await.unwrap();
    let mut counter = 0;
    for entry in path_vec.iter() {
        counter += 1;
        logger::log_name(&entry.path().to_str().unwrap().to_string());
        println!("{:?}",counter);
    }
}

