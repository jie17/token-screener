use cmc::async_api::Cmc;
use csv::Writer;
use serde_json::Value;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(serde::Serialize)]
struct Row {
    id: String,
    symbol: String,
    rank: i64,
    platform_symbol: String,
}

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
        // will be written to stdout.
        .with_max_level(Level::INFO)
        // completes the builder.
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
    info!("Starting CMC metadata collection.");
    dotenvy::dotenv().unwrap();
    let cmc_api_key = std::env::var("CMC_API_KEY").unwrap();
    let cmc = Cmc::new(cmc_api_key);

    let id_map = cmc.id_map(1, 5000, cmc::Sort::CmcRank).await.unwrap();

    let mut wtr = Writer::from_path("data/id_map.csv").unwrap();
    for id in id_map.data.iter() {
        wtr.serialize(Row {
            id: id.id.to_string(),
            symbol: id.symbol.clone(),
            rank: id.rank,
            platform_symbol: match &id.platform {
                Value::Object(platform) => platform
                    .get("symbol")
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_string(),
                _ => String::from(""),
            },
        })
        .unwrap();
    }
    wtr.flush().unwrap()
}
