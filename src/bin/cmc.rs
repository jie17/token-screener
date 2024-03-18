use cmc::{api::cryptocurrency::Metadata, async_api::Cmc};
use csv::Writer;
use serde_json::Value;
use tokio::time::{sleep, Duration};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(serde::Serialize)]
struct Row {
    id: String,
    name: String,
    source_code_url: String,
    date_launched: Value,
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

    let mut metadata_vec: Vec<Metadata> = Vec::new();

    for chunk in id_map.data.chunks(100) {
        info!("Fetching metadata for chunk of 100 coins.");
        let ids: Vec<String> = chunk.iter().map(|cc| cc.id.to_string()).collect();
        let query = ids.join(",");

        // Call metadata_map for the current chunk.
        let result = cmc.metadata_map(query).await.unwrap();

        metadata_vec.extend(result.values().cloned().collect::<Vec<Metadata>>());

        // Sleep for 1 second before making the next call.
        sleep(Duration::from_secs(15)).await;
    }

    let mut wtr = Writer::from_path("data/metadata.csv").unwrap();
    for metadata in metadata_vec.iter() {
        if !metadata.urls.source_code.is_empty() {
            wtr.serialize(Row {
                id: metadata.id.to_string(),
                name: metadata.name.clone(),
                source_code_url: metadata.urls.source_code[0].to_string(),
                date_launched: metadata.date_launched.clone(),
            })
            .unwrap();
        }
    }
    wtr.flush().unwrap()
}
