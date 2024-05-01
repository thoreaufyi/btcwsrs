use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use mysql::prelude::Queryable;
use mysql::{Error as MysqlError, Pool, PooledConn};
use serde::{de::Error, Deserialize, Serialize};
use serde_json::{from_str, to_string, Value};
use std::error::Error as StdError;
use std::time::Duration;
use tokio::time;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;

#[derive(Debug, Serialize, Deserialize)]
struct BitcoinTrade {
    exchange: String,
    amount: f64,
    side: String, // "buy" or "sell"
    price: f64,
    timestamp: i64, // Unix timestamp in milliseconds
}

#[derive(Debug, Serialize, Deserialize)]
struct BybitTrade {
    i: String, // Trade ID
    T: i64,    // Timestamp
    p: String, // Price
    v: String, // Volume
    S: String, // Side (Buy/Sell)
    s: String, // Symbol (e.g., BTCUSDT)
    BT: bool,  // Boolean indicator
}

#[derive(Debug, Serialize, Deserialize)]
struct BinanceTrade {
    stream: String,
    data: BinanceTradeData,
}

#[derive(Debug, Serialize, Deserialize)]
struct BinanceTradeData {
    e: String,
    E: i64,
    s: String,
    p: String,
    q: String,
    T: i64,
    m: bool,
    X: String,
}

#[derive(Serialize)]
struct BitfinexSubscribe {
    event: String,
    channel: String,
    symbol: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct CoinbaseSubscribe {
    #[serde(rename = "type")]
    type_field: String,
    product_ids: Vec<String>,
    channels: Vec<CoinbaseChannel>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum CoinbaseChannel {
    Name(String),
    WithProducts {
        name: String,
        product_ids: Vec<String>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
struct CoinbaseError {
    #[serde(rename = "type")]
    type_field: String,
    message: String,
    reason: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct CoinbaseTicker {
    #[serde(rename = "type")]
    type_field: String,
    sequence: i64,
    product_id: String,
    price: String,
    open_24h: String,
    volume_24h: String,
    low_24h: String,
    high_24h: String,
    volume_30d: String,
    best_bid: String,
    best_bid_size: String,
    best_ask: String,
    best_ask_size: String,
    side: String,
    time: String,
    trade_id: i64,
    last_size: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct BitfinexTradeDetails {
    id: i64,
    timestamp: i64,
    amount: f64,
    price: f64,
}

#[derive(Debug, Serialize, Deserialize)]
struct CoinbaseHeartbeat {
    #[serde(rename = "type")]
    type_field: String,
    last_trade_id: i64,
    product_id: String,
    sequence: i64,
    time: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct CoinbaseSubscriptions {
    #[serde(rename = "type")]
    type_field: String,
    channels: Vec<CoinbaseChannelSubscription>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CoinbaseChannelSubscription {
    name: String,
    product_ids: Vec<String>,
    account_ids: Option<Vec<String>>,
}

// Define a type alias for clarity
type BoxedError = Box<dyn StdError + Send>;

lazy_static::lazy_static! {
    static ref MYSQL_POOL: Pool = {
        let url_encoded_password = urlencoding::encode("password");
        let url = format!(
            "mysql://thoreau:{}@localhost:3310/bitcoin_trades",
            url_encoded_password
        );
        Pool::new(url).expect("Failed to create MySQL pool")
    };
}

fn save_bitcoin_order(conn: &mut PooledConn, trade: &BitcoinTrade) -> Result<(), MysqlError> {
    let query =
        "INSERT INTO orders (exchange, amount, side, price, timestamp) VALUES (?, ?, ?, ?, now())";
    conn.exec_drop(
        query,
        (
            trade.exchange.as_str(),
            trade.amount,
            trade.side.as_str(),
            trade.price,
        ),
    )?;

    Ok(())
}

fn parse_bitfinex_trade(message: &str) -> Result<BitfinexTradeDetails, serde_json::Error> {
    let json: Value = serde_json::from_str(message)?;

    if json[1] != "te" {
        return Err(serde_json::Error::custom("Invalid message type"));
    }

    let id = json[0]
        .as_i64()
        .ok_or_else(|| serde_json::Error::custom("Invalid id"))?;
    let timestamp = json[2][0]
        .as_i64()
        .ok_or_else(|| serde_json::Error::custom("Invalid timestamp"))?;
    let amount = json[2][2]
        .as_f64()
        .ok_or_else(|| serde_json::Error::custom("Invalid amount"))?;
    let price = json[2][3]
        .as_f64()
        .ok_or_else(|| serde_json::Error::custom("Invalid price"))?;

    Ok(BitfinexTradeDetails {
        id,
        timestamp,
        amount,
        price,
    })
}

fn convert_binance_trade(binance_trade: &BinanceTrade) -> BitcoinTrade {
    BitcoinTrade {
        exchange: "binance".to_string(),
        amount: binance_trade.data.q.parse().unwrap_or(0.0),
        side: if binance_trade.data.m {
            "sell".to_string()
        } else {
            "buy".to_string()
        },
        price: binance_trade.data.p.parse().unwrap_or(0.0),
        timestamp: binance_trade.data.T,
    }
}

fn convert_bitfinex_trade(bitfinex_trade: &BitfinexTradeDetails) -> BitcoinTrade {
    let trade = BitcoinTrade {
        exchange: "bitfinex".to_string(),
        amount: bitfinex_trade.amount.abs(),
        side: if bitfinex_trade.amount > 0.0 {
            "buy".to_string()
        } else {
            "sell".to_string()
        },
        price: bitfinex_trade.price,
        timestamp: bitfinex_trade.timestamp,
    };
    println!("Bitfinex: {:?}", trade);
    trade
}

fn convert_coinbase_ticker(coinbase_ticker: &CoinbaseTicker) -> BitcoinTrade {
    BitcoinTrade {
        exchange: "coinbase".to_string(),
        amount: coinbase_ticker.last_size.parse().unwrap_or(0.0),
        side: coinbase_ticker.side.clone(),
        price: coinbase_ticker.price.parse().unwrap_or(0.0),
        timestamp: coinbase_ticker
            .time
            .parse::<DateTime<Utc>>()
            .unwrap()
            .timestamp_millis(),
    }
}

fn convert_bybit_trade(bybit_trade: &BybitTrade) -> BitcoinTrade {
    BitcoinTrade {
        exchange: "bybit".to_string(),
        amount: bybit_trade.v.parse().unwrap_or(0.0),
        side: bybit_trade.S.clone().to_lowercase(),
        price: bybit_trade.p.parse().unwrap_or(0.0),
        timestamp: bybit_trade.T,
    }
}

fn create_coinbase_subscription() -> CoinbaseSubscribe {
    CoinbaseSubscribe {
        type_field: "subscribe".to_string(),
        product_ids: vec!["BTC-USD".to_string()],
        channels: vec![
            CoinbaseChannel::Name("level2".to_string()),
            CoinbaseChannel::Name("heartbeat".to_string()),
            CoinbaseChannel::WithProducts {
                name: "ticker".to_string(),
                product_ids: vec!["BTC-USD".to_string()],
            },
        ],
    }
}

async fn manage_websocket_connection(name: &str, url: &str) -> Result<(), BoxedError> {
    let mut should_reconnect = true;
    let mut reconnect_delay = Duration::from_secs(1); // Initial delay for reconnection attempts

    while should_reconnect {
        let url = Url::parse(url).unwrap();
        let (mut ws_stream, _) = connect_async(url)
            .await
            .map_err(|e| Box::new(e) as BoxedError)?;

        match name {
            "bitfinex" => {
                let subscribe_message = BitfinexSubscribe {
                    event: "subscribe".to_string(),
                    channel: "trades".to_string(),
                    symbol: "BTCUSD".to_string(),
                };
                let message =
                    to_string(&subscribe_message).map_err(|e| Box::new(e) as BoxedError)?;
                ws_stream
                    .send(Message::Text(message))
                    .await
                    .map_err(|e| Box::new(e) as BoxedError)?;
            }
            "coinbase" => {
                let subscribe_message = create_coinbase_subscription();
                let message =
                    to_string(&subscribe_message).map_err(|e| Box::new(e) as BoxedError)?;
                ws_stream
                    .send(Message::Text(message))
                    .await
                    .map_err(|e| Box::new(e) as BoxedError)?;
            }
            "bybit" => {
                let subscribe_message = r#"{"op":"subscribe","args":["publicTrade.BTCUSDT"]}"#;
                ws_stream
                    .send(Message::Text(subscribe_message.to_string()))
                    .await
                    .map_err(|e| Box::new(e) as BoxedError)?;
            }
            "binance" => {
                let message = r#"{"method": "LIST_SUBSCRIPTIONS", "id": 123}"#;
                ws_stream
                    .send(Message::Text(message.to_string()))
                    .await
                    .map_err(|e| Box::new(e) as BoxedError)?;
            }
            _ => {}
        }

        while let Some(message) = ws_stream.next().await {
            match message {
                Ok(Message::Text(text)) => match name {
                    "binance" => handle_binance_message(&text),
                    "bitfinex" => handle_bitfinex_message(&text),
                    "coinbase" => handle_coinbase_message(&text),
                    "bybit" => handle_bybit_message(&text),
                    _ => println!("{}: {}", name, text),
                },
                Ok(Message::Binary(_bin)) => println!("{}: Binary data received", name),
                Err(e) => {
                    eprintln!("Error receiving message: {:?}", e);
                    should_reconnect = true; // Set reconnect flag to true
                    break; // Break the inner loop to attempt reconnection
                }
                _ => break,
            }
        }

        // Delay before attempting reconnection
        if should_reconnect {
            tokio::time::sleep(reconnect_delay).await;
            reconnect_delay *= 2; // Exponential backoff for reconnection delay
        }
    }

    Ok(())
}

fn handle_binance_message(text: &str) {
    match from_str::<BinanceTrade>(text) {
        Ok(trade) => {
            let trade = convert_binance_trade(&trade);
            let mut conn = MYSQL_POOL.get_conn().unwrap();
            if let Err(err) = save_bitcoin_order(&mut conn, &trade) {
                eprintln!("Error saving Bitcoin order: {}", err);
            }
            println!("Binance: {:?}", trade);
        }
        Err(err) => {
            eprintln!("Failed to deserialize Binance trade data: {}", err);
        }
    }
}

fn handle_bybit_message(text: &str) {
    let parsed = serde_json::from_str::<Value>(text).unwrap();
    match parsed {
        Value::Object(obj) => {
            if let Some(data_array) = obj.get("data").and_then(|data| data.as_array()) {
                for trade_data in data_array {
                    let trade = serde_json::from_value::<BybitTrade>(trade_data.clone()).unwrap();
                    let bitcoin_trade = convert_bybit_trade(&trade);
                    let mut conn = MYSQL_POOL.get_conn().unwrap();
                    save_bitcoin_order(&mut conn, &bitcoin_trade).unwrap();
                    println!("Bybit: {:?}", bitcoin_trade);
                }
            }
        }
        _ => return,
    }
}

fn handle_bitfinex_message(text: &str) {
    let trade = parse_bitfinex_trade(text);
    match trade {
        Ok(trade) => {
            let bitcoin_trade = convert_bitfinex_trade(&trade);
            let mut conn = MYSQL_POOL.get_conn().unwrap();
            save_bitcoin_order(&mut conn, &bitcoin_trade).unwrap();
            println!("Bitfinex: {:?}", bitcoin_trade);
        }
        Err(_e) => {}
    }
}

fn handle_coinbase_message(text: &str) {
    match serde_json::from_str::<CoinbaseTicker>(text) {
        Ok(ticker) => {
            let trade = convert_coinbase_ticker(&ticker);
            let mut conn = MYSQL_POOL.get_conn().unwrap();
            save_bitcoin_order(&mut conn, &trade).unwrap();
            println!("Coinbase: {:?}", trade);
        }
        Err(_) => {}
    }
}
fn init_db() -> Result<(), MysqlError> {
    let mut conn = MYSQL_POOL.get_conn()?;
    conn.exec_drop(
        r"CREATE TABLE IF NOT EXISTS orders (
            id INTEGER AUTO_INCREMENT PRIMARY KEY,
            exchange VARCHAR(50) NOT NULL,
            amount DOUBLE NOT NULL,
            side VARCHAR(10) NOT NULL,
            price DOUBLE NOT NULL,
            timestamp BIGINT NOT NULL,
            INDEX idx_timestamp (timestamp)
        )",
        (),
    )?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send>> {
    init_db().expect("Failed to initialize database or create tables");

    loop {
        // Instead of storing strings in a vector, you can create them inside the map directly.
        let exchanges = vec![
            (
                "binance",
                "wss://fstream.binance.com/stream?streams=btcusdt@trade",
            ),
            ("bitfinex", "wss://api-pub.bitfinex.com/ws/2"),
            ("coinbase", "wss://ws-feed.pro.coinbase.com"),
            ("bybit", "wss://stream.bybit.com/v5/public/spot"),
        ];

        let handles: Vec<_> = exchanges
            .iter()
            .map(|&(name, url)| {
                let name = name.to_string(); // Create owned String here
                let url = url.to_string(); // Create owned String here
                tokio::spawn(async move { manage_websocket_connection(&name, &url).await })
            })
            .collect();

        for handle in handles {
            let result = handle.await; // This returns Result<Result<(), BoxedError>, JoinError>
            match result {
                Ok(Ok(())) => {} // Everything went fine
                Ok(Err(e)) => {
                    // This is your custom error from manage_websocket_connection
                    eprintln!("Error processing websocket connection: {:?}", e);
                }
                Err(e) => {
                    // This captures errors related to the spawned task panicking
                    eprintln!("Task panicked: {:?}", e);
                }
            }
        }

        // Sleep for 5 minutes before restarting the connections
        time::sleep(Duration::from_secs(43200)).await;
    }
}
