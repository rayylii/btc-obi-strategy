use futures_util::StreamExt;
use serde::Deserialize;
use tokio::signal;
use tokio_tungstenite::{
    connect_async, tungstenite::Message, tungstenite::client::IntoClientRequest,
};

#[derive(Deserialize, Debug)]
struct Quote {
    #[serde(rename = "b")]
    best_bid_price: String,

    #[serde(rename = "B")]
    best_bid_qty: String,

    #[serde(rename = "a")]
    best_ask_price: String,

    #[serde(rename = "A")]
    best_ask_qty: String,
}

#[derive(Debug)]
struct OrderBook {
    best_bid_price: f64,
    best_bid_qty: f64,
    best_ask_price: f64,
    best_ask_qty: f64,
    spread: f64,
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            best_bid_price: 0.0,
            best_bid_qty: 0.0,
            best_ask_price: 0.0,
            best_ask_qty: 0.0,
            spread: 0.0,
        }
    }

    pub fn update(&mut self, quote: Quote) {
        self.best_bid_price = quote.best_bid_price.parse::<f64>().unwrap();
        self.best_bid_qty = quote.best_bid_qty.parse::<f64>().unwrap();
        self.best_ask_price = quote.best_ask_price.parse::<f64>().unwrap();
        self.best_ask_qty = quote.best_ask_qty.parse::<f64>().unwrap();
        self.spread = self.best_ask_price - self.best_bid_price;
    }

    pub fn display(&self) {
        println!(
            "-\nbid: {}, qty: {}\nask: {}, qty: {}\nspread: {}",
            self.best_bid_price,
            self.best_bid_qty,
            self.best_ask_price,
            self.best_ask_qty,
            self.spread,
        )
    }
}

#[tokio::main]
async fn main() {
    let request = "wss://stream.binance.com:9443/ws/btcusdt@bookTicker"
        .into_client_request()
        .unwrap();
    let (mut ws_stream, _) = connect_async(request).await.unwrap();
    let mut order_book = OrderBook::new();

    loop {
        tokio::select! {
            _ = signal::ctrl_c() => {
                println!("-\nshutting down");
                break; },
            msg = ws_stream.next() => {
                if let Some(Ok(Message::Text(text))) = msg {
                    let quote: Quote = serde_json::from_str(&text).unwrap();

                    order_book.update(quote);
                    order_book.display();
                }
            }
        }
    }
}
