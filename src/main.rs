use futures_util::StreamExt;
use serde::Deserialize;
use tokio::signal;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

#[derive(Debug, Deserialize)]
struct Quotes {
    bids: Vec<(String, String)>,
    asks: Vec<(String, String)>,
}

#[derive(Debug)]
struct OrderBook {
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
    spread: f64,
    order_book_imbalance: f64,
}

impl OrderBook {
    pub fn new() -> Self {
        OrderBook {
            bids: Vec::new(),
            asks: Vec::new(),
            spread: 0.0,
            order_book_imbalance: 0.0,
        }
    }

    pub fn update(&mut self, quotes: Quotes) {
        self.bids = quotes
            .bids
            .into_iter()
            .map(|(price, qty)| (price.parse::<f64>().unwrap(), qty.parse::<f64>().unwrap()))
            .collect::<Vec<(f64, f64)>>();
        self.asks = quotes
            .asks
            .into_iter()
            .map(|(price, qty)| (price.parse::<f64>().unwrap(), qty.parse::<f64>().unwrap()))
            .collect::<Vec<(f64, f64)>>();
        self.spread = self.asks.first().unwrap().0 - self.bids.first().unwrap().0;
        let bid_volume = self.bids.iter().map(|(_price, qty)| qty).sum::<f64>();
        let ask_volume = self.asks.iter().map(|(_price, qty)| qty).sum::<f64>();
        self.order_book_imbalance = bid_volume / (bid_volume + ask_volume);
    }

    pub fn display(&self) {
        println!("-");
        for (price, qty) in self.bids.iter() {
            println!("bid: {}, qty: {}", price, qty);
        }
        println!(
            "-\nspread: {}\norder book imbalance: {}\n-",
            self.spread, self.order_book_imbalance
        );
        for (price, qty) in self.asks.iter().rev() {
            println!("ask: {}, qty: {}", price, qty);
        }
    }
}

#[tokio::main]
async fn main() {
    let request = "wss://stream.binance.com:9443/ws/btcusdt@depth10"
        .into_client_request()
        .unwrap();

    let (mut ws_stream, _) = connect_async(request).await.unwrap();
    let mut order_book = OrderBook::new();

    loop {
        tokio::select! {
            _ = signal::ctrl_c() => {
                println!("-\nshutting down");
                break;
            }
            msg = ws_stream.next() => {
                if let Some(Ok(Message::Text(text))) = msg {
                    let quotes: Quotes = serde_json::from_str(&text).unwrap();

                    order_book.update(quotes);
                    order_book.display();

                }
            }
        }
    }
}
