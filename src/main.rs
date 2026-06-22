use futures_util::StreamExt;
use serde::{Deserialize, Deserializer};
use std::fmt;
use tokio::signal;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

#[derive(Debug, Deserialize)]
struct Quotes {
    #[serde(deserialize_with = "deserialize_price_qty")]
    bids: Vec<(f64, f64)>,

    #[serde(deserialize_with = "deserialize_price_qty")]
    asks: Vec<(f64, f64)>,
}

fn deserialize_price_qty<'de, D>(deserializer: D) -> Result<Vec<(f64, f64)>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw: Vec<(&str, &str)> = Deserialize::deserialize(deserializer)?;
    raw.into_iter()
        .map(|(p, q)| {
            Ok((
                p.parse().map_err(serde::de::Error::custom)?,
                q.parse().map_err(serde::de::Error::custom)?,
            ))
        })
        .collect()
}

#[derive(Debug)]
enum Signal {
    StrongBuy,
    Buy,
    Neutral,
    Sell,
    StrongSell,
}

impl Signal {
    fn from_obi(obi: f64, prev_obi: f64) -> Self {
        let shift = obi - prev_obi;
        if obi > 0.7 && shift > 0.05 {
            Signal::StrongBuy
        } else if obi > 0.6 {
            Signal::Buy
        } else if obi < 0.3 && shift < -0.05 {
            Signal::StrongSell
        } else if obi < 0.4 {
            Signal::Sell
        } else {
            Signal::Neutral
        }
    }
}

impl fmt::Display for Signal {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = match self {
            Signal::StrongBuy => "strong buy",
            Signal::Buy => "buy",
            Signal::Neutral => "neutral",
            Signal::Sell => "sell",
            Signal::StrongSell => "strong sell",
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug, Default)]
struct OrderBook {
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
    spread: Option<f64>,
    obi: Option<f64>, // order book imbalance
    prev_obi: Option<f64>,
}

impl OrderBook {
    fn new() -> Self {
        Self::default()
    }

    fn update(&mut self, quotes: Quotes) {
        self.prev_obi = self.obi;
        self.bids = quotes.bids;
        self.asks = quotes.asks;

        self.spread = match (self.asks.first(), self.bids.first()) {
            (Some(ask), Some(bid)) => Some(ask.0 - bid.0),
            _ => None,
        };
        let bid_volume = self.bids.iter().map(|(_, qty)| qty).sum::<f64>();
        let ask_volume = self.asks.iter().map(|(_, qty)| qty).sum::<f64>();
        let total_volume = bid_volume + ask_volume;

        self.obi = if total_volume > 0.0 {
            Some(bid_volume / total_volume)
        } else {
            None
        };
    }

    fn signal(&self) -> Option<Signal> {
        match (self.obi, self.prev_obi) {
            (Some(obi), Some(prev_obi)) => Some(Signal::from_obi(obi, prev_obi)),
            _ => None,
        }
    }
}

impl fmt::Display for OrderBook {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "-")?;
        for (price, qty) in self.asks.iter().rev() {
            writeln!(f, "ask: {:.2}, qty: {:.4}", price, qty)?;
        }
        let spread_str = match self.spread {
            Some(s) => format!("{:.2}", s),
            None => "unavailable".to_string(),
        };

        let obi_str = match self.obi {
            Some(o) => format!("{:.4}", o),
            None => "unavailable".to_string(),
        };

        let signal_str = match self.signal() {
            Some(s) => s.to_string(),
            None => "unavailable".to_string(),
        };
        writeln!(
            f,
            "-\nspread: {}\norder book imbalance: {}\nsignal: {}\n-",
            spread_str, obi_str, signal_str,
        )?;
        for (price, qty) in self.bids.iter() {
            writeln!(f, "bid: {:.2}, qty: {:.4}", price, qty)?;
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let request = "wss://stream.binance.com:9443/ws/btcusdt@depth10"
        .into_client_request()
        .expect("failed to parse");

    let (mut ws_stream, _) = connect_async(request).await.expect("failed to connect");
    let mut order_book = OrderBook::new();

    loop {
        tokio::select! {
            _ = signal::ctrl_c() => {
                println!("-\nshutting down");
                break;
            }
            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if let Ok(quotes) = serde_json::from_str::<Quotes>(&text) {
                            order_book.update(quotes);
                            print!("{}", order_book)
                        }
                    }
                    Some(Err(e)) => {
                        println!("-\nerror: {}", e);
                        break;
                    }
                    None => {
                        println!("-\nconnection closed");
                        break;
                    }
                    _ => {}
                }
            }
        }
    }
}
