use chrono::Local;
use futures_util::StreamExt;
use serde::{Deserialize, Deserializer};
use std::fmt;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;
use tokio::signal;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::time::Duration;
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

#[derive(Clone, Copy, PartialEq)]
enum PositionSide {
    Long,
    Short,
}

#[derive(Clone, Copy)]
struct Position {
    side: PositionSide,
    entry_price: f64,
}

#[derive(Default)]
struct PnlTracker {
    total_pnl_usd: f64,
    trade_count: u32,
    position: Option<Position>,
}

struct TradeIntent {
    signal: Signal,
    price: f64,
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

    fn best_bid(&self) -> Option<f64> {
        self.bids.first().map(|(p, _)| *p)
    }

    fn best_ask(&self) -> Option<f64> {
        self.asks.first().map(|(p, _)| *p)
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

fn log_trade(
    side: &str,
    entry_price: f64,
    exit_price: f64,
    pnl: f64,
    total_pnl: f64,
    trade_count: u32,
) {
    let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string();
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("trades.csv")
        .unwrap();
    if file.metadata().unwrap().len() == 0 {
        writeln!(
            file,
            "timestamp,side,entry_price,exit_price,pnl,total_pnl,trade_count"
        )
        .unwrap();
    }
    writeln!(
        file,
        "{},{},{:.2},{:.2},{:.4},{:.4},{}",
        timestamp, side, entry_price, exit_price, pnl, total_pnl, trade_count
    )
    .unwrap()
}

async fn executor(mut rx: mpsc::Receiver<TradeIntent>, state: Arc<Mutex<PnlTracker>>) {
    while let Some(intent) = rx.recv().await {
        let mut tracker = state.lock().await;

        let current_position = tracker.position;
        match (current_position, &intent.signal) {
            (None, Signal::StrongBuy) => {
                println!(
                    "side: long, price: ${:.2}, size: {} BTC",
                    intent.price, TRADE_SIZE_BTC
                );
                tracker.position = Some(Position {
                    side: PositionSide::Long,
                    entry_price: intent.price,
                })
            }
            (None, Signal::StrongSell) => {
                println!(
                    "side: short, price: ${:.2}, size: {} BTC",
                    intent.price, TRADE_SIZE_BTC
                );
                tracker.position = Some(Position {
                    side: PositionSide::Short,
                    entry_price: intent.price,
                })
            }
            (Some(pos), Signal::StrongSell) if pos.side == PositionSide::Long => {
                let pnl = (intent.price - pos.entry_price) * TRADE_SIZE_BTC;
                tracker.total_pnl_usd += pnl;
                tracker.trade_count += 1;
                tracker.position = None;
                println!(
                    "closed long, entry: ${:.2}, exit: ${:.2}, pnl: ${:.4}, total pnl: ${:.4}, trades = {}",
                    pos.entry_price, intent.price, pnl, tracker.total_pnl_usd, tracker.trade_count
                );
                log_trade(
                    "long",
                    pos.entry_price,
                    intent.price,
                    pnl,
                    tracker.total_pnl_usd,
                    tracker.trade_count,
                )
            }
            (Some(pos), Signal::StrongBuy) if pos.side == PositionSide::Short => {
                let pnl = (pos.entry_price - intent.price) * TRADE_SIZE_BTC;
                tracker.total_pnl_usd += pnl;
                tracker.trade_count += 1;
                tracker.position = None;
                println!(
                    "closed short, entry: ${:.2}, exit = ${:.2}, pnl: ${:.4}, total pnl: ${:.4}, trades = {}",
                    pos.entry_price, intent.price, pnl, tracker.total_pnl_usd, tracker.trade_count
                );
                log_trade(
                    "short",
                    pos.entry_price,
                    intent.price,
                    pnl,
                    tracker.total_pnl_usd,
                    tracker.trade_count,
                )
            }
            _ => {}
        }
    }
}

const TRADE_SIZE_BTC: f64 = 0.001;
const STARTING_CAPITAL_USD: f64 = 100.0;
const SESSION_DURATION_SECS: u64 = 1800;

#[tokio::main]
async fn main() {
    let request = "wss://stream.binance.com:9443/ws/btcusdt@depth10@100ms"
        .into_client_request()
        .expect("failed to parse");

    let (mut ws_stream, _) = connect_async(request).await.expect("failed to connect");

    let (tx, mut rx) = mpsc::channel::<String>(32);
    let pnl_tracker = Arc::new(Mutex::new(PnlTracker::default()));
    let pnl_tracker_clone = Arc::clone(&pnl_tracker);
    let (trade_tx, trade_rx) = mpsc::channel::<TradeIntent>(32);

    tokio::spawn(executor(trade_rx, pnl_tracker_clone));

    tokio::spawn(async move {
        while let Some(msg) = ws_stream.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    if tx.send(text.to_string()).await.is_err() {
                        break;
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    println!("-\nerror: {}", e);
                    break;
                }
            }
        }
    });

    let mut order_book = OrderBook::new();
    let session_end = tokio::time::sleep(Duration::from_secs(SESSION_DURATION_SECS));
    tokio::pin!(session_end);

    loop {
        tokio::select! {
            _ = signal::ctrl_c() => {
                let tracker = pnl_tracker.lock().await;
                let pct_return = (tracker.total_pnl_usd / STARTING_CAPITAL_USD) * 100.0;
                println!("-\nshutting down, trades: {}, pnl: ${:.4}, return: {:.4}%", tracker.trade_count, tracker.total_pnl_usd, pct_return);
                break;
            }
            _ = &mut session_end => {
                let tracker = pnl_tracker.lock().await;
                let pct_return = (tracker.total_pnl_usd / STARTING_CAPITAL_USD) * 100.0;
                println!("-\nauto shutdown, trades : {}, pnl: ${:.4}, return: {:.4}%", tracker.trade_count, tracker.total_pnl_usd, pct_return);
                break;
            }
            msg = rx.recv() => {
                match msg {
                    Some(text) => {
                        if let Ok(quotes) = serde_json::from_str::<Quotes>(&text) {
                            order_book.update(quotes);
                            print!("{}", order_book);
                            match order_book.signal() {
                                Some(Signal::StrongBuy) => {
                                    if let Some(price) = order_book.best_ask() {
                                        let _ = trade_tx.send(TradeIntent {
                                            signal: Signal::StrongBuy,
                                            price,
                                        }).await;
                                    }
                                }
                                Some(Signal::StrongSell) =>  {
                                    if let Some(price) = order_book.best_bid() {
                                        let _ = trade_tx.send(TradeIntent {
                                            signal: Signal::StrongSell,
                                            price,
                                        }).await;
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    None => {
                        println!("-\nstream closed");
                        break;
                    }
                }
            }
        }
    }
}
