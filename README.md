# btc-obi-strategy

a paper trading bot implementing an order book imbalance (obi) signal on btc/usdt using binance websocket data.

## strategy

at each tick, compute obi as:

obi = bid_volume / (bid_volume + ask_volume)

enter long when obi > 0.7 and shift > 0.05 (strong buying pressure)  
enter short when obi < 0.3 and shift < -0.05 (strong selling pressure)  
close on opposite signal

## results (2 sessions x 1 hour, \~2,500 trades, 0.001 btc (\~$60) position size)

win rate: \~89%  
ev per trade: \~$0.012  
binomial p-value: < 1e-60  

## limitations

strategy is not viable with real fees. pnl no fees: \~$17/hour. pnl with fees (binance spot taker 0.1%): \~-$215/hour. round trip cost is \~$0.12 per trade, 10x the ev. assumes perfect execution at best bid/ask, in practice slippage and latency would further reduce returns.

## stack

rust, python

## run

install rust: https://rustup.rs

```bash
git clone https://github.com/rayylii/btc-obi-strategy.git
cd btc-obi-strategy
pip install pandas matplotlib scipy jupyter
cargo run
```

to analyse results:

```bash
jupyter notebook analysis.ipynb
```

session duration and signal thresholds are configurable via constants at the top of `src/main.rs`. run `analysis.ipynb` to analyse results. use `analyse(df, session_id)` to analyse any session.