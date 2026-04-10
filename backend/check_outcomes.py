import sys
sys.path.insert(0, r'C:\Users\visra\OneDrive\Desktop\trading_adv\nse_edge_live\backend')
from feed import get_kite
from config import KITE_TOKENS

kite = get_kite()

signals = [
    ('INFY', '13:23', 1279.0, 1275.2, 1282.8, 1293.0, True),
    ('LT', '15:00', 3610.9, 3600.1, 3621.7, 3633.5, True),
    ('SBIN', '15:05', 1018.2, 1015.1, 1021.2, 1024.2, True),
    ('TATASTEEL', '15:19', 193.8, 194.4, 193.2, 192.6, False),
    ('INFY', '15:22', 1296.9, 1300.8, 1293.0, 1289.1, False),
]

print("Signal Outcomes - April 2, 2026")
print("=" * 80)

for sym, time, entry, sl, t1, t2, is_buy in signals:
    token = KITE_TOKENS[sym]
    candles = kite.historical_data(token, '2026-04-02', '2026-04-02', 'minute')
    
    sig_idx = None
    for i, c in enumerate(candles):
        if c['date'].strftime('%H:%M') == time:
            sig_idx = i + 1
            break
    
    if sig_idx is None or sig_idx >= len(candles):
        print(f"{sym:12} {time} - No entry candle")
        continue
    
    reached_t1 = False
    reached_t2 = False
    hit_sl = False
    exit_price = None
    exit_time = None
    outcome = None
    
    for j in range(sig_idx, min(sig_idx + 45, len(candles))):
        c = candles[j]
        
        if is_buy:
            if c['low'] <= sl:
                hit_sl = True
                exit_price = sl
                exit_time = c['date'].strftime('%H:%M')
                outcome = "SL HIT"
                break
            if c['high'] >= t1:
                reached_t1 = True
            if c['high'] >= t2:
                reached_t2 = True
                exit_price = t2
                exit_time = c['date'].strftime('%H:%M')
                outcome = "T2 HIT"
                break
        else:
            if c['high'] >= sl:
                hit_sl = True
                exit_price = sl
                exit_time = c['date'].strftime('%H:%M')
                outcome = "SL HIT"
                break
            if c['low'] <= t1:
                reached_t1 = True
            if c['low'] <= t2:
                reached_t2 = True
                exit_price = t2
                exit_time = c['date'].strftime('%H:%M')
                outcome = "T2 HIT"
                break
    
    if not outcome:
        last_c = candles[-1]
        exit_price = last_c['close']
        exit_time = last_c['date'].strftime('%H:%M')
        outcome = "EXPIRED"
    
    pnl = (exit_price - entry) if is_buy else (entry - exit_price)
    pnl_pct = (pnl / entry) * 100
    
    result = "T2 HIT" if reached_t2 else "T1 HIT" if reached_t1 else "SL HIT" if hit_sl else "EXPIRED"
    print(f"{sym:12} {time} | {result:8} | Entry:{entry:.1f} -> Exit:{exit_price:.1f} | {pnl:+.2f} ({pnl_pct:+.3f}%)")

print("=" * 80)
