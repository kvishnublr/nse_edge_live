import sys
sys.path.insert(0, r'C:\Users\visra\OneDrive\Desktop\trading_adv\nse_edge_live\backend')
from feed import get_kite
from config import KITE_TOKENS, FNO_SYMBOLS

kite = get_kite()
symbols = [s for s in FNO_SYMBOLS if s in KITE_TOKENS and s not in ('INDIAVIX', 'NIFTY', 'BANKNIFTY')]

print("Analyzing confirmation filter effectiveness...")
print("=" * 80)

# Test different confirmation thresholds
thresholds = [
    (0.0, 0.0),    # No confirmation needed
    (0.01, -0.01), # Any move in direction
    (0.05, -0.05), # Small confirmation
    (0.10, -0.10), # Moderate confirmation
    (0.15, -0.15), # Strict confirmation
]

results = {t: {"total": 0, "wins": 0, "losses": 0} for t in range(len(thresholds))}

for sym in symbols[:10]:  # Quick test on 10 symbols
    try:
        token = KITE_TOKENS[sym]
        candles = kite.historical_data(token, '2026-04-02', '2026-04-02', 'minute')
        
        if not candles or len(candles) < 25:
            continue
        
        avg_vol = sum(c['volume'] for c in candles[:20]) / 20
        
        for i in range(20, len(candles) - 5):
            c = candles[i]
            cn = candles[i + 1]
            
            vm = c['volume'] / avg_vol if avg_vol > 0 else 0
            cp = (c['close'] - c['open']) / c['open'] * 100 if c['open'] else 0
            
            if vm < 1.5 or abs(cp) < 0.3:
                continue
            
            is_buy = cp > 0
            cn_cp = (cn['close'] - cn['open']) / cn['open'] * 100 if cn['open'] else 0
            
            for t_idx, (buy_thresh, sell_thresh) in enumerate(thresholds):
                required = buy_thresh if is_buy else sell_thresh
                confirmed = cn_cp >= required if is_buy else cn_cp <= required
                
                if confirmed:
                    results[t_idx]["total"] += 1
                    
                    # Check outcome (T1 = +0.3%, SL = -0.25%)
                    entry = cn['open']
                    t1 = entry * 1.003 if is_buy else entry * 0.997
                    sl = entry * 0.997 if is_buy else entry * 1.003
                    
                    win = False
                    loss = False
                    for j in range(i + 2, min(i + 45, len(candles))):
                        nc = candles[j]
                        if is_buy:
                            if nc['low'] <= sl:
                                loss = True
                                break
                            if nc['high'] >= t1:
                                win = True
                                break
                        else:
                            if nc['high'] >= sl:
                                loss = True
                                break
                            if nc['low'] <= t1:
                                win = True
                                break
                    
                    if win:
                        results[t_idx]["wins"] += 1
                    elif loss:
                        results[t_idx]["losses"] += 1

    except Exception as e:
        pass

print("\nConfirmation Filter Analysis:")
print("-" * 80)
for t_idx, (buy_thresh, sell_thresh) in enumerate(thresholds):
    r = results[t_idx]
    total = r["total"]
    if total > 0:
        wr = r["wins"] / total * 100
        print(f"Threshold {buy_thresh:+.2f}%: {total:3d} signals | WR: {wr:.1f}% | {r['wins']}W {r['losses']}L")
    else:
        print(f"Threshold {buy_thresh:+.2f}%: 0 signals")
