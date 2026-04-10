import sys
sys.path.insert(0, r'C:\Users\visra\OneDrive\Desktop\trading_2\edge_live\backend')

from feed import get_kite
from config import KITE_TOKENS, FNO_SYMBOLS

kite = get_kite()
symbols = [s for s in FNO_SYMBOLS if s in KITE_TOKENS and s not in ('INDIAVIX', 'NIFTY', 'BANKNIFTY')]

print(f"F&O Stocks: {len(symbols)}")

# Test with different parameters
results = []

# Quick test: different vol/price thresholds
configs = [
    (1.5, 0.2, 0.0),   # Relaxed
    (1.8, 0.25, 0.0),  # Balanced
    (2.0, 0.3, 0.0),    # Current style
    (1.5, 0.2, 0.02),  # With confirmation
    (2.0, 0.3, 0.02),  # Strict with confirmation
]

for vol_min, price_min, confirm in configs:
    all_signals = []
    
    for sym in symbols[:30]:  # Test 30 stocks
        try:
            candles = kite.historical_data(KITE_TOKENS[sym], '2026-03-01', '2026-04-02', 'minute')
            if not candles or len(candles) < 25:
                continue
            
            avg_vol = sum(c['volume'] for c in candles[:20]) / 20
            
            for i in range(20, len(candles) - 5):
                c = candles[i]
                cm = c['date'].hour * 60 + c['date'].minute
                
                # Time: 9:30 - 14:00
                if cm < 570 or cm > 840:
                    continue
                
                vm = c['volume'] / avg_vol if avg_vol > 0 else 0
                cp = (c['close'] - c['open']) / c['open'] * 100 if c['open'] else 0
                
                if vm < vol_min or abs(cp) < price_min:
                    continue
                
                # Confirmation
                cn = candles[i + 1]
                cn_cp = (cn['close'] - cn['open']) / cn['open'] * 100 if cn['open'] else 0
                is_buy = cp > 0
                
                if confirm > 0:
                    if is_buy and cn_cp < confirm:
                        continue
                    if not is_buy and cn_cp > -confirm:
                        continue
                
                # Entry at next candle
                entry = cn['open']
                t1 = entry * 1.003 if is_buy else entry * 0.997
                sl = entry * 0.997 if is_buy else entry * 1.003
                
                # Check outcome
                win = loss = False
                for j in range(i + 2, min(i + 46, len(candles))):
                    nc = candles[j]
                    if nc['date'].strftime('%Y-%m-%d') != c['date'].strftime('%Y-%m-%d'):
                        break
                    
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
                
                if win or loss:
                    all_signals.append(1 if win else 0)
        
        except Exception as e:
            pass
    
    total = len(all_signals)
    wins = sum(all_signals) if all_signals else 0
    wr = wins / total * 100 if total > 0 else 0
    
    results.append({
        'vol': vol_min,
        'price': price_min,
        'confirm': confirm,
        'total': total,
        'wins': wins,
        'wr': wr
    })

print("\nPARAMETER OPTIMIZATION RESULTS:")
print("=" * 70)
print(f"{'Vol':>5} {'Price':>6} {'Confirm':>8} {'Total':>7} {'Wins':>6} {'WR%':>6}")
print("-" * 70)

results.sort(key=lambda x: -x['wr'])
for r in results:
    print(f"{r['vol']:>5.2f} {r['price']:>6.2f} {r['confirm']:>8.2f} {r['total']:>7} {r['wins']:>6} {r['wr']:>6.1f}")

print("\nBEST CONFIG:", results[0] if results else "No results")
