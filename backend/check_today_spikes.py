import sys
sys.path.insert(0, r'C:\Users\visra\OneDrive\Desktop\trading_adv\nse_edge_live\backend')
from feed import get_kite
from config import KITE_TOKENS, FNO_SYMBOLS

kite = get_kite()

# Get all stocks and check for spikes manually
symbols = [s for s in FNO_SYMBOLS if s in KITE_TOKENS and s not in ('INDIAVIX', 'NIFTY', 'BANKNIFTY')]

print("Scanning today's 1-min data for spikes...")
print("=" * 60)

all_spikes = []

for sym in symbols:
    try:
        token = KITE_TOKENS[sym]
        candles = kite.historical_data(token, '2026-04-02', '2026-04-02', 'minute')
        
        if not candles or len(candles) < 20:
            continue
            
        avg_vol = sum(c['volume'] for c in candles[:20]) / 20
        
        for c in candles:
            vm = c['volume'] / avg_vol if avg_vol > 0 else 0
            cp = abs((c['close'] - c['open']) / c['open'] * 100) if c['open'] else 0
            cm = c['date'].hour * 60 + c['date'].minute
            
            # Check if qualifies as spike (relaxed filters for live display)
            if vm >= 1.5 and cp >= 0.3:
                direction = "BUY" if (c['close'] > c['open']) else "SELL"
                all_spikes.append({
                    'symbol': sym,
                    'time': c['date'].strftime('%H:%M'),
                    'type': direction,
                    'vol_mult': round(vm, 1),
                    'chg_pct': round(cp, 2),
                    'price': c['close'],
                    'candle_min': cm
                })
    except Exception as e:
        pass

# Sort by volume multiplier
all_spikes.sort(key=lambda x: (-x['vol_mult'], -x['candle_min']))

print(f"\nFound {len(all_spikes)} potential spikes today:")
print("=" * 60)

for s in all_spikes[:20]:
    print(f"{s['symbol']:12} {s['time']} {s['type']:4} Vol:{s['vol_mult']:.1f}x Chg:{s['chg_pct']:.2f}% Price:{s['price']}")
