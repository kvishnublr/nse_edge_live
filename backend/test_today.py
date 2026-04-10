import sys
sys.path.insert(0, r'C:\Users\visra\OneDrive\Desktop\trading_adv\nse_edge_live\backend')
from feed import get_kite
from config import KITE_TOKENS

kite = get_kite()
token = KITE_TOKENS['SBIN']
candles = kite.historical_data(token, '2026-04-02', '2026-04-02', 'minute')
avg_vol = sum(c['volume'] for c in candles[:20]) / 20

print(f"Date: April 2, 2026")
print(f"Avg vol: {avg_vol:.0f}")
print(f"Total candles: {len(candles)}")
print()

# Check for spikes
for c in candles:
    vm = c['volume'] / avg_vol
    if vm < 1.5:
        continue
    cp = abs((c['close'] - c['open']) / c['open'] * 100) if c['open'] else 0
    print(f"{c['date'].strftime('%H:%M')} Vol:{vm:.1f}x Chg:{cp:.2f}%")
