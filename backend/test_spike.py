import sys
sys.path.insert(0, r'C:\Users\visra\OneDrive\Desktop\trading_adv\nse_edge_live\backend')
from feed import get_kite
from config import KITE_TOKENS

kite = get_kite()
token = KITE_TOKENS['SBIN']
candles = kite.historical_data(token, '2026-03-27', '2026-03-27', 'minute')
avg_vol = sum(c['volume'] for c in candles[:20]) / 20

print(f"Avg vol: {avg_vol}")
print(f"Total candles: {len(candles)}")
print()

passing = 0
for c in candles:
    vm = c['volume'] / avg_vol
    cp = abs((c['close'] - c['open']) / c['open'] * 100)
    cm = c['date'].hour * 60 + c['date'].minute
    
    # Filter 1: vol range
    if vm < 2.5 or vm > 7.0:
        continue
    # Filter 2: price range
    if cp < 0.4 or cp > 1.5:
        continue
    # Filter 3: first 5 min special rule
    if cm < 560 and not (cp >= 0.7 and vm >= 3.5):
        continue
    
    passing += 1
    print(f"{c['date'].strftime('%H:%M')} Vol:{vm:.1f}x Chg:{cp:.2f}%")

print(f"\nTotal passing filters: {passing}")
