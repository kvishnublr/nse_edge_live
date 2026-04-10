import sys
sys.path.insert(0, r'C:\Users\visra\OneDrive\Desktop\trading_adv\nse_edge_live\backend')
from feed import get_kite
from config import KITE_TOKENS

kite = get_kite()

# Check TATASTEEL 09:15 spike
sym = 'TATASTEEL'
token = KITE_TOKENS[sym]
candles = kite.historical_data(token, '2026-04-02', '2026-04-02', 'minute')

print(f"{sym} - April 2, 2026")
print("=" * 60)

# Show 09:15 to 10:00 candles
for c in candles[:50]:
    time = c['date'].strftime('%H:%M')
    if time < "10:00":
        vm = c['volume'] / (sum(x['volume'] for x in candles[:20]) / 20)
        cp = (c['close'] - c['open']) / c['open'] * 100
        marker = " <<<" if vm > 2 and abs(cp) > 0.5 else ""
        print(f"{time} O:{c['open']:.1f} H:{c['high']:.1f} L:{c['low']:.1f} C:{c['close']:.1f} Vol:{c['volume']} ({vm:.1f}x) Chg:{cp:+.2f}%{marker}")
