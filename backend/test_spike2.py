import sys
sys.path.insert(0, r'C:\Users\visra\OneDrive\Desktop\trading_adv\nse_edge_live\backend')
from feed import get_kite
from config import KITE_TOKENS

kite = get_kite()
token = KITE_TOKENS['SBIN']
candles = kite.historical_data(token, '2026-03-27', '2026-03-27', 'minute')
avg_vol = sum(c['volume'] for c in candles[:20]) / 20

print(f"Avg vol: {avg_vol:.0f}")
print()

for c in candles[:10]:
    vm = c['volume'] / avg_vol
    cp = abs((c['close'] - c['open']) / c['open'] * 100) if c['open'] else 0
    cm = c['date'].hour * 60 + c['date'].minute
    print(f"{c['date'].strftime('%H:%M')} Vol:{c['volume']:>8} ({vm:.1f}x) Chg:{cp:.2f}% cm:{cm}")

print("\n--- Checking filter thresholds ---")
for c in candles:
    vm = c['volume'] / avg_vol
    if vm < 2.5:
        continue
    cp = abs((c['close'] - c['open']) / c['open'] * 100) if c['open'] else 0
    cm = c['date'].hour * 60 + c['date'].minute
    
    reason = []
    if vm < 2.5: reason.append(f"vol_low:{vm:.1f}")
    if vm > 7.0: reason.append(f"vol_high:{vm:.1f}")
    if cp < 0.4: reason.append(f"chg_low:{cp:.2f}")
    if cp > 1.5: reason.append(f"chg_high:{cp:.2f}")
    if cm < 560 and not (cp >= 0.7 and vm >= 3.5): reason.append(f"open_noise:cm={cm},cp={cp:.2f},vm={vm:.1f}")
    
    if not reason:
        print(f"PASS: {c['date'].strftime('%H:%M')} Vol:{vm:.1f}x Chg:{cp:.2f}%")
    elif vm > 2.0 and cp > 0.3:
        print(f"FAIL: {c['date'].strftime('%H:%M')} Vol:{vm:.1f}x Chg:{cp:.2f}% -> {', '.join(reason)}")
