import sys
sys.path.insert(0, r'C:\Users\visra\OneDrive\Desktop\trading_adv\nse_edge_live\backend')
from feed import get_kite
from config import KITE_TOKENS, FNO_SYMBOLS

kite = get_kite()
symbols = [s for s in FNO_SYMBOLS if s in KITE_TOKENS and s not in ('INDIAVIX', 'NIFTY', 'BANKNIFTY')]

print("Checking ALL spikes with confirmation filter...")
print("=" * 70)

total_confirmed = 0
all_results = []

for sym in symbols:
    try:
        token = KITE_TOKENS[sym]
        candles = kite.historical_data(token, '2026-04-02', '2026-04-02', 'minute')
        
        if not candles or len(candles) < 25:
            continue
        
        avg_vol = sum(c['volume'] for c in candles[:20]) / 20
        
        for i in range(20, len(candles) - 1):
            c = candles[i]
            cn = candles[i + 1]
            
            vm = c['volume'] / avg_vol if avg_vol > 0 else 0
            cp = (c['close'] - c['open']) / c['open'] * 100 if c['open'] else 0
            cm = c['date'].hour * 60 + c['date'].minute
            
            # Basic filters
            if vm < 1.5 or abs(cp) < 0.3:
                continue
            if cm < 560 and not (abs(cp) >= 0.7 and vm >= 3.5):
                continue
            
            # Confirmation: next candle must continue in same direction
            is_buy = cp > 0
            cn_cp = (cn['close'] - cn['open']) / cn['open'] * 100 if cn['open'] else 0
            
            confirmed = False
            if is_buy and cn_cp > 0.05:
                confirmed = True
            if not is_buy and cn_cp < -0.05:
                confirmed = True
            
            direction = "BUY" if is_buy else "SELL"
            result = "CONFIRMED" if confirmed else "REJECTED"
            
            if confirmed:
                total_confirmed += 1
                entry = cn['open']
                sl = round(entry * (0.997 if is_buy else 1.003), 2)
                t1 = round(entry * (1.003 if is_buy else 0.997), 2)
                t2 = round(entry * (1.006 if is_buy else 0.994), 2)
                
                print(f"{sym:12} {c['date'].strftime('%H:%M')} {direction:4} Vol:{vm:.1f}x Chg:{cp:+.2f}% -> Entry:{entry:.1f} SL:{sl:.1f} T1:{t1:.1f} [{result}]")
    except Exception as e:
        pass

print()
print("=" * 70)
print(f"TOTAL CONFIRMED SPIKES TODAY: {total_confirmed}")
