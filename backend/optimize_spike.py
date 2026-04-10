import sys
sys.path.insert(0, r'C:\Users\visra\OneDrive\Desktop\trading_adv\nse_edge_live\backend')

from feed import get_kite
from config import KITE_TOKENS, FNO_SYMBOLS
from datetime import datetime, timedelta
import itertools

kite = get_kite()

# Get all FNO stocks
all_symbols = [s for s in FNO_SYMBOLS if s in KITE_TOKENS and s not in ('INDIAVIX', 'NIFTY', 'BANKNIFTY')]
print(f"Total F&O stocks: {len(all_symbols)}")

# Date range: last 30 trading days
end_date = '2026-04-02'
start_date = '2026-02-01'

print(f"Date range: {start_date} to {end_date}")
print("=" * 80)

# Parameter grid to test
vol_mins = [1.5, 2.0, 2.5]
price_mins = [0.2, 0.3, 0.4]
confirm_thresholds = [0.0, 0.02, 0.05]
time_windows = [(555, 870), (570, 840), (570, 810)]  # 9:15-14:30, 9:30-14:00, 9:30-13:30

best_results = {
    'params': None,
    'total': 0,
    'wins': 0,
    'wr': 0,
    'pf': 0
}

param_combinations = list(itertools.product(vol_mins, price_mins, confirm_thresholds, time_windows))
print(f"Testing {len(param_combinations)} parameter combinations...")

results_by_params = []

for idx, (vol_min, price_min, confirm_thresh, time_window) in enumerate(param_combinations):
    if idx % 20 == 0:
        print(f"Progress: {idx}/{len(param_combinations)}")
    
    all_signals = []
    
    for sym in all_symbols[:20]:  # Start with 20 stocks for speed
        try:
            token = KITE_TOKENS[sym]
            candles = kite.historical_data(token, start_date, end_date, 'minute')
            
            if not candles or len(candles) < 25:
                continue
            
            avg_vol = sum(c['volume'] for c in candles[:20]) / 20
            
            prev_date = None
            for i in range(20, len(candles) - 1):
                c = candles[i]
                
                # Day change for cooldown
                curr_date = c['date'].strftime('%Y-%m-%d')
                if curr_date != prev_date:
                    prev_date = curr_date
                
                cm = c['date'].hour * 60 + c['date'].minute
                
                # Time window filter
                if cm < time_window[0] or cm > time_window[1]:
                    continue
                
                vm = c['volume'] / avg_vol if avg_vol > 0 else 0
                cp = (c['close'] - c['open']) / c['open'] * 100 if c['open'] else 0
                
                # Basic filters
                if vm < vol_min or abs(cp) < price_min:
                    continue
                
                # Early candle special rule
                if cm < 560 and not (abs(cp) >= 0.7 and vm >= 3.5):
                    continue
                
                # Confirmation filter
                cn = candles[i + 1]
                cn_cp = (cn['close'] - cn['open']) / cn['open'] * 100 if cn['open'] else 0
                
                is_buy = cp > 0
                required = confirm_thresh if is_buy else -confirm_thresh
                if not (cn_cp > required if is_buy else cn_cp < required):
                    continue
                
                # Entry at next candle open
                entry = cn['open']
                t1 = entry * 1.003 if is_buy else entry * 0.997
                sl = entry * 0.997 if is_buy else entry * 1.003
                
                # Check outcome
                win = False
                loss = False
                pnl = 0
                
                for j in range(i + 2, min(i + 46, len(candles))):
                    nc = candles[j]
                    if nc['date'].strftime('%Y-%m-%d') != curr_date:
                        break
                    
                    if is_buy:
                        if nc['low'] <= sl:
                            loss = True
                            pnl = (sl - entry) / entry * 100
                            break
                        if nc['high'] >= t1:
                            win = True
                            pnl = (t1 - entry) / entry * 100
                            break
                    else:
                        if nc['high'] >= sl:
                            loss = True
                            pnl = (entry - sl) / entry * 100
                            break
                        if nc['low'] <= t1:
                            win = True
                            pnl = (entry - t1) / entry * 100
                            break
                
                if win or loss:
                    all_signals.append({'win': win, 'loss': loss, 'pnl': pnl})
        
        except Exception as e:
            pass
    
    if len(all_signals) >= 10:
        wins = sum(1 for s in all_signals if s['win'])
        total = len(all_signals)
        wr = wins / total * 100
        avg_win = sum(s['pnl'] for s in all_signals if s['win']) / max(wins, 1) if wins else 0
        avg_loss = abs(sum(s['pnl'] for s in all_signals if s['loss']) / max(sum(1 for s in all_signals if s['loss']), 1)) if sum(1 for s in all_signals if s['loss']) else 0
        pf = avg_win / avg_loss if avg_loss > 0 else 0
        
        results_by_params.append({
            'vol_min': vol_min,
            'price_min': price_min,
            'confirm': confirm_thresh,
            'time_start': time_window[0],
            'time_end': time_window[1],
            'total': total,
            'wins': wins,
            'wr': wr,
            'pf': pf
        })
        
        if wr > best_results['wr'] and total >= 20:
            best_results = {
                'params': (vol_min, price_min, confirm_thresh, time_window),
                'total': total,
                'wins': wins,
                'wr': wr,
                'pf': pf
            }

# Sort by win rate
results_by_params.sort(key=lambda x: (-x['wr'], -x['total']))

print("\n" + "=" * 80)
print("TOP 15 PARAMETER COMBINATIONS (by Win Rate):")
print("-" * 80)
print(f"{'Vol':>5} {'Price':>6} {'Conf':>5} {'Time':>10} {'Total':>6} {'Wins':>5} {'WR%':>6} {'PF':>5}")
print("-" * 80)

for r in results_by_params[:15]:
    time_str = f"{r['time_start']//60}:{r['time_start']%60:02d}-{r['time_end']//60}:{r['time_end']%60:02d}"
    print(f"{r['vol_min']:>5.1f} {r['price_min']:>6.2f} {r['confirm']:>5.2f} {time_str:>10} {r['total']:>6} {r['wins']:>5} {r['wr']:>6.1f} {r['pf']:>5.2f}")

print("\n" + "=" * 80)
print("BEST PARAMETERS:")
if best_results['params']:
    print(f"  Vol Min: {best_results['params'][0]}")
    print(f"  Price Min: {best_results['params'][1]}")
    print(f"  Confirm: {best_results['params'][2]}")
    print(f"  Time: {best_results['params'][3][0]//60}:{best_results['params'][3][0]%60:02d} - {best_results['params'][3][1]//60}:{best_results['params'][3][1]%60:02d}")
    print(f"  Win Rate: {best_results['wr']:.1f}%")
    print(f"  Total Signals: {best_results['total']}")
    print(f"  Profit Factor: {best_results['pf']:.2f}")
