import sys
sys.path.insert(0, r'C:\Users\visra\OneDrive\Desktop\trading_adv\nse_edge_live\backend')
import backtest_data as bd
bd.init_db()
conn = bd.get_conn()
tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
print('Tables:', [t[0] for t in tables])
conn.close()
