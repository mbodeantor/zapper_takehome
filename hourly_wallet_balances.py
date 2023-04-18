from rpc_wallet import get_wallet_balance
from hourly_wallet_transfers import get_wallet_transfers
from datetime import datetime, timedelta
import sqlite3

token_address = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
wallet_address = "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"
# Hardcoding token decimals for USDC for now, future work would include pulling this from the RPC
current_balance = get_wallet_balance(wallet_address, token_address, 6)

con = sqlite3.connect('wallet_balances.db')
cur = con.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS hourly_balances (id integer PRIMARY KEY, wallet_address text, token_address text, datetime text, balance real)")
transfers = get_wallet_transfers(wallet_address, token_address)

# Looks like data from the BigQuery API is a few hours delayed, future work would include pulling the rest of the token
# transfers from the RPC to fill the gap. For now, just assigning the current balance from the RPC to the first hour
# after the BigQuery data
start_time = transfers[0][1] + timedelta(hours=1)
cur.execute("INSERT INTO hourly_balances (wallet_address, token_address, datetime, balance) Values (?, ?, ?, ?)", (wallet_address, token_address, start_time, current_balance))

# Less active wallet addresses won't have data for every hour, future work would include filling in the hour gaps to
# show the balance didn't change
for transfer in transfers:
    current_balance += transfer[2]
    cur.execute("INSERT INTO hourly_balances (wallet_address, token_address, datetime, balance) Values (?, ?, ?, ?)", (transfer[0], token_address, transfer[1], current_balance))

con.commit()
results = cur.execute('SELECT * FROM hourly_balances').fetchall()
print([r for r in results])