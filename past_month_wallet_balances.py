from google.cloud import bigquery
from google.oauth2 import service_account
import sqlite3

goog_creds = "REPLACE_WITH_PATH_TO_GOOGLE_CREDENTIALS"
goog_id = "REPLACE_WITH_GOOGLE_PROJECT_ID"

def get_wallet_balances(token_address, creds=goog_creds):
    credentials = service_account.Credentials.from_service_account_file(creds)
    project_id = goog_id
    client = bigquery.Client(credentials=credentials, project=project_id)
    query_job = client.query(f"""
        with transaction_addresses as (
          SELECT from_address, to_address, CAST(value as int)/1000000 as value
          FROM `bigquery-public-data.crypto_ethereum.token_transfers` 
          WHERE token_address = '{token_address}'
              and DATE(block_timestamp) > "2023-03-17"
        ),

        out_addresses as (
          SELECT from_address, SUM(-1*value) as total_value
          FROM transaction_addresses
          GROUP BY from_address
        ),

        in_addresses as (
          SELECT to_address, SUM(value) as total_value
          FROM transaction_addresses
          GROUP BY to_address
        ),

        all_addresses as (
          SELECT from_address as address, total_value
          FROM out_addresses

          UNION ALL
          SELECT to_address as address, total_value
          FROM in_addresses
        )

        SELECT address, sum(total_value) as balance
        FROM all_addresses
        GROUP BY address
        HAVING sum(total_value) > 0
        LIMIT 10
    """)

    results = query_job.result()

    return results #[r for r in results]

con = sqlite3.connect('wallet_balances.db')
curs = con.cursor()
curs.execute("Drop table balances")
curs.execute("CREATE TABLE IF NOT EXISTS balances (id integer PRIMARY KEY, wallet_address text, token_address text, balance real)")
token_address = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
balances = get_wallet_balances(token_address)
for b in balances:
    curs.execute("INSERT INTO balances (wallet_address, token_address, balance) Values (?, ?, ?)", (b[0], token_address, b[1]))

con.commit()
results = curs.execute('SELECT * FROM balances').fetchall()
print([r for r in results])