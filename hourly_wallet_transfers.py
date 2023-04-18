from google.cloud import bigquery
from google.oauth2 import service_account
import sqlite3

goog_creds = "REPLACE_WITH_PATH_TO_GOOGLE_CREDENTIALS"
goog_id = "REPLACE_WITH_GOOGLE_PROJECT_ID"

def get_wallet_transfers(wallet_address, token_address, creds=goog_creds):
    credentials = service_account.Credentials.from_service_account_file(creds)
    project_id = goog_id
    client = bigquery.Client(credentials=credentials, project=project_id)
    query_job = client.query(f"""
        with transaction_addresses as (
          SELECT from_address, to_address, block_timestamp, CAST(value as int)/1000000 as value
          FROM `bigquery-public-data.crypto_ethereum.token_transfers` 
          WHERE token_address = '{token_address}'
            and (from_address = '{wallet_address}'
                  or to_address = '{wallet_address}')
              and DATE(block_timestamp) > "2023-03-18"
        ),

        out_addresses as (
          SELECT from_address, timestamp_trunc(block_timestamp, HOUR) as hour, SUM(-1*value) as hourly_change
          FROM transaction_addresses
          WHERE from_address = '{wallet_address}'
          GROUP BY from_address, timestamp_trunc(block_timestamp, HOUR)
        ),

        in_addresses as (
          SELECT to_address, timestamp_trunc(block_timestamp, HOUR) as hour, SUM(value) as hourly_change
          FROM transaction_addresses
          WHERE to_address = '{wallet_address}'
          GROUP BY to_address, timestamp_trunc(block_timestamp, HOUR)
        ),

        all_addresses as (
          SELECT from_address as address, hour, hourly_change
          FROM out_addresses

          UNION ALL
          SELECT to_address as address, hour, hourly_change
          FROM in_addresses
        )

        SELECT address, hour, sum(hourly_change) as hourly_change
        FROM all_addresses
        GROUP BY address, hour
        ORDER BY address, hour desc
    """)

    results = query_job.result()

    return [r for r in results]