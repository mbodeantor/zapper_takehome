with transaction_addresses as (
  SELECT t.from_address, t.to_address
  FROM (
    select transaction_hash, transaction_index
    from `bigquery-public-data.crypto_ethereum.logs`
    where address = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
--Future work would be to make date window dynamic
      and DATE(block_timestamp) > "2023-03-17") l
  join `bigquery-public-data.crypto_ethereum.transactions` t
  on t.hash = l.transaction_hash and t.transaction_index = l.transaction_index
  where DATE(t.block_timestamp) > "2023-03-17"
  ),

from_addresses as (
  SELECT from_address as address, sum(1) as transaction_count
  FROM transaction_addresses
  GROUP BY from_address
),

to_addresses as (
  SELECT to_address as address, sum(1) as transaction_count
  FROM transaction_addresses
  GROUP BY to_address
),

all_addresses as (
  SELECT * from from_addresses
  UNION ALL
  SELECT * from to_addresses
)

SELECT address, sum(transaction_count) as full_count
FROM all_addresses
GROUP BY address
ORDER BY full_count desc
LIMIT 10