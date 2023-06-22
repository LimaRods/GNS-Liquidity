import pandas as pd
import requests as re
#--------------------------------------- FLIPSIDE QUERIES ------------------------------------------------------------------#
# Queries used to extract the desired on-chain information from Flipside app
# There are variables in some scripts below like the start date of the query and the period for aggregating (day,week,month)

# Swap on Arbitrum by Liquidity Pools. Considering only relevant pools for GNS (Uniswap
swap_arb_pools = """
WITH
  pool_created AS (
    SELECT
      *,
      (
        CASE
          WHEN pool_address = LOWER('0xC91B7b39BBB2c733f0e7459348FD0c80259c8471') THEN 'GNS-ETH 0.3% ARB'
          WHEN pool_address = LOWER('0xfB30135d5bDe908b88E5422baa6093065304D98b') THEN 'GNS-ETH 1% ARB'
          ELSE token0_symbol || '-' || token1_symbol || ' 1% ARB'
        END
      ) AS pool_name
    FROM
      (
        SELECT
          'Arbitrum' AS blockchain,
          'uniswap-v3' AS platform,
          decoded_log:pool::STRING AS pool_address,
          decoded_log:token0::STRING as token0_address,
          decoded_log:token1::STRING as token1_address,
          'GNS' AS token0_symbol,
          (
            CASE
              WHEN decoded_log:token1::STRING = LOWER('0x82aF49447D8a07e3bd95BD0d56f35241523fBab1') THEN 'WETH'
              WHEN decoded_log:token1::STRING = LOWER('0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8') THEN 'USDC'
              WHEN decoded_log:token1::STRING = LOWER('0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1') THEN 'DAI'
            END
          ) AS token1_symbol,
          (
            CASE
              WHEN decoded_log:token1::STRING = LOWER('0xC91B7b39BBB2c733f0e7459348FD0c80259c8471') THEN 0.003 -- GNS/ETH 0.3%
              ELSE 0.01 -- GNS/DAI 1% , GNS/USDC 1% , GNS/ETH 1%
            END
          ) AS fee
        FROM
          arbitrum.core.fact_decoded_event_logs
        WHERE
          EVENT_NAME = 'PoolCreated'
          AND DECODED_LOG:pool::STRING IN (
            LOWER('0xC91B7b39BBB2c733f0e7459348FD0c80259c8471'), -- GNS/ETH 0.3%
            LOWER('0x4d2fE06Fd1c4368042B926D082484D2E3cC8F3F5'), -- GNS/DAI 1%
            LOWER('0x8D76e9c2bD1aDDE00A3DcDC315Fcb2774Cb3D1D6'), -- GNS/USDC 1%
            LOWER('0xfB30135d5bDe908b88E5422baa6093065304D98b') -- GNS/ETH 1%
          )
      ) AS x
  ),
  swap_raw_table AS (
    SELECT
      tx_hash,
      date_trunc('hour', block_timestamp) as hour,
      pool.pool_address,
      decoded_log:recipient::STRING as recipient,
      token0_address,
      token1_address,
      token0_symbol,
      token1_symbol,
      {0},
      fee,
      decoded_log:amount0::INTEGER as amount0,
      decoded_log:amount1::INTEGER as amount1
    FROM
      arbitrum.core.fact_decoded_event_logs event
      LEFT JOIN pool_created pool ON event.contract_address = pool.pool_address
    WHERE
      EVENT_NAME LIKE '%Swap%'
      -- Uniswap V3 Swap Router
      --AND DECODED_LOG: sender:: STRING = LOWER('0xE592427A0AEce92De3Edee1F18E0157C05861564')
      AND event.contract_address IN (
        LOWER('0xC91B7b39BBB2c733f0e7459348FD0c80259c8471'), -- GNS/ETH 0.3%
        LOWER('0x4d2fE06Fd1c4368042B926D082484D2E3cC8F3F5'), -- GNS/DAI
        LOWER('0x8D76e9c2bD1aDDE00A3DcDC315Fcb2774Cb3D1D6'), -- GNS/USDC 1%
        LOWER('0xfB30135d5bDe908b88E5422baa6093065304D98b') -- GNS/ETH 1%
      )
  ),
  -- PRICES
  swap_adj AS (
    SELECT
      tx_hash,
      swap.hour,
      pool_address,
      recipient,
      token0_symbol,
      token1_symbol,
      {0},
      fee,
      (amount0 / POW(10, price0.decimals)) * price0.price as amount0_usd,
      (amount1 / POW(10, price1.decimals)) * price1.price as amount1_usd
    FROM
      swap_raw_table swap
      JOIN arbitrum.core.fact_hourly_token_prices price0 ON token0_address = price0.token_address
      AND swap.hour = price0.hour
      JOIN arbitrum.core.fact_hourly_token_prices price1 ON token1_address = price1.token_address
      AND swap.hour = price1.hour
  ),
  swap_final AS (
    SELECT
      tx_hash,
      hour,
      pool_address,
      recipient,
      CASE
        WHEN amount0_usd > 0 THEN token0_symbol
        ELSE token1_symbol
      END AS token_in,
      CASE
        WHEN amount0_usd < 0 THEN token0_symbol
        ELSE token1_symbol
      END AS toke_out,
      pool_name,
      fee,
      CASE
        WHEN amount0_usd > 0 THEN amount0_usd
        ELSE amount1_usd
      END AS amount_in_usd,
      CASE
        WHEN amount0_usd < 0 THEN ABS(amount0_usd)
        ELSE ABS(amount1_usd)
      END AS amount_out_usd
    FROM
      swap_adj
  )
SELECT
  DATE_TRUNC('{1}', hour) AS date,
  {0},
  SUM(amount_in_usd) AS vol,
  SUM(amount_out_usd),
  AVG(
    (amount_in_usd * (1 - fee) - amount_out_usd) / NULLIF(amount_in_usd,0)
  ) AS avg_slippage,
  (SELECT AVG((amount_in_usd * (1 - fee) - amount_out_usd) / NULLIF(amount_in_usd,0)) FROM swap_final) as slippage_overall
FROM
  swap_final
WHERE
  date >= DATE('{2}')
GROUP BY
  date,
  {0}
ORDER BY
  date ASC, vol DESC

"""
# Swap on Arbitrum. Considering only relevant pools for GNS (Uniswap)
swap_arb = """
WITH pool_created AS (
  SELECT
  *,
  (CASE
    WHEN pool_address = LOWER('0xC91B7b39BBB2c733f0e7459348FD0c80259c8471') THEN 'GNS-ETH 0.3% ARB'
    WHEN pool_address = LOWER('0xfB30135d5bDe908b88E5422baa6093065304D98b') THEN 'GNS-ETH 1% ARB'
    ELSE token0_symbol || '-' || token1_symbol || ' ARB'
  END) AS pool_name
FROM
  (
  SELECT
      'Arbitrum' AS blockchain,
      'uniswap-v3' AS platform,
      decoded_log:pool::STRING AS pool_address,
      decoded_log:token0::STRING as token0_address,
      decoded_log:token1::STRING as token1_address,
      'GNS' AS  token0_symbol,
      (CASE
        WHEN decoded_log:token1::STRING = LOWER('0x82aF49447D8a07e3bd95BD0d56f35241523fBab1') THEN 'WETH'
        WHEN decoded_log:token1::STRING = LOWER('0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8') THEN 'USDC'
        WHEN decoded_log:token1::STRING = LOWER('0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1') THEN 'DAI'
      END) AS token1_symbol,
      (CASE
        WHEN decoded_log:token1::STRING = LOWER('0xC91B7b39BBB2c733f0e7459348FD0c80259c8471') THEN 0.003 -- GNS/ETH 0.3%
        ELSE 0.01 -- GNS/DAI 1% , GNS/USDC 1% , GNS/ETH 1%
      END) AS fee
      
  FROM
    arbitrum.core.fact_decoded_event_logs
  WHERE
    EVENT_NAME = 'PoolCreated'
    AND DECODED_LOG: pool:: STRING IN (LOWER('0xC91B7b39BBB2c733f0e7459348FD0c80259c8471'), -- GNS/ETH 0.3%
                              LOWER('0x4d2fE06Fd1c4368042B926D082484D2E3cC8F3F5'), -- GNS/DAI 1%
                              LOWER('0x8D76e9c2bD1aDDE00A3DcDC315Fcb2774Cb3D1D6'), -- GNS/USDC 1%
                              LOWER('0xfB30135d5bDe908b88E5422baa6093065304D98b') -- GNS/ETH 1%
                              )
  ) AS x
),

swap_raw_table AS (
  SELECT
    tx_hash,
    date_trunc('hour', block_timestamp) as hour ,
    pool.pool_address,
    decoded_log:recipient::STRING as recipient,
    token0_address,
    token1_address,
    token0_symbol,
    token1_symbol,
    {0},
    fee,
    decoded_log:amount0::INTEGER as amount0,
    decoded_log:amount1::INTEGER as amount1
  FROM
   arbitrum.core.fact_decoded_event_logs event
    LEFT JOIN pool_created pool ON event.contract_address = pool.pool_address
  WHERE
    EVENT_NAME LIKE '%Swap%'
    -- Uniswap V3 Swap Router
    --AND DECODED_LOG: sender:: STRING = LOWER('0xE592427A0AEce92De3Edee1F18E0157C05861564')
    AND  event.contract_address IN (LOWER('0xC91B7b39BBB2c733f0e7459348FD0c80259c8471'), -- GNS/ETH 0.3%
                              LOWER('0x4d2fE06Fd1c4368042B926D082484D2E3cC8F3F5'), -- GNS/DAI
                              LOWER('0x8D76e9c2bD1aDDE00A3DcDC315Fcb2774Cb3D1D6'), -- GNS/USDC 1%
                              LOWER('0xfB30135d5bDe908b88E5422baa6093065304D98b') -- GNS/ETH 1%
                              )
),

-- PRICES
swap_adj AS (
  SELECT
    tx_hash,
    swap.hour,
    pool_address,
    recipient,
    token0_symbol,
    token1_symbol,
    {0},
    fee,
    (amount0/POW(10,price0.decimals)) * price0.price as amount0_usd,
    (amount1/POW(10,price1.decimals)) * price1.price as amount1_usd
  
  FROM
    swap_raw_table swap
    JOIN arbitrum.core.fact_hourly_token_prices price0
      ON token0_address = price0.token_address AND swap.hour = price0.hour
    JOIN arbitrum.core.fact_hourly_token_prices price1
      ON token1_address = price1.token_address AND swap.hour = price1.hour
),

swap_final AS (
SELECT
  tx_hash,
  hour,
  pool_address,
  recipient,
  CASE WHEN amount0_usd > 0 THEN token0_symbol  ELSE token1_symbol END AS token_in,
  CASE WHEN amount0_usd < 0 THEN token0_symbol  ELSE  token1_symbol END AS toke_out,
  {0},
  fee,
  CASE WHEN amount0_usd > 0 THEN amount0_usd  ELSE amount1_usd END AS amount_in_usd,
  CASE WHEN amount0_usd < 0 THEN ABS(amount0_usd)  ELSE ABS(amount1_usd) END AS amount_out_usd

FROM
  swap_adj
)

SELECT
  DATE_TRUNC('{1}',hour) AS date,
  SUM(amount_in_usd) AS vol,
  AVG((amount_in_usd * (1-fee) - amount_out_usd)/amount_in_usd) AS slippage
  
FROM
  swap_final
WHERE
   date >= DATE('{2}')
GROUP BY date
ORDER BY date ASC, vol DESC

"""

 # Swap on Polygon by Liquidity Pools. Considering only relevant pools for GNS (Uniswap and Quickswap)

swap_matic_pool = """

WITH swap_table AS
    (
      SELECT
          DATE_TRUNC('{1}', block_timestamp) AS date,
          platform,
          {0},
          amount_in_usd,
          amount_out_usd,
          (CASE
            WHEN contract_address = LOWER('0x6E53cB6942e518376E9e763554dB1A45DDCd25c4') THEN 0.003
            WHEN contract_address = LOWER('0x384d2094D0Df788192043a1CBd200308DD60b068') THEN 0.00238
            WHEN contract_address = LOWER('0xa56796f13566c515471A2fBBAB731F88cE5DE428') THEN 0.00222
            WHEN contract_address = LOWER('0xBa0216254163B57aF68B7161cf824dBadcAD61Df') THEN 0.01
            WHEN contract_address = LOWER('0xFC469d13542E70f1512569EBf60C1E8fA01B6931') THEN 0.01
            ELSE 0.003
          END) AS fee
          
      FROM
        polygon.core.ez_dex_swaps
      WHERE
        DATE(block_timestamp) >= DATE('{2}')
        AND contract_address IN (
                                  LOWER('0x6E53cB6942e518376E9e763554dB1A45DDCd25c4'),
                                  LOWER('0x384d2094D0Df788192043a1CBd200308DD60b068'),
                                  LOWER('0xa56796f13566c515471A2fBBAB731F88cE5DE428'),
                                  LOWER('0xEFa98Fdf168f372E5e9e9b910FcDfd65856f3986'),
                                  LOWER('0xBa0216254163B57aF68B7161cf824dBadcAD61Df'),
                                  LOWER('0x32A222f69d00e717845a3D857D0392D6A25a2ACd'),
                                  LOWER('0xFC469d13542E70f1512569EBf60C1E8fA01B6931'),
                                  LOWER('0xCe0BbB1E51ee21cde86257593f29cBD9A60CA97A')
                                )
  ),

vol_table AS (
  SELECT
    date,
    {0},
    SUM(amount_in_usd) as vol,
    AVG((amount_in_usd * (1 - fee) - amount_out_usd) / NULLIF(amount_in_usd,0) * (1 - fee)) AS slippage
  FROM
    swap_table
  GROUP BY
    date,
    {0}
  ORDER BY
    date,
    vol DESC
)

SELECT
  *,
  (SELECT AVG((amount_in_usd * (1 - fee) - amount_out_usd) / NULLIF(amount_in_usd,0)) FROM swap_table) as slippage_overall,
  SUM(vol) OVER (PARTITION BY {0} ORDER BY date) as vol_cumulative
FROM
  vol_table 
 """

 # Swap on Polygon. Considering only relevant pools for GNS (Uniswap and Quickswap)

swap_matic = """

WITH swap_table AS
    (
      SELECT
          DATE_TRUNC('{1}', block_timestamp) AS date,
          amount_in_usd,
          amount_out_usd,
          (CASE
            WHEN contract_address = LOWER('0x6E53cB6942e518376E9e763554dB1A45DDCd25c4') THEN 0.003
            WHEN contract_address = LOWER('0x384d2094D0Df788192043a1CBd200308DD60b068') THEN 0.00238
            WHEN contract_address = LOWER('0xa56796f13566c515471A2fBBAB731F88cE5DE428') THEN 0.00222
            WHEN contract_address = LOWER('0xBa0216254163B57aF68B7161cf824dBadcAD61Df') THEN 0.01
            WHEN contract_address = LOWER('0xFC469d13542E70f1512569EBf60C1E8fA01B6931') THEN 0.01
            ELSE 0.003
          END) AS fee
          
      FROM
        polygon.core.ez_dex_swaps
      WHERE
        DATE(block_timestamp) >= DATE('{2}')
        AND contract_address IN (
                                  LOWER('0x6E53cB6942e518376E9e763554dB1A45DDCd25c4'),
                                  LOWER('0x384d2094D0Df788192043a1CBd200308DD60b068'),
                                  LOWER('0xa56796f13566c515471A2fBBAB731F88cE5DE428'),
                                  LOWER('0xEFa98Fdf168f372E5e9e9b910FcDfd65856f3986'),
                                  LOWER('0xBa0216254163B57aF68B7161cf824dBadcAD61Df'),
                                  LOWER('0x32A222f69d00e717845a3D857D0392D6A25a2ACd'),
                                  LOWER('0xFC469d13542E70f1512569EBf60C1E8fA01B6931'),
                                  LOWER('0xCe0BbB1E51ee21cde86257593f29cBD9A60CA97A')
                                )
  ),

vol_table AS (
  SELECT
    date,
    SUM(amount_in_usd) as vol,
    AVG((amount_in_usd * (1 - fee) - amount_out_usd) / NULLIF(amount_in_usd,0)) AS slippage
  FROM
    swap_table
  GROUP BY
    date
  ORDER BY
    date,
    vol DESC
)

SELECT
  *,
  (SELECT AVG((amount_in_usd * (1 - fee) - amount_out_usd) / NULLIF(amount_in_usd,0)) FROM swap_table) as slippage_overall,
  SUM(vol) OVER ( ORDER BY date) as vol_cumulative
FROM
  vol_table 
 """

 #----------------------------------------------------- TOKEN PRICES ----------------------------------------------------#
# Token prices related to the  main GNS liquidity pools available in the market. Data from Coingeko API
# Token 0: GNS | Token1:  WETH, DAI, USDC, MATIC

#Getting current prices
def get_token_prices():
  url = "https://api.coingecko.com/api/v3/simple/price?ids=ethereum%2Cdai%2Cusd-coin%2Cmatic-network%2Cgains-network&vs_currencies=usd&include_market_cap=false&include_24hr_vol=false&include_24hr_change=false&include_last_updated_at=false"
  respon = re.get(url)
  df_prices = pd.DataFrame.from_records(respon.json())
  df_prices.columns = ['DAI', 'WETH', 'GNS', 'MATIC', 'USDC']
  token_prices = {token: df_prices[token].values[0] for token in list(df_prices.columns)}
  return token_prices