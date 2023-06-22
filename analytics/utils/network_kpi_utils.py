import os
import time
from datetime import datetime, timedelta
from typing import List, Tuple

import ipinfo
import numpy as np
import pandas as pd
import requests
import web3
from dotenv import load_dotenv

from . import eth
# from ..utils.eth import get_web3_client
# from ..utils.general_utils import convert_col_to_numeric
from . import general_utils as gu
from ..utils.subgraph import SubgraphQuery, network_date_picker

import logging

def get_dates(num_weeks: int=5) -> List[int]:
    """Define date range in week increments starting on monday
    
    Parameters
    num_weeks: number of weeks prior to start date to return

    Returns
    unix_dates: list of dates with length==num_weeks of dates in unix format
    """
    today = datetime.today()
    monday = today - timedelta(days=today.weekday(), hours=today.hour, minutes=today.minute, seconds=today.second)
    dates = [monday - timedelta(days=7*i) for i in range(num_weeks)]
    dates.sort()
    unix_dates = [int(time.mktime(date.timetuple())) for date in dates]
    return unix_dates

def get_block_by_timestamp(dates: list) -> List[int]:
    """Query Etherscan API to get blocks for each date in dates

    Parameters:
    dates: list of unix dates. output from get_dates

    Returns:
    blocks: list of blocks from corresponding ot dates    
    """

    blocks = []
    for date in dates:
        result = requests.get(
            url='https://api.etherscan.io/api',
            params={
                'module':'block',
                'action':'getblocknobytime',
                'timestamp':int(date),
                'closest':'before',
                'apikey':'RU9MJSADRK6F5X7GIU12821C96WNZ9Q4J5'
            },
            headers={'User-Agent':'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Mobile Safari/537.36'
            }
        )
        block_number = int(result.json()['result'])
        blocks.append(block_number)
        time.sleep(.5)
    return blocks

def get_date_to_block_ref(dates: list, blocks: list) -> pd.DataFrame:
    """Derive dataframe to use a reference for date and block start/ends to analyze
    
    Parameters
        dates: list of unix dates. output from get_dates
        blocks: list of blocks from corresponding to dates. output from get_block_by_timestamp
    Returns
        date_block_ref: dataframe with start and end dates and corresponding block numbers  
    """
    date_block_ref = []
    for i in range(len(dates) - 1):
        date_block_ref.append({
            'start_date':int(dates[i]),
            'start_block':int(blocks[i]),
            'end_date':int(dates[i+1]),
            'end_block':int(blocks[i+1])
        })

    date_block_ref = pd.DataFrame(date_block_ref)
    return date_block_ref

def get_transcoders() -> dict:
    """Orchestrate querying transcoders from Livepeer subgraph

    Returns
        result: dataframe with transcoders data
    """

    query = """
    {
        transcoders(first: %s, skip: %s) {
            id
            totalStake
            totalVolumeETH
            rewardCut
            activationRound
            active
            delegator {
                bondedAmount
            }
            delegators {
                id
                bondedAmount
            }
            pools(orderBy:id, orderDirection:desc, first: 100) {
                rewardTokens
                totalStake
                fees
                round {
                    id
                    startBlock
                    endBlock
                }
            }
        }
    }
    """
    subgraph = SubgraphQuery(network='mainnet')
    l1 = subgraph.run_and_paginate_query(query)
    subgraph = SubgraphQuery(network='arbitrum')
    l2 = subgraph.run_and_paginate_query(query)
    response = pd.concat([l1, l2])
    response = response[response.activationRound != 0]
    response = response.drop_duplicates(keep='last', subset='id')
    return response

def extract_delegator_details(result) -> pd.DataFrame:
    """Format result from get_transcoders into dataframe
    
    Parameters
        result: output from get_transcoders
    
    Return:
        transcoders: input dataframe with num_delegators and rewardCut added
    """
    #transcoders = pd.json_normalize(result['data']['transcoders'])
    result['total_delegators'] = result.delegators.apply(lambda x: len(x))
    result = result.apply(gu.convert_col_to_numeric)
    result['rewardCut'] = result.rewardCut / 1000000

    return result.drop(columns='delegators')

def extract_rounds_data(transcoders: pd.DataFrame) -> pd.DataFrame:
    """Expand array of pools for each transcoder and format to analyze
    
    Parameters
        transcoders: dataframe with transcoder level aggregated stats
    Returns
        pools: dataframe with pool level data for each transcoder
    """

    pools = transcoders[['id', 'pools']].explode(column='pools', ignore_index=True)
    pools = pools[pools.pools.notnull()]
    pools = pd.concat([pools.drop(columns='pools'), pools.pools.apply(pd.Series)], axis=1)
    pools = pd.concat([pools.drop(columns='round'), pools['round'].apply(pd.Series).rename(columns={'id':'round_id'})], axis=1)
    pools = pools.apply(gu.convert_col_to_numeric)
    pools = pools.rename(columns={col:"round_"+col for col in pools.columns if col != 'id' and 'round' not in col})

    return pools

def get_week_end_date(df:pd.DataFrame, date_col:str, date_block_ref:pd.DataFrame, ref_cols: List[str]) -> np.ndarray:
    """Use week block number start and end to find which week each rounds belongs in
    
    Parameters
        pools: dataframe with pool level data for each transcoder
        date_col: name of date column to assign to week - should be in unix seconds time
        date_block_ref: dataframe with start and end dates and corresponding block numbers  
        ref_col: names of column to check range over, e.g. ['start_date', 'end_date']
    Returns
        end_dates: np.array containing the date corrresponding to the end of that pool's week
    """
    conditions = [
        df[date_col].between(int(date_block_ref[ref_cols[0]][i]), int(date_block_ref[ref_cols[1]][i]))
        for i in range(date_block_ref.shape[0])
    ]
    end_dates = np.select(
        condlist=conditions,
        choicelist=date_block_ref.end_date.tolist(),
        default=time.mktime(datetime.now().timetuple())
    )

    return end_dates.astype(int)

def get_best_regions(date_ref: pd.DataFrame) -> pd.DataFrame:
    """Query Livepeers serverless agg stats api and find the best region for each pool
    
    Parameters
        date_ref: dataframe with start and end dates to query aggregated stats over
    Returns
        best_regions_df: dataframe with the best regions and score for each transcoder/pool 
    """
    best_regions = []
    date_dfs = []
    for i in range(date_ref.shape[0]):
        start_date = int(date_ref.start_date[i])
        end_date = int(date_ref.end_date[i])

        result = requests.get(
            url="https://leaderboard-serverless.vercel.app/api/aggregated_stats/",
            params={
                'since':start_date,
                'until':end_date
            }
        ).json()
        ids = list(result.keys())
        id_dfs = []
        for id in ids:
            temp = pd.DataFrame.from_dict(data=result[id], orient='index').reset_index().rename(columns={'index':'region'})
            temp['id'] = id
            temp['start'] = start_date
            temp['end'] = end_date
            id_dfs.append(temp)
        id_df = pd.concat(id_dfs)
        date_dfs.append(id_df)

        region_results = []
        for id in result.keys():
            maxVal = 0
            maxRegion = ''
            for region in result[id]:
                if result[id][region]['score'] > maxVal:
                    maxVal = result[id][region]['score']
                    maxRegion = region
        
            region_results.append(
                {
                'id':id,
                'regional_high_score':maxVal,
                'best_region':maxRegion,
                'week_start_date':start_date,
                'week_end_date':end_date
                }
            )

        best_regions.append(pd.DataFrame(region_results))
    best_regions_df = pd.concat(best_regions)
    all_perf_df = pd.concat(date_dfs)
    return best_regions_df, all_perf_df

def aggregate_round_data(pools) -> pd.DataFrame:
    """Aggregated pool level stats up to their assocaited week
    
    Parameters
        pools: pool level data
        regions: best region data. output from get_best_regions
    Returns
        agg: aggregated data for each for each transcoder  
    """
    agg = pools.groupby(['id', 'week_end_date']) \
        .agg({
        'round_id':['min', 'max'], 
        'round_rewardTokens':['count', 'sum'], 
        'round_totalStake':'mean'
        }) \
            .reset_index()
        
    agg.columns = [f'{i}{j}' for i, j in agg.columns]
    return agg

def get_threshold_df() -> pd.DataFrame:
    """Format manually derived threshold into a dataframe to easily reference
        each value corresponds to the upper threshold for each metric
    Returns
        threshold_df: thresholds for each metric sort lowest to highest
    """
    threshold_df = pd.DataFrame({
        'bands':['high', 'mid', 'low', 'lowest'],
        'regional_high_score':[1, .63, .20, .01],
        'call_ratio':[1, .75, .24, .01],
        'round_total_stake':[np.Inf, 59999, 2500, 2499],
        'total_delegators':[np.Inf, 99, 9, 1],
        'week_eth_fees':[np.Inf, 5, 1, .01]
    })

    threshold_df = threshold_df.sort_values(by='regional_high_score')
    return threshold_df

def categorize_metrics(df: pd.DataFrame, threshold_df: pd.DataFrame, metric: str) -> pd.Series:
    """Bin merics according to thresholds derived in get_threshold_df
    
    Parameters
        df: dataframe with metrics to be categorized
        threshold_df: dataframe with cutoffs and bins. cutoffs should be the upper limit
        metric: one of ['regional_high_score', 'call_ratio', 'round_total_stake', 'total_delgators', 'week_eth_fees']
    
    Returns
        banded: pd.Series of corresponding bins for each metric
    """
    banded = pd.cut(
        df[metric], 
        bins=[0] + threshold_df[metric].tolist(), 
        labels=threshold_df.bands, 
        include_lowest=True).astype(object)
    banded = banded.fillna('lowest')
    return banded

def derive_bands(df: pd.DataFrame, threshold_df: pd.DataFrame) -> pd.DataFrame:
    """Orchestrate the binning by repeated calls to categorize_metrics
    
    Parameters
        df: dataframe with metrics to be categorized
        threshold_df: dataframe with cutoffs and bins. cutoffs should be the upper limit
    Returns
        df: same dataframe as input but with metric + "_band" columns attached
    """
    metrics = ['regional_high_score', 'call_ratio', 'round_total_stake', 'total_delegators', 'week_eth_fees']
    for metric in metrics:
        df[metric] = pd.to_numeric(df[metric])
        if metric != 'regional_high_score':
            df[metric] = df[metric].fillna(0)
        
        df[metric + "_band"] = categorize_metrics(df, threshold_df, metric)

    return df

def derive_segments(df: pd.DataFrame) -> pd.DataFrame:
    """Derive 'segments' based on the bands for each metric
    
    Parameters
        df: dataframe with metric bands attached. output from derive_bands

    Returns
        df: same as input with 'segment' added  
    """
    conditions = [
        (df.regional_high_score_band == 'high') & (df.round_total_stake_band == 'high'), # A - Top Performing
        (df.regional_high_score_band == 'high') & (df.round_total_stake_band.isin(['low', 'lowest', 'mid'])), # B - High Performers
        (df.regional_high_score_band == 'mid') & (df.round_total_stake_band == 'high'), # C - Mid Performing with High Stake 
        (df.regional_high_score_band == 'mid') & (df.round_total_stake_band != 'high'), # D - Mid Performing with Low Stats
        (df.regional_high_score_band.isin(['low', 'lowest', 'mid'])) & (df.round_total_stake_band.isin(['mid', 'high'])) & (df.call_ratio_band.isin(['mid', 'high'])), # E - High Stake & Call - Low Performing
        (df.regional_high_score_band == 'lowest') & (df.round_total_stake_band.isin(['mid', 'high'])) & (df.call_ratio_band.isin(['high', 'mid'])), # X - Non Performing with High or Medium Stake Nodes and High Call
        (df.regional_high_score_band.isin(['low', 'lowest'])) & (df.round_total_stake_band.isin(['low', 'lowest'])) & (df.call_ratio_band.isin(['low', 'lowest'])) # Z's
    ]

    choices = [
        'A - Top Nodes',
        'B - High Performing Nodes', 
        'C - Mid Perf/0 Delegated Nodes',
        'D - Mid Performing with Low Stats',
        'E - High Stake/High Call Nodes', 
        'X - 0 Performing w High/Mid Stake Nodes',
        'Z - 0 Contributor Nodes'
    ]

    segment = np.select(
        condlist=conditions,
        choicelist=choices,
        default='ZZ - Other'
    )

    return segment

def get_aggregated_transcoding_price_stats() -> pd.DataFrame:
    """Query the serverless orchestrator stats api to get by-orchestrator stats"""
    response = requests.get(url="https://nyc.livepeer.com/orchestratorStats").json()
    orchestrator_stats = pd.json_normalize(response)
    orchestrator_stats['DelegatedStake'] = orchestrator_stats['DelegatedStake'].astype(str)
    orchestrator_stats = orchestrator_stats[['Address', 'ServiceURI', 'PricePerPixel']]
    return orchestrator_stats.rename(columns={
        'Address':'id',
        'PricePerPixel':'price_per_pixel_aggregated'
    })

def query_pixel_price_history(id) -> pd.DataFrame:
    """Query serverless price history api"""
    response = requests.get(f"https://nyc.livepeer.com/priceHistory/{id}?limit=10000")
    price = pd.json_normalize(response.json())
    price['id'] = id
    return price

def get_pixel_price_history(ids: List[str]) -> pd.DataFrame:
    """iterate through list of orchestrator ids to get all price histories
    
    Parameters
    ids - list of orchestrator eth addresses
    
    Returns
    pd.DataFrame of prices for each orchestrator"""
    prices = [
        query_pixel_price_history(id) for id in ids
    ]
    return pd.concat(prices)

def calc_price_per_pixel_range(
    prices: pd.DataFrame, 
    id: str, 
    start: int, 
    end: int) -> float:
    """Takes price history get_pixel_price_history and finds average price across
        a range
    
    Parameters
        prices: dataframe of prices for each orchestrator
        id: orchestrator address to calculate for
        start: unix timestamp for start of range
        end: unix timestamp for end of range
        
    Returns
        average price per pixel of the range"""
    id_prices = prices[(prices.id == id) & (prices.PricePerPixel > 0)]
    return id_prices[id_prices.Time.between(start, end)].PricePerPixel.mean()

def get_transcoding_price_history(raw: pd.DataFrame) -> pd.DataFrame:
    """orhcestrate the calculation of the price per pixel using the history api

    Parameters
        raw: dataframe with transcoders, pipeline df
        
    Returns
        raw: dataframe with price_per_pixel_history added"""
    ids = raw.id.unique().tolist()
    prices = get_pixel_price_history(ids)
    raw['price_per_pixel_history'] =  \
        raw.apply(lambda x: calc_price_per_pixel_range(
            prices, x['id'], x['week_start_date'], x['week_end_date']), axis=1)

    return raw

def get_transcoding_price_stats(raw: pd.DataFrame) -> pd.DataFrame:
    """orhcestrate the calculation of the price per pixel for each orchestrator for each week
        ideally use the more detailed priceHistory api but fallback to the aggregated api (only most recent week
        
    Parameters
        raw: dataframe with transcoders, pipeline df
        
    Returns
        raw: dataframe with price_per_pixel added"""
    aggregated_stats = get_aggregated_transcoding_price_stats()
    raw = raw.merge(aggregated_stats, on='id', how='left')
    raw = get_transcoding_price_history(raw)
    raw['price_per_pixel'] = np.where(raw.price_per_pixel_history.isna(), raw.price_per_pixel_aggregated, raw.price_per_pixel_history)

    return raw

def get_ip_location(raw: pd.DataFrame)-> pd.DataFrame:
    """attempt to get ip addresses from the ServiceURI field (from the subgraph)"""
    load_dotenv()
    handler = ipinfo.getHandler(os.getenv('IPINFO_TOKEN'))
    # Simplify service uri to estimate ip address
    raw['ip_address'] = raw.ServiceURI.str.replace("https://", "")
    raw['ip_address'] = raw.ip_address.str.replace(":.*$", "", regex=True)

    response = handler.getBatchDetails(raw.ip_address.dropna())
    all_details = pd.DataFrame.from_dict(response, orient='index')
    all_details = all_details[all_details.status != 404]
    raw = raw.merge(all_details.drop(columns=['status', 'error']), left_on='ip_address', right_on='ip', how='left')

    return raw

def get_block_date_infura(blockno: int, rpc_client: web3.HTTPProvider) -> datetime.date:
    """use infura api to get the date of each block. used to map blocks to dates
    
    Parameters
        blockno: int of block number to lookup
        rpc_client: web http provider. use utils.eth.get_web3_client
        
    Returns
        datetime.datetime of block"""
    logging.debug("blocknumber %s", blockno)
    try:
        block = rpc_client.eth.get_block(int(blockno))
        timestamp = block['timestamp']  
        date = datetime.fromtimestamp(int(timestamp)) # .strftime("%Y-%m-%d W:%W H:%H")
    except Exception as e:
        logging.error("error in get_block_date ", e)
        date = datetime.today()
    return date

def get_l2_round_info(network: str) -> pd.DataFrame:
    """Query rounds info for dashboard
    
    Parameters
        network: str, either mainnet or arbitrum
        
    Returns
        dataframe with round level details"""
    query = """
        {
            rounds (where: {initialized: true, participationRate_not: "0"}, first: %s, skip: %s) {
                id
                volumeETH
                volumeUSD
                participationRate
                totalActiveStake
                newStake
                movedStake
                startBlock
                endBlock
                startTimestamp
                delegatorsCount
            }
        }
    """
    subgraph = SubgraphQuery(network=network)
    rounds = subgraph.run_and_paginate_query(query)
    rounds['start_date'] = pd.to_datetime(rounds.startTimestamp, unit='s')
    rounds['end_date'] = pd.to_datetime(rounds.startTimestamp.shift(-1), unit='s').fillna(datetime.now())

    return rounds

def get_protocol_by_block(block_no, network='mainnet'):
    query = """
        {
            protocol(id: 0, block: {number: %s}) {
                id
                totalVolumeUSD
                totalVolumeETH
                participationRate
            }
        } """ % block_no
    subgraph = SubgraphQuery(network=network)
    protocol = subgraph.run_query(query)

    return protocol



def get_l1_round_info(network: str) -> pd.DataFrame:
    """Query rounds info for dashboard
    
    Parameters
        network: str, either mainnet or arbitrum
        
    Returns
        dataframe with round level details"""

    query = """
        {
            rounds (where: {initialized: true, participationRate_not: "0"}, first: %s, skip: %s) {
                id
                volumeETH
                volumeUSD
                participationRate
                totalActiveStake
                newStake
                movedStake
                startBlock
                endBlock
            }
        }
    """
    subgraph = SubgraphQuery(network='mainnet')
    rounds = subgraph.run_and_paginate_query(query)
    # subset to unique block numbers because get_block_date_infura is fairly slow
    blocks = pd.concat([rounds.startBlock, rounds.endBlock]).unique().tolist()    
    client = eth.get_web3_client(rpc=eth.infura_url)
    block_dates = [get_block_date_infura(int(x), rpc_client=client) for x in blocks]
    block_df = pd.DataFrame({'block':blocks, 'timestamp':block_dates})

    # merge block dates back onto rounds
    rounds = rounds.merge(block_df.rename(columns={'block':'startBlock', 'timestamp':'start_date'}), on='startBlock', how='left')
    rounds = rounds.merge(block_df.rename(columns={'block':'endBlock', 'timestamp':'end_date'}), on='endBlock', how='left')

    rounds = get_l1_delegators(rounds)

    return rounds

def get_fee_derived_minutes(rounds):
    pixelsPerMinute = 905114444 # 2,995,488,000, 905,114,444
    pricePerPixel = 1.2e-15 # 1000 wei in eth 0.0000000000000012
    rounds['eth_dai_rate'] = rounds.volumeETH / rounds.volumeUSD
    rounds['usd_average_price_per_pixel'] = pricePerPixel / rounds.eth_dai_rate
    rounds['fee_derived_minutes'] = rounds.volumeUSD / rounds.usd_average_price_per_pixel / pixelsPerMinute
    rounds['fee_derived_minutes_original'] = rounds.volumeUSD / rounds.usd_average_price_per_pixel / 2995488000

    rounds['fee_derived_minutes_v2'] = rounds.volumeETH / pricePerPixel / pixelsPerMinute
    return rounds

def get_l1_delegators(rounds: pd.DataFrame) -> pd.DataFrame:
    """get delegator counts from both levels - l1 delegators may not be present in the
        arbitrum subgraph if they haven't changed anything
    
    Parameters
        rounds: dataframe - output from get_round_info

    Returns
        input dataframe with delegator counts added on
    """

    query = """
    {
        delegators(first: %s, skip: %s, where:{bondedAmount_gt: 0}) {
            id
            startRound
            bondedAmount
        }
    }
    """
    l1 = SubgraphQuery(network='mainnet')
    l1_delegators = l1.run_and_paginate_query(query)
    l1_delegators = l1_delegators.apply(gu.convert_col_to_numeric)
    # for some reason the subgraph query misses some of these
    l1_delegators = l1_delegators[l1_delegators.bondedAmount > 0]
    # a few exist in both levels - take their original creation
    l1_delegators = l1_delegators.drop_duplicates(subset="id")

    counts = l1_delegators.groupby('startRound').id.count().rename("new_delegators").reset_index()    
    counts = counts.rename(columns={'startRound':'id'})

    rounds = pd.merge(rounds, counts, on='id', how='left')
    rounds = rounds.sort_values('id')
    rounds = rounds.fillna(0)
    rounds['cumulative_delegators'] = rounds.new_delegators.cumsum()
    
    return rounds

def get_round_details():
    """orchestrate round details"""
    l1_rounds = get_l1_round_info(network='mainnet')
    l2_rounds = get_l2_round_info(network='arbitrum')
    l2_rounds['new_delegators'] = l2_rounds.delegatorsCount.shift(-1) - l2_rounds.delegatorsCount
    rounds = pd.concat([l1_rounds, l2_rounds.drop(columns=['startTimestamp']).rename(columns={'delegatorsCount':'cumulative_delegators'})])

    rounds["end_date"] = pd.to_datetime(rounds["end_date"])
    rounds["dow"] = rounds.end_date.dt.day_of_week
    rounds['week'] = rounds.apply(lambda x: x['end_date'] + timedelta(days=-x['dow'] + 6), axis=1).dt.strftime("%m-%d-%Y")
    rounds['date'] = rounds.end_date.dt.strftime("%Y-%m-%d H:%H")
    rounds['year'] = rounds.end_date.dt.isocalendar().year

    rounds = get_fee_derived_minutes(rounds)
    return rounds

def get_transcoder_week_fees(date_block_ref: pd.DataFrame) -> pd.DataFrame:
    """query the subgraph to get eth fees earned for each transcoder by day
        the volumeETH field connect to `rounds` is not being collected
    
    Parameters
        date_block_ref: pipeline dataframe reference start/end blocks with their start/end dates
        
    Returns
        dataframe with eth amounts for each transcoder aggregated by week"""
    query = """
    { 
        transcoderDays(first: %s, skip: %s , orderBy:date, orderDirection:desc) {
            id
            volumeETH
            date
            transcoder {
                id
            }
        }
    }
    """
    subgraph = SubgraphQuery(network='mainnet')
    l1 = subgraph.run_and_paginate_query(query) 
    subgraph = SubgraphQuery(network='arbitrum')
    l2 = subgraph.run_and_paginate_query(query) 
    volume = pd.concat([l1, l2])
    volume = volume.rename(columns={'transcoder_id': 'orchestrator_id'})
    volume['week_end_date'] = get_week_end_date(volume, "date", date_block_ref, ref_cols=['start_date', 'end_date'])
    volume = volume.groupby(['orchestrator_id', 'week_end_date']).volumeETH.sum().reset_index()
    volume = volume.rename(columns={'orchestrator_id':'id', 'volumeETH':'week_eth_fees'})
    return volume

# profitability script
def estimate_txn_cost(start: int, end: int) -> float:
    """calculate the median reward call transaction cost over the a week
        used in the profitability calculation
        
    Parameters
        start: unix timestamp of time period to calculate over
        end: unix timestamp of time period to calculate over
        
    Returns
        median txn cost over that time period in USD"""

    query = f"""
    {{
        rewardEvents(where: {{timestamp_gt: {start}, timestamp_lt: {end}}}, orderBy: timestamp, orderDirection: desc, first: %s, skip: %s){{
            id
            timestamp
            transaction{{
                id
                gasUsed
                gasPrice
            }}
        }}
    }}
    """ 
    network = network_date_picker(start)
    subgraph = SubgraphQuery(network)
    reward = subgraph.run_and_paginate_query(query)
    reward['gas_price_gwei'] = reward.transaction_gasPrice / 1e9
    reward['txn_cost'] = reward.transaction_gasUsed * reward.transaction_gasPrice
    reward['txn_cost_eth'] = reward.txn_cost / 1e18

    request = requests.get(
        url='https://api.etherscan.io/api',
        params={
            'module':'stats',
            'action':'ethprice',
            'apiKey':'RU9MJSADRK6F5X7GIU12821C96WNZ9Q4J5'
        }
    ).json()['result']
    eth_price = float(request['ethusd'])
    reward['txn_cost_usd'] = reward.txn_cost_eth * eth_price
    txn_cost = reward.txn_cost_usd.median()

    return txn_cost

def get_lpt_price_range(start: int, end:int) -> float:
    """calculate average price of lpt token over a time period
    
    Parameters
        start: unix timestamp of time period to calculate over
        end: unix timestamp of time period to calculate over
        
    Returns
        average lpt price over that time period in USD"""
    logging.debug("start: %s, end: %s", start, end)
    try:
        response = requests.get(
                url="https://api.coingecko.com/api/v3/" + "coins/livepeer/market_chart/range",
                params={
                    'vs_currency':'usd',
                    'from':start,
                    'to':end
                }
        )
        prices = pd.DataFrame(response.json()['prices'], columns=['timestamp', 'prices'])
        avg_price = prices.prices.mean()
        time.sleep(5)
        return avg_price
    except Exception as e:
        logging.error(response, e)
        return np.nan

def get_lpt_price() -> float:
    """convenience function to get current lpt price in usd"""
    response = requests.get(
        url="https://api.coingecko.com/api/v3/" + "simple/price",
        params={
            'ids':'livepeer',
            'vs_currencies':'usd'
        }
    ).json()
    response
    lpt_price = float(response['livepeer']['usd'])
    return lpt_price

def get_protocol() -> dict:
    """query protocol detail"""
    protocol_query = """
    {
        protocols {
            id
            inflation
            numActiveTranscoders
            totalActiveStake
            totalSupply
        }
    }
    """
    subgraph = SubgraphQuery()
    protocol = subgraph.run_query(protocol_query)
    protocol = protocol['data']['protocols'][0]
    protocol['inflation'] = int(protocol['inflation']) / 1e9
    protocol['totalSupply'] = float(protocol['totalSupply'])
    protocol['totalActiveStake'] = float(protocol['totalActiveStake'])
    return protocol

def get_protocol_by_block(block_no, network='mainnet'):
    query = """
        {
            protocol(id: 0, block: {number: %s}) {
                id
                inflation
                numActiveTranscoders
                totalActiveStake
                totalSupply
            }
        } """ % block_no
    subgraph = SubgraphQuery(network=network)
    protocol = subgraph.run_query(query)
    df = pd.DataFrame.from_dict(protocol['data'], orient='index')
    df['block_no'] = block_no
    df['inflation'] = int(df['inflation']) / 1e9
    df['totalSupply'] = float(df['totalSupply'])
    df['totalActiveStake'] = float(df['totalActiveStake'])
    return df

def get_round_protocol(dates):
    protocols_list = []
    for block in dates.end_block:
        if block >= 14247704:
            network='arbitrum'
        else:
            network='mainnet'
        protocols_list.append(get_protocol_by_block(block, network))

    protocols = pd.concat(protocols_list)

    dates = dates.merge(protocols, left_on='end_block', right_on='block_no', how='left')

    return dates

def calc_profitability_threshold(
    txn_cost: float,
    lpt_price: float,
    total_active_stake: int,
    inflation: float,
    total_supply: int,
    delegated_stake: int,
    reward_cut: float
    ) -> float: 
    """Calculate the breakeven self-stake needed for an orchestrator to be profitable"""
    return txn_cost / lpt_price * total_active_stake / (inflation * total_supply) - delegated_stake * reward_cut

def get_reward_calls_profitability(transcoders: pd.DataFrame, dates:pd.DataFrame) -> pd.DataFrame:
    """orchestrate calculating the breakeven self-stake profitability for each transcoder
    
    Parameters
        transcoders - dataframe with transcoders info. needs to have:
            ['week_start_date', 'week_end_date']
            
    Return
        input dataframe with breakeven_self_stake_needed and profitable_lpt_calls added"""

    dates = get_round_protocol(dates)

    dates['txn_cost'] = dates.apply(lambda x: estimate_txn_cost(int(x['start_date']), int(x['end_date'])), axis=1)
    dates['lpt_price'] = dates.apply(lambda x: get_lpt_price_range(int(x['start_date']), int(x['end_date'])), axis=1)
    dates['lpt_price'] = dates['lpt_price'].fillna(dates.lpt_price.median())
    
    transcoders = transcoders.merge(dates[['end_date', 'inflation', 'txn_cost', 'lpt_price', 'totalActiveStake', 'totalSupply', 'block_no']], 
        left_on='week_end_date', right_on='end_date', how='left')
   
    transcoders['breakeven_self_stake_needed'] = transcoders.apply(lambda x: calc_profitability_threshold(
        txn_cost=x['txn_cost'],
        lpt_price=x['lpt_price'],
        total_active_stake=x['totalActiveStake'],
        inflation=x['inflation'],
        total_supply=x['totalSupply'],
        delegated_stake=x['round_total_stake'],
        reward_cut=x['rewardCut']
    ), axis=1)

    transcoders['profitable_lpt_calls'] = np.where(transcoders.breakeven_self_stake_needed < transcoders.self_stake, True, False)
    return transcoders

def run_pipeline(num_weeks=10) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Orchestrate the derivation of the dashboard table mean to illustrate the performance 
        of the network by binning the transcoders into performance bands
    Parameters
    transcoders: dataframe output from get_transcoders. currently necessary to run separately due 
        to challenges with asyncio
    Returns
    table: dataframe pivoted with segment indexes and date columns containing counts 
    raw: dataframe pre-pivot with raw transcoder/date level data
    """

    unix_dates = get_dates(num_weeks)
    logging.info("getting blocks")
    blocks = get_block_by_timestamp(unix_dates)
    date_block_ref = get_date_to_block_ref(unix_dates, blocks)

    logging.info("formatting transcoders")
    transcoders = get_transcoders()
    transcoders = transcoders.rename(columns={'totalVolumeETH':'lifetime_eth_fees_earned', 'totalStake':'current_total_stake', 'delegator_bondedAmount':'self_stake'})
    transcoders = extract_delegator_details(transcoders)

    logging.info("getting rounds")
    rounds = extract_rounds_data(transcoders)
    rounds['week_end_date'] = get_week_end_date(rounds, 'round_endBlock', date_block_ref, ref_cols=['start_block', 'end_block'])
    
    logging.info("aggregating rounds")
    agg = aggregate_round_data(rounds)

    logging.info("getting regions")
    regions, all_performance = get_best_regions(date_block_ref)
    transcoders_regions = pd.merge(transcoders, regions, on='id', how='left')
    
    transcoder_rounds = pd.merge(
        transcoders_regions.drop(columns=['pools', 'delegator']),
        agg,
        on=['id', 'week_end_date'],
        how='left'
    )
    transcoder_rounds['num_rounds'] = transcoder_rounds.round_idmax.fillna(0).astype(int) - transcoder_rounds.round_idmin.fillna(0).astype(int) + 1
    transcoder_rounds['call_ratio'] = transcoder_rounds.round_rewardTokenscount / transcoder_rounds.num_rounds
    transcoder_rounds = transcoder_rounds.rename(columns={
        'round_totalStakemean':'round_total_stake', 
        'round_rewardTokenssum':'round_reward_tokens', 
        'round_rewardTokenscount':'round_number_reward_calls'
        })

    logging.info("getting eth fees by week")
    eth_volume = get_transcoder_week_fees(date_block_ref)
    transcoder_rounds = transcoder_rounds.merge(
        eth_volume.rename(columns={'orchestrator_id':'id'}), on=['id', 'week_end_date'], how='left')

    logging.info("deriving orchestrator segments")
    threshold_df = get_threshold_df()
    transcoder_rounds = derive_bands(transcoder_rounds, threshold_df)
    transcoder_rounds['segment'] = derive_segments(transcoder_rounds)
    transcoder_rounds['date'] = pd.to_datetime(transcoder_rounds.week_end_date, unit='s').dt.strftime("%Y-%m-%d")

    logging.info("getting transcoding price per pixel and ip details")
    transcoder_rounds = get_transcoding_price_stats(transcoder_rounds)
    transcoder_rounds = get_ip_location(transcoder_rounds)

    logging.info("getting reward call profitability")
    transcoder_rounds = get_reward_calls_profitability(transcoder_rounds, dates=date_block_ref)

    logging.info("formating final output")
    df_pivot = transcoder_rounds.groupby(['date', 'segment']).id.count().reset_index()
    pivot = df_pivot.pivot(index='segment', columns='date', values='id').fillna(0).reset_index()
    pivot = pivot.sort_values(by='segment') 

    logging.info("getting info on rounds")
    rounds = get_round_details()

    return pivot, transcoder_rounds, rounds, all_performance
