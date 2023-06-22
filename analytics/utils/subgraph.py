from ast import Call
import requests
from urllib3 import Retry
from web3 import Web3
from typing import Callable
import pandas as pd
from . import general_utils as gu
from datetime import datetime
import logging
subgraph_urls = {
    "arbitrum": "https://api.thegraph.com/subgraphs/name/livepeer/arbitrum-one",
    "mainnet": 'https://api.thegraph.com/subgraphs/name/livepeer/livepeer',
    "l1":'https://gateway.thegraph.com/api/405c329cb2d45b59c2d836121804d54/subgraphs/id/FDD65maya4xVfPnCjSgDRBz6UBWKAcmGtgY6BmUueJCg',
    "arbitrum-rinkeby": 'https://api.thegraph.com/subgraphs/name/livepeer/arbitrum-rinkeby',
    "rinkeby": 'https://api.thegraph.com/subgraphs/name/livepeer/livepeer-rinkeby'
}

def network_date_picker(timestamp):
    if datetime.fromtimestamp(timestamp) < datetime(2022, 2, 14):
        network='mainnet'
    else:
        network='arbitrum'

    return network

class SubgraphQuery:
    """Class to faciliate querying subgraphs"""
    def __init__(self, network="arbitrum"):
        self.livepeer_subgraph_url = subgraph_urls[network]
        
    def run_query(self, query):
        """Executes query to the subgraph"""
        request = requests.post(self.livepeer_subgraph_url,
                                json={'query': query})
        if request.status_code == 200:
            response = request.json()
            if 'errors' in response.keys():
                try:
                    raise Exception('Error in query - {} at {}'.format(response['errors'][0]['message'], response['errors'][0]['locations']))         
                except:
                    print("unknown error encountered - inspect response", response)
                    return response
            else:
                return response
        else:
            raise Exception('Query failed - {}. Status {}. Query supplied: {}'.format(request.reason, request.status_code, query))

    def validate_paginated_query(self, query):
        """Check for pagination terms"""
        terms = ['first', 'skip']
        check = [term in query for term in terms]
        assert all(check), "missing pagination terms in query, either " + ' or '.join(terms)

    def run_and_paginate_query(self, query: str, first: int=100, skip: int=None, verbose=False):
        """By default subgraph queries only return 100 documents. Orchestrate and run paginated queries.
        Note: this cannot return more than 5000 documents.
        
        Parameters
        query - string of graphql query. needs to have a 'first' and a 'skip' parameter
        first - number of documents to return, e.g. chunk size
        skip - number of documents to skip, e.g. number of docs already returned
            usually only used internally
        verbose - boolean used to debug
        """
        self.validate_paginated_query(query)

        offset = skip if skip else 0
        _continue = True
        results = []
        while _continue:
            cur_query = query % (first, offset)
            cur_result = self.run_query(cur_query)
            cur_df = self.format_subgraph_response(cur_result)
            results.append(cur_df)
            if len(cur_df) < first:
                offset += len(cur_df)
                print("returning {offset} results".format(offset=offset))
                _continue = False
            elif offset == 5000:
                logging.warn("reached maximum skip value of 5000")
                _continue = False
            else:
                offset += first   
                if verbose: print("paginating to next {offset}....".format(offset=offset))

        df = pd.concat(results).reset_index(drop=True)

        # subgraph is stored as strings - convert to numeric as expected
        df = df.apply(gu.convert_col_to_numeric)

        return df

    def format_subgraph_response(self, resp):
        """Process API response and convert to pandas dataframe.
        
        Returns
        dataframe or an exception"""
        try:
            # uses the keys to parse multilevels of the graphql response
            keys = list(resp['data'].keys())
            data = [
                pd.json_normalize(resp['data'][key])
                for key in keys
            ]
            df = pd.concat(data)
            # json normalize uses dots between the different levels of the schema
            # replace with hyphens to match python styling
            df.columns = [col.replace(".", "_") for col in df.columns]
            return df
        except:
            print(resp)

    # convenience wrappers for common queries
    def get_orchestrators(self, offset=0, page_size=100):
        """Run _get_orchestrators_query
        
        Parameters
        offset: graphql "skip" parameter. used in pagination
        page_size: graphql "first" parameter. number of results to return

        Returns
        result["data"]["transcoders"]
        """
        result = self.run_query(_get_orchestrators_query(page_size, offset))
        return result["data"]["transcoders"]

    def get_delegators(self, offset=0, page_size=100):
        """Run _get_delegators_query
        
        Parameters
        offset: graphql "skip" parameter. used in pagination
        page_size: graphql "first" parameter. number of results to return

        Returns
        result["data"]["delegators"]
        """
        result = self.run_query(_get_delegators_query(page_size, offset))
        return result["data"]["delegators"]

    def get_fee_reward(self, offset, page_size, params):
        """Run _get_fee_reward_query for a specific block
        
        Parameters
        offset: graphql "skip" parameter. used in pagination
        page_size: graphql "first" parameter. number of results to return
        params: uses params["block"]

        Returns
        result["data"]["transcoders"]
        """
        result = self.run_query(_get_fee_reward_query(page_size, offset, params))
        return result["data"]["transcoders"]

    def get_migrators(self, offset, page_size):
        """Run _get_migrators_query
        
        Parameters
        offset: graphql "skip" parameter. used in pagination
        page_size: graphql "first" parameter. number of results to return

        Returns
        result["data"]["migrateDelegatorFinalizedEvents"]
        """
        result = self.run_query(_get_migrators_query(page_size, offset))
        return result["data"]["migrateDelegatorFinalizedEvents"]

    def get_delegator_claim(self, offset, page_size):
        """Run _get_delegator_claim_query
        
        Parameters
        offset: graphql "skip" parameter. used in pagination
        page_size: graphql "first" parameter. number of results to return

        Returns
        result["data"]["stakeClaimedEvents"]
        """
        result = self.run_query(_get_delegator_claim_query(page_size, offset))
        return result["data"]["stakeClaimedEvents"]

    def get_recent_rounds(self, offset, page_size):
        """Run _get_recent_round_query - returns list of round ids
        
        Parameters
        offset: graphql "skip" parameter. used in pagination
        page_size: graphql "first" parameter. number of results to return

        Returns
        [int(round["id"]) for round in result["data"]["rounds"]]
        """
        result = self.run_query(_get_recent_round_query(page_size, offset))
        return [int(round["id"]) for round in result["data"]["rounds"]]

    def get_round_protocol(self, offset, page_size, params):
        """Run _get_round_protocol to get protocol of a round for a specific block
        
        Parameters
        offset: graphql "skip" parameter. used in pagination
        page_size: graphql "first" parameter. number of results to return
        params: uses params["block"]

        Returns
        result["data"]["protocols"][0]
        """
        result = self.run_query(_get_round_protocol(page_size, offset, params))
        return result["data"]["protocols"][0]

    def get_recent_rebonds(self, offset, page_size):
        """Run _get_recent_rebonds_query 
        
        Parameters
        offset: graphql "skip" parameter. used in pagination
        page_size: graphql "first" parameter. number of results to return

        Returns
        result["data"]["rebondEvents"]
        """
        result = self.run_query( _get_recent_rebonds_query(page_size, offset))
        return result["data"]["rebondEvents"]

    def get_recent_rebonds_l1(self, offset, page_size):
        """Run _get_recent_rebonds_query_l1 
        
        Parameters
        offset: graphql "skip" parameter. used in pagination
        page_size: graphql "first" parameter. number of results to return

        Returns
        return result["data"]["rebondEvents"]
        """
        result = self.run_query(_get_recent_rebonds_query_l1(page_size, offset))
        return result["data"]["rebondEvents"]

    def get_current_round(self):
        """Gets current round"""
        query = "{ rounds(first: 1, orderBy: startBlock, orderDirection: desc) { id } }"
        result = self.run_query( query)
        return int(result["data"]["rounds"][0]["id"])




# individual calls
def get_pending_fees(contract, delegator_address, current_round):
    return contract.caller.pendingFees(Web3.toChecksumAddress(delegator_address), current_round)

def get_pending_stake(contract, delegator_address, current_round):
    return contract.caller.pendingStake(Web3.toChecksumAddress(delegator_address), current_round)

def get_earnings_pool(contract, delegator_address, current_round):
    return contract.caller.getTranscoderEarningsPoolForRound(Web3.toChecksumAddress(delegator_address), current_round)

# convenience wrappers for formating graphql queries
def _get_recent_rebonds_query(limit, skip):
    return "{ rebondEvents (where: {round_gt:\"2468\"}, first: %s, skip: %s){ round { id } amount } }" % (limit, skip)

def _get_recent_rebonds_query_l1(limit, skip):
    return "{ rebondEvents (where: {round_gt:\"2425\"}, first: %s, skip: %s){ round { id } amount } }" % (limit, skip)

def _get_recent_round_query(limit, skip):
    return "{ rounds(orderBy:id, orderDirection: desc, first: %s, skip: %s) { id } }" % (limit, skip)

def _get_delegators_query(limit, skip):
    return "{ delegators(first: %s, skip: %s) { id delegate { id } } }" % (limit, skip)

def _get_migrators_query(limit, skip):
    return "{ migrateDelegatorFinalizedEvents(first: %s, skip: %s) { delegate delegatedStake stake l1Addr } }" % (limit, skip)

def _get_fee_reward_query(limit, skip, params):
    return "{ transcoders(first: %s, skip: %s, block:{number: %s}) { id active totalStake feeShare rewardCut } }" % (limit, skip, params["block"])

def _get_round_protocol(limit, skip, params):
    return "{ protocols( block:{number: %s}) { id inflation totalActiveStake totalSupply } }" % (params["block"])

def _get_orchestrators_query(limit, skip):
    return "{ transcoders(first: %s, skip: %s) { id active totalStake } }" % (limit, skip)

def _get_delegator_claim_query(limit, skip):
    return "{ stakeClaimedEvents(first: %s, skip: %s) { id delegator delegate } }" % (limit, skip)