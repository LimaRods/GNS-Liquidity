import pandas as pd
pd.set_option("display.max_columns", 500)
import requests

import requests
import web3
from ..utils.subgraph import SubgraphQuery
import numpy as np

def interpret_change(col):
    conditions = [
        col < 0, 
        col > 0,
        col == 0,
        np.isnan(col),

    ]
    choices = [
        'more to stakers',
        'more to orch',
        'no change',
        'initial'
    ]
    return np.select(conditions, choices)
    
def get_parameter_updates():
    update_query = """
    {
        transcoderUpdateEvents(first: %s, skip: %s) {
            timestamp
            round {
                id
            }
            delegate {
                id
            }
            rewardCut
            feeShare
        }
    }
    """
    subgraph = SubgraphQuery()
    updates = subgraph.run_and_paginate_query(update_query)

    updates['date'] = pd.to_datetime(updates.timestamp, unit='s')
    updates['reward_cut_perc'] = updates['rewardCut'] / 10000
    updates['fee_cut_perc'] = 100 - (updates['feeShare'] / 10000)
    updates = updates.sort_values(by=['delegate_id', 'timestamp'])
    updates = updates.rename(columns={'delegate_id':'orchestrator_id'}) 
    updates['reward_change'] = updates.groupby('orchestrator_id').reward_cut_perc.diff()
    updates['reward_change_inter'] = interpret_change(updates.reward_change)

    updates['fee_cut_change'] = updates.groupby("orchestrator_id").fee_cut_perc.diff()
    updates['fee_change_inter'] = interpret_change(updates.fee_cut_change)
    
    updates['net_change'] = updates.reward_change + updates.fee_cut_change

    cases = [
        (updates.reward_change_inter == 'more to stakers') & (updates.fee_change_inter.isin(['more to stakers', 'no change'])),
        (updates.reward_change_inter.isin(['more to stakers', 'no change'])) & (updates.fee_change_inter.isin(['more to stakers'])),
        (updates.reward_change_inter == 'more to orch') & (updates.fee_change_inter.isin(['more to orch', 'no change'])),
        (updates.reward_change_inter.isin(['more to orch', 'no change'])) & (updates.fee_change_inter.isin(['more to orch'])),
        (updates.reward_change_inter == 'more to orch') & (updates.fee_change_inter == 'more to stakers'),
        (updates.reward_change_inter == 'more to stakers') & (updates.fee_change_inter == 'more to orch'),
        (updates.reward_change_inter == 'initial') | (updates.fee_change_inter == 'initial')
    ]

    choices = [
        'stakers',
        'stakers',
        'orch',
        'orch',
        'balanced',
        'balanced',
        'initial'
    ]

    updates['benefit'] = np.select(cases, choices, default=np.nan)
    
    updates['next_change'] = updates.groupby('orchestrator_id').timestamp.shift(-1).fillna(np.inf)
    updates['next_change_round'] = updates.groupby('orchestrator_id').round_id.shift(-1)
    updates['rounds_between_update'] = updates.next_change_round - updates.round_id

    return updates.drop(columns=['rewardCut', 'feeShare'])

def get_unbonds():
    unbond_query = '''
    {
        unbondEvents(first: %s, skip: %s, where: {timestamp_gt:1609480800}) {
            timestamp
            amount
            round {
                id
            }
            withdrawRound
            unbondingLockId
            delegate {
                id
            }
            delegator {
                id
            }
        }
    }'''
    subgraph = SubgraphQuery('mainnet')
    l1 = subgraph.run_and_paginate_query(query=unbond_query)
    subgraph = SubgraphQuery('arbitrum')
    l2 = subgraph.run_and_paginate_query(query=unbond_query)
    unbonds = pd.concat([l1, l2])
    unbonds['type'] = 'unbond'
    unbonds = unbonds.rename(columns={'delegate_id':'old_orchestrator_id'})

    return unbonds

def get_rebonds():
    rebond_query = """
    {
    rebondEvents(first: %s, skip: %s,where: {timestamp_gt:1609480800}) {
        timestamp
        amount
        round {
            id
        }
        unbondingLockId
        delegator {
        id
        }
        delegate {
        id
        }
    }
    }
    """  
    subgraph = SubgraphQuery('mainnet')
    l1 = subgraph.run_and_paginate_query(query=rebond_query)
    subgraph = SubgraphQuery('arbitrum')
    l2 = subgraph.run_and_paginate_query(query=rebond_query)
    rebonds = pd.concat([l1, l2])
    rebonds['type'] = 'rebond'
    rebonds = rebonds.rename(columns={'delegate_id':'new_orchestrator_id'})

    return rebonds

def get_bonds():
    bond_query = """
    {
    bondEvents(first: %s, skip: %s, where: {timestamp_gt:1609480800}){
        timestamp
        bondedAmount
        additionalAmount
        round {
            id
        }
        newDelegate {
            id
        }
        oldDelegate {
            id
        }
        delegator {
            id
        }
    }
    }
    """
    subgraph = SubgraphQuery('mainnet')
    l1 = subgraph.run_and_paginate_query(query=bond_query)
    subgraph = SubgraphQuery('arbitrum')
    l2 = subgraph.run_and_paginate_query(query=bond_query)
    bonds = pd.concat([l1, l2])
    bonds['type'] = 'bond'
    bonds['additionalAmount'] = bonds.additionalAmount.astype(float)
    bonds = bonds.rename(columns={'bondedAmount':'amount', 'newDelegate_id':'new_orchestrator_id', 'oldDelegate_id':'old_orchestrator_id'})
    bonds['amount'] = bonds.amount.astype(float)

    return bonds

def get_redelegation_events(updates):
    """fee/reward cut updates thtat resulted in delegation changes 
        either, bonding, unbonding or transferred bonding
        removes self-delegating changes
        
    """
    print("getting unbonds")
    unbonds = get_unbonds()
    print("getting rebonds")
    rebonds = get_rebonds()
    print("getting bonds")
    bonds   = get_bonds()

    all_bondings = pd.concat(
        [
            unbonds.drop(columns=['unbondingLockId', 'withdrawRound']), 
            rebonds.drop(columns=['unbondingLockId']), 
            bonds.drop(columns=['additionalAmount'])
        ]
    )

    unbondings = updates.merge(all_bondings, left_on='orchestrator_id', right_on='old_orchestrator_id', suffixes=['', '_bondings'])
    bondings = updates.merge(all_bondings, left_on='orchestrator_id', right_on='new_orchestrator_id', suffixes=['', '_bondings'])
    combined = pd.concat([unbondings, bondings])

    # filter out self-delegation events
    combined = combined[combined.orchestrator_id != combined.delegator_id]

    combined['amount'] = pd.to_numeric(combined.amount)
    combined['amount'] = np.where(combined.new_orchestrator_id != combined.orchestrator_id, combined.amount * -1, combined.amount)

    combined['date_bondings'] = pd.to_datetime(combined.timestamp_bondings, unit='s')
    combined['time_diff'] = combined.date_bondings - combined.date
    combined['time_diff'] = combined.time_diff.dt.days
    combined['round_diff'] = combined.round_id_bondings - combined.round_id

    combined = combined[combined.timestamp_bondings.between(combined.timestamp, combined.next_change)]

    # only return unique and key columns
    key_cols = ['orchestrator_id', 'timestamp']
    keep_cols = [col for col in combined.columns if (col not in updates.columns) or (col in key_cols)]
    return combined[keep_cols], all_bondings

def get_redelegations():
    updates = get_parameter_updates()

    redelegations, all_bondings = get_redelegation_events(updates)
    
    changes = updates.merge(redelegations, on=['orchestrator_id', 'timestamp'], how='left')
    changes = changes.merge(changes.groupby('orchestrator_id').timestamp.nunique().rename('update_count').reset_index(), on='orchestrator_id', how='left')
    changes = changes[(changes.date > '2022-03-01') & (changes.benefit != 'initial')]

    all_bondings['date'] = pd.to_datetime(all_bondings.timestamp, unit='s')
    all_bondings['year_mon'] = all_bondings['date'].dt.strftime("%b %Y")
    return changes, all_bondings