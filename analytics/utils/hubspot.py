import os
import pandas as pd
pd.set_option("display.max_columns", 500)
from analytics.utils.database import init_db_engine
import plotly.express as px
import re
import analytics.utils.general_utils as gu
import analytics.utils.database as db
from pprint import pprint
from hubspot import HubSpot
from analytics.utils.timing import timing
from dotenv import load_dotenv
load_dotenv()

def format_hubspot_response(record):
    record = record.to_dict()
    keep = record['properties'] if 'properties' in record.keys() else record
    if 'teams' in record.keys():
        if record['teams'] is not None:
            teams = record['teams']
            primary_team = [team for team in teams if team['primary']]
            primary_team_name = primary_team[0]['name']
        else:
            primary_team_name = None
        keep['team_name'] = primary_team_name
    keep['id'] = record['id']
    return keep

@timing
def query_hubspot_data():
    api_client = HubSpot(access_token=os.getenv('HUBSPOT_TOKEN'))
    properties=['firstname', 'lastname', 'email', 'lp_stripe_customer_id', 'lp_original_event_source', 'attributable_to', 'stripe_pricing_api_id', 'hubspot_owner_id', 'livepeer_studio_email', 'company', 'associatedcompanyid', 'twitterhandle', 'telegram_handle', 'ecosystem_program', 'hs_lead_status', 'use_case']
    all_contacts = api_client.crm.contacts.get_all(properties=properties)
    contacts = pd.DataFrame.from_records([format_hubspot_response(record) for record in all_contacts])

    # add primary company
    all_companies = api_client.crm.companies.get_all(properties=['name', 'domain', 'id', 'industry'])
    companies = pd.DataFrame.from_records([format_hubspot_response(record) for record in all_companies])
    companies = companies.rename(columns={'name':'company_name', 'domain':'company_domain', 'id':'associatedcompanyid', 'industry':'company_industry'})
    contacts = contacts.merge(companies[['company_domain', 'company_name', 'associatedcompanyid', 'company_industry']], on='associatedcompanyid', how='left')    

    # add hubspot 'owner'
    all_owners = api_client.crm.owners.get_all()
    owners = pd.DataFrame.from_records([format_hubspot_response(record) for record in all_owners])
    owners.columns = ["hubspot_owner_" + col for col in owners.columns]
    detailed = contacts.merge(owners[['hubspot_owner_id', 'hubspot_owner_email', 'hubspot_owner_first_name', 'hubspot_owner_last_name', 'hubspot_owner_team_name']], left_on='hubspot_owner_id', right_on='hubspot_owner_id', how='left', suffixes=["", "_owner"])
    detailed = detailed[(detailed.email != "")]

    return detailed

def combine_studio_hubspot(studio_users, hubspot_details):
    # make sure strings are ready to match
    # studio_users['email'] = studio_users.email.str.lower()
    # hubspot_details['email'] = hubspot_details.email.str.lower()  
    studio_users_emails = studio_users.email.tolist()

    email_matches = studio_users.merge(hubspot_details, on='email')
    stripe_matches = studio_users[
        ~(studio_users.email.isin(email_matches.email)) & 
        (studio_users.stripe_customer_id.notna())] \
            .merge(
                hubspot_details[hubspot_details.lp_stripe_customer_id.notna()], 
                left_on='stripe_customer_id', 
                right_on='lp_stripe_customer_id', 
                suffixes=['', '_hubspot'])
    unmatched = studio_users[
        ~(studio_users.email.isin(email_matches.email)) & 
        ~(studio_users.email.isin(stripe_matches.email))]

    comb = pd.concat([email_matches, stripe_matches, unmatched])
    comb_email_list = comb.email.tolist()

    assert all([email in comb_email_list for email in studio_users_emails])
    assert comb.shape[0] == studio_users.shape[0]

    return comb