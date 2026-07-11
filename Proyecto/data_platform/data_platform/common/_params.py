# Databricks notebook source
import re
import requests as _requests

# COMMAND ----------

account_id = '25ecde06-8cd3-4377-863b-acf4d84b39d8'
admin = 'atintaya@utec.edu.pe'

# COMMAND ----------

workspace = 'adb-1703116329100891.11'
workspace_url = f'https://{workspace}.azuredatabricks.net'
warehouse_id = '8ba2a6f5ddb13574'

workspace_pattern = re.compile(r'adb-(\d+)\.\d')
workspace_id = workspace_pattern.findall(workspace)[0]

# COMMAND ----------

_sp_client_id     = dbutils.secrets.get(scope="de-scope", key="sp-client-id")
_sp_client_secret = dbutils.secrets.get(scope="de-scope", key="sp-client-secret")

_token_response = _requests.post(
    f'https://accounts.azuredatabricks.net/oidc/accounts/{account_id}/v1/token',
    data={
        'grant_type': 'client_credentials',
        'client_id': _sp_client_id,
        'client_secret': _sp_client_secret,
        'scope': 'all-apis'
    }
)
token = _token_response.json()['access_token']
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# COMMAND ----------

_account_base = f'https://accounts.azuredatabricks.net/api/2.0/accounts/{account_id}/scim/v2'

endpoints = {
    'account_users_endpoint':  f'{_account_base}/Users',
    'account_groups_endpoint': f'{_account_base}/Groups',
    'genie_endpoint':          f'{workspace_url}/api/2.0/data-rooms',
    'folders_endpoint':        f'{workspace_url}/api/2.0/folders',
    'access_policies_endpoint':f'{workspace_url}/api/2.0/accesspolicies',
}

# COMMAND ----------

schema_descriptions = {
    'bronze': 'Raw data schema',
    'silver': 'Cleaned data schema',
    'gold':   'Aggregated data schema'
}

# COMMAND ----------

privileges = {
    'reader': ['BROWSE', 'USE CATALOG', 'USE SCHEMA', 'SELECT', 'EXECUTE', 'READ VOLUME'],
    'writer': ['BROWSE', 'USE CATALOG', 'USE SCHEMA', 'SELECT', 'EXECUTE', 'READ VOLUME',
               'APPLY TAG', 'MODIFY', 'CREATE FUNCTION', 'CREATE MATERIALIZED VIEW', 'CREATE MODEL', 'CREATE TABLE',
               'READ SECRET', 'REFRESH'],
    'admin':  ['ALL PRIVILEGES'],
    # 'genie_reader': ['USE SCHEMA'],
    # 'genie_admin':  ['USE SCHEMA']
}

# COMMAND ----------

genie_permissions = {
    'genie_reader': ['CAN_RUN'],
    'genie_admin':  ['CAN_MANAGE']
}