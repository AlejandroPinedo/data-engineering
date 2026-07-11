# Databricks notebook source
# MAGIC %run ./_params

# COMMAND ----------

import requests
import json
import time

# COMMAND ----------

def _safe_json(response):
    try:
        return response.json()
    except Exception:
        print(f'Non-JSON response ({response.status_code}): {response.text[:200]}')
        return None

# COMMAND ----------

def _current_user_in_group(group_name):
    return spark.sql(f"SELECT IS_ACCOUNT_GROUP_MEMBER('{group_name}')").collect()[0][0]

# COMMAND ----------

def _get_user_id(email):
    response = requests.get(
        f'{endpoints["account_users_endpoint"]}?filter=userName eq "{email}"',
        headers=headers
    )
    data = _safe_json(response)
    if not data:
        return None
    resources = data.get('Resources', [])
    return resources[0]['id'] if resources else None

# COMMAND ----------

def _get_group_id(group_name):
    response = requests.get(
        f'{endpoints["account_groups_endpoint"]}?filter=displayName eq "{group_name}"',
        headers=headers
    )
    data = _safe_json(response)
    if not data:
        return None
    resources = data.get('Resources', [])
    return resources[0]['id'] if resources else None

# COMMAND ----------

def _assign_group_to_workspace(group_id):
    response = requests.put(
        f'https://accounts.azuredatabricks.net/api/2.0/accounts/{account_id}/workspaces/{workspace_id}/permissionassignments/principals/{group_id}',
        headers=headers,
        data=json.dumps({"permissions": ["USER"]})
    )
    if response.status_code in [200, 204]:
        return True
    print(f'Failed to assign group to workspace ({response.status_code}): {response.text}')
    return False

# COMMAND ----------

def _add_member_to_group(group_id, member_id):
    data = {
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
        "Operations": [{
            "op": "add",
            "path": "members",
            "value": [{"value": member_id}]
        }]
    }
    response = requests.patch(
        f'{endpoints["account_groups_endpoint"]}/{group_id}',
        headers=headers,
        data=json.dumps(data)
    )
    return response.status_code in [200, 204]

# COMMAND ----------

def create_groups(catalog_name):
    group_ids = {}
    for role in privileges.keys():
        group_name = f'{catalog_name}_{role}'
        data = {
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
            "displayName": group_name
        }
        response = requests.post(
            endpoints['account_groups_endpoint'],
            headers=headers,
            data=json.dumps(data)
        )
        if response.status_code == 201:
            group_id = response.json()['id']
            group_ids[role] = {'name': group_name, 'id': group_id}
            _assign_group_to_workspace(group_id)
            print(f'Group created and assigned to workspace: {group_name}')
        elif response.status_code == 409:
            group_id = _get_group_id(group_name)
            if group_id:
                group_ids[role] = {'name': group_name, 'id': group_id}
                _assign_group_to_workspace(group_id)
                print(f'Group already exists, reusing: {group_name}')
        else:
            print(f'Failed to create group {group_name} ({response.status_code}): {response.text}')
    return group_ids

# COMMAND ----------

def grant_catalog_privileges(catalog_name, group_ids):
    if not group_ids:
        print('No groups to grant privileges to, skipping.')
        return
    for role, group_info in group_ids.items():
        group_name = group_info['name']
        grants = ', '.join(privileges[role])
        spark.sql(f'GRANT {grants} ON CATALOG {catalog_name} TO `{group_name}`')
        print(f'Privileges granted to {group_name}')

# COMMAND ----------

def assign_users_to_groups(group_ids, leader, members, reader_group='utec-de'):
    if not group_ids:
        print('No groups found, skipping user assignment.')
        return

    if 'admin' in group_ids:
        leader_id = _get_user_id(leader)
        if leader_id:
            _add_member_to_group(group_ids['admin']['id'], leader_id)
            print(f'{leader} added to admin group')
        else:
            print(f'User not found: {leader}')

    if 'writer' in group_ids:
        for email in members:
            member_id = _get_user_id(email)
            if member_id:
                _add_member_to_group(group_ids['writer']['id'], member_id)
                print(f'{email} added to writer group')
            else:
                print(f'User not found: {email}')

    if 'reader' in group_ids:
        reader_group_id = _get_group_id(reader_group)
        if reader_group_id:
            _add_member_to_group(group_ids['reader']['id'], reader_group_id)
            print(f'Group "{reader_group}" added to reader group')
        else:
            print(f'Group not found: {reader_group}')

# COMMAND ----------

def delete_groups(catalog_name):
    for role in privileges.keys():
        group_name = f'{catalog_name}_{role}'
        group_id = _get_group_id(group_name)
        if group_id:
            response = requests.delete(
                f'{endpoints["account_groups_endpoint"]}/{group_id}',
                headers=headers
            )
            if response.status_code == 204:
                print(f'Group deleted: {group_name}')
            else:
                print(f'Failed to delete group {group_name}: {response.text}')
        else:
            print(f'Group not found, skipping: {group_name}')

# COMMAND ----------

def generate_dataproduct(domain, dp_name, leader, members, action='create'):
    if not _current_user_in_group('utec-admin'):
        print('Access denied: you must belong to the utec-admin group to manage data products.')
        return

    catalog_name = f'{domain["code"]}_{dp_name}'
    description = f'Data product de {dp_name} del dominio de {domain["name"]}'

    if action == 'create':
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name} COMMENT '{description}'")
        for schema_name, schema_desc in schema_descriptions.items():
            full_schema = f'{catalog_name}.{schema_name}'
            spark.sql(f'CREATE SCHEMA IF NOT EXISTS {full_schema} COMMENT "{schema_desc}"')
        print(f'Catalog {catalog_name} created')

        group_ids = create_groups(catalog_name)
        grant_catalog_privileges(catalog_name, group_ids)  # GRANT while SP still owns catalog
        assign_users_to_groups(group_ids, leader, members)

        # Transfer ownership and schema ownership to admin after all grants are done
        for schema_name in schema_descriptions.keys():
            spark.sql(f'ALTER SCHEMA {catalog_name}.{schema_name} OWNER TO `{admin}`')
        spark.sql(f'ALTER CATALOG {catalog_name} OWNER TO `{admin}`')

    elif action == 'delete':
        delete_groups(catalog_name)

        for schema_name in schema_descriptions.keys():
            spark.sql(f'DROP SCHEMA IF EXISTS {catalog_name}.{schema_name} CASCADE')
        spark.sql(f'DROP SCHEMA IF EXISTS {catalog_name}.default CASCADE')
        spark.sql(f'DROP CATALOG IF EXISTS {catalog_name} CASCADE')
        print(f'Data product {catalog_name} deleted')