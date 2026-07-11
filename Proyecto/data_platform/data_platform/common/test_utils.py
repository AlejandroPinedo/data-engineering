# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Configuration

# COMMAND ----------

TEST_CATALOG = 'test_dp'
TEST_LEADER  = 'atintaya@utec.edu.pe'
TEST_MEMBERS = ['atintaya@utec.edu.pe', 'angel.tintaya@utec.edu.pe']

results = []

def log(msg):
    results.append(msg)
    print(msg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Test Auth — verify the account token works

# COMMAND ----------

import requests as _r
resp = _r.get(endpoints['account_groups_endpoint'], headers=headers)
if resp.status_code == 200:
    log(f'[OK] Auth — account has {resp.json().get("totalResults", 0)} groups')
else:
    log(f'[FAIL] Auth: {resp.status_code} {resp.text}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Test _get_user_id

# COMMAND ----------

user_id = _get_user_id(TEST_LEADER)
if user_id:
    log(f'[OK] _get_user_id: {TEST_LEADER} -> {user_id}')
else:
    log(f'[FAIL] _get_user_id: {TEST_LEADER} not found')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Test _get_group_id (before creation — should return None)

# COMMAND ----------

existing = _get_group_id(f'{TEST_CATALOG}_reader')
if existing is None:
    log(f'[OK] _get_group_id before creation: None as expected')
else:
    log(f'[WARN] _get_group_id before creation: group already exists ({existing}) — leftover from previous run')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Test create_groups (also creates the catalog for grant testing)

# COMMAND ----------

# Drop first to ensure SP always owns a fresh catalog (avoids leftover state from failed runs)
try:
    spark.sql(f'DROP CATALOG IF EXISTS {TEST_CATALOG} CASCADE')
except Exception:
    pass
spark.sql(f'CREATE CATALOG IF NOT EXISTS {TEST_CATALOG}')
group_ids = create_groups(TEST_CATALOG)
if len(group_ids) == 3:
    log(f'[OK] create_groups: {list(group_ids.keys())}')
else:
    missing = [r for r in ['reader', 'writer', 'admin'] if r not in group_ids]
    log(f'[FAIL] create_groups: missing {missing}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Test grant_catalog_privileges

# COMMAND ----------

try:
    grant_catalog_privileges(TEST_CATALOG, group_ids)
    log(f'[OK] grant_catalog_privileges: privileges granted on {TEST_CATALOG}')
except Exception as e:
    log(f'[FAIL] grant_catalog_privileges: {e}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Test assign_users_to_groups

# COMMAND ----------

assign_users_to_groups(group_ids, TEST_LEADER, TEST_MEMBERS)
log('[OK] assign_users_to_groups: completed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## CLEANUP — revoke privileges then delete groups

# COMMAND ----------

for role, group_info in group_ids.items():
    try:
        spark.sql(f'REVOKE ALL PRIVILEGES ON CATALOG {TEST_CATALOG} FROM `{group_info["name"]}`')
        log(f'[OK] Revoked privileges from {group_info["name"]}')
    except Exception as e:
        log(f'[WARN] Revoke {group_info["name"]}: {e}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Test delete_groups

# COMMAND ----------

delete_groups(TEST_CATALOG)
log('[OK] delete_groups: test groups deleted')
try:
    spark.sql(f'DROP CATALOG IF EXISTS {TEST_CATALOG} CASCADE')
    log(f'[OK] Catalog {TEST_CATALOG} dropped')
except Exception as e:
    log(f'[WARN] Could not drop catalog {TEST_CATALOG} (may be owned by another user): {e}')

# COMMAND ----------

dbutils.notebook.exit('\n'.join(results))