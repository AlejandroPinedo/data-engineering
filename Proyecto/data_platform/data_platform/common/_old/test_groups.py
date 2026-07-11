# Databricks notebook source
"""
for group, access in privileges.items():
    group_name = f'{data_product["catalog_name"]}_{group}'
    data = {"displayName": group_name}
    response = requests.post(
        endpoints['groups_endpoint'],
        headers=headers,
        data=json.dumps(data)
    )
    if response.status_code == 201:
        print(f"Group {group_name} created successfully")
        
        tries = 0
        max_tries = 5

        while tries < max_tries:
            try:
                spark.sql(f'GRANT USAGE ON CATALOG {data_product["catalog_name"]} TO `{group_name}`')
                break
            except Exception as e:
                tries += 1
                print(f'CATALOG USAGE RETRY => [{tries}/{max_tries}]')
                time.sleep(5)
        
        spark.sql(f'GRANT {", ".join(access)} ON CATALOG {data_product["catalog_name"]} TO `{group_name}`')
    else:
        print(f"Failed to create group {group_name}: {response.text}")
"""