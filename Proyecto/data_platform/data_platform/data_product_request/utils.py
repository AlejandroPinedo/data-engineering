# Databricks notebook source
import re

def _validate_group(group):
    if not re.match(r'^g([12]|x)\d{2}$', group):
        raise ValueError(f"Invalid group '{group}'. Must be g101–g106 or g201–g206.")

def _validate_email(email, field='email'):
    if not re.match(r'^[^@\s]+@utec\.edu\.pe$', email.strip()):
        raise ValueError(f"Invalid {field}: '{email}'. Must be a @utec.edu.pe address.")

def _validate_domain_code(code):
    if not re.match(r'^[a-z]{3}$', code):
        raise ValueError(f"Invalid domain_code '{code}'. Must be exactly 3 lowercase letters.")

def _validate_name(value, field):
    if not value or len(value) > 20:
        raise ValueError(f"Invalid {field} '{value}'. Must be 1–20 characters.")

def _validate_inputs(group, leader, members_str, data_products):
    errors = []
    try: _validate_group(group)
    except ValueError as e: errors.append(str(e))

    try: _validate_email(leader, 'leader email')
    except ValueError as e: errors.append(str(e))

    for email in [m.strip() for m in members_str.split(',') if m.strip()]:
        try: _validate_email(email, 'member email')
        except ValueError as e: errors.append(str(e))

    if not data_products:
        errors.append("data_products list is empty.")

    for i, dp in enumerate(data_products):
        if len(dp) != 3:
            errors.append(f"data_products[{i}]: expected (domain_code, domain_name, dp_name).")
            continue
        code, name, dpn = dp
        try: _validate_domain_code(code)
        except ValueError as e: errors.append(str(e))
        try: _validate_name(name, 'domain_name')
        except ValueError as e: errors.append(str(e))
        try: _validate_name(dpn, 'dp_name')
        except ValueError as e: errors.append(str(e))

    if errors:
        print("Validation failed:")
        for e in errors:
            print(f"  - {e}")
        return False
    return True

def submit_dataproduct_request(group, leader, members, data_products):
    if not _validate_inputs(group, leader, members, data_products):
        return

    members_clean = ', '.join([m.strip() for m in members.split(',') if m.strip()])
    submitted = 0

    for domain_code, domain_name, dp_name in data_products:
        existing = spark.sql(f"""
            SELECT status FROM data_platform.admin.dataproduct_requests
            WHERE group = '{group}' AND domain_code = '{domain_code}' AND dp_name = '{dp_name}'
            ORDER BY submitted_at DESC
            LIMIT 1
        """).collect()

        if existing:
            status = existing[0]['status']
            if status == 'created':
                print(f"  [Domain: {domain_code} / Data Product: {dp_name}] Already created — skipped.")
            elif status == 'deleted':
                print(f"  [Domain: {domain_code} / Data Product: {dp_name}] Data product was deleted and cannot be created again. Propose another one.")
            elif status == 'failed':
                spark.sql(f"""
                    UPDATE data_platform.admin.dataproduct_requests
                    SET leader = '{leader}', members = '{members_clean}',
                        domain_name = '{domain_name}',
                        status = 'pending', error = NULL,
                        submitted_at = current_timestamp(), processed_at = NULL
                    WHERE group = '{group}' AND domain_code = '{domain_code}'
                        AND dp_name = '{dp_name}' AND status = 'failed'
                """)
                print(f"  [Domain: {domain_code} / Data Product: {dp_name}] Resubmitted (was failed).")
                submitted += 1
            else:
                spark.sql(f"""
                    UPDATE data_platform.admin.dataproduct_requests
                    SET leader = '{leader}', members = '{members_clean}',
                        domain_name = '{domain_name}',
                        submitted_at = current_timestamp()
                    WHERE group = '{group}' AND domain_code = '{domain_code}'
                        AND dp_name = '{dp_name}' AND status = 'pending'
                """)
                print(f"  [Domain: {domain_code} / Data Product: {dp_name}] Updated (still pending).")
                submitted += 1
        else:
            spark.sql(f"""
                INSERT INTO data_platform.admin.dataproduct_requests
                    (group, leader, members, domain_code, domain_name, dp_name, status, submitted_at)
                VALUES
                    ('{group}', '{leader}', '{members_clean}',
                     '{domain_code}', '{domain_name}', '{dp_name}',
                     'pending', current_timestamp())
            """)
            print(f"  [Domain: {domain_code} / Data Product: {dp_name}] Submitted.")
            submitted += 1

    print(f"\nDone. {submitted} data product(s) submitted for group {group}.")

    spark.sql(f"""
        SELECT group, domain_code, domain_name, dp_name, status, submitted_at, processed_at, error
        FROM data_platform.admin.dataproduct_requests
        WHERE group = '{group}'
        ORDER BY submitted_at
    """).display()