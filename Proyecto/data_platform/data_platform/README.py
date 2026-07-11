# Databricks notebook source
# Data Platform

Automation system for managing student data products in the DE course.
Uses Databricks Unity Catalog to create and govern catalogs per student group.

## Folder Structure

```
data_platform/
├── common/                  # Shared code — do not modify unless you know what you are doing
│   ├── _params.py           # Auth and endpoint configuration (service principal)
│   ├── utils.py             # Core functions: create/delete catalogs, groups, and grants
│   └── test_utils.py        # Unit tests for utils.py
├── data_product_request/    # Shared with students
│   ├── submit_request.py    # Students fill this and run it to submit their request
│   └── utils.py             # Validation and table insert/update logic
└── manage/                  # Professor-only tools
    ├── setup_requests_table.py  # Run once to create the requests table and grants
    ├── create_dataproducts.py   # Processes pending requests and creates data products
    └── delete_dataproducts.py   # Deletes created data products and marks them as deleted
```

## Workflow

### 1. Setup (once)
Run `manage/setup_requests_table.py` as the catalog owner (`atintaya@utec.edu.pe`).
Creates `data_platform.admin.dataproduct_requests` and grants `utec-de` group access to it.

### 2. Student submission
Students open `data_product_request/submit_request.py`, fill in their details, and run it.
Each data product they define generates one row with status `pending` in the requests table.

### 3. Create data products (professor)
Run `manage/create_dataproducts.py` at any time.
Processes all `pending` rows, creates the corresponding Unity Catalog catalogs with groups and grants, then marks each row as `created`. Safe to run multiple times.

### 4. Delete data products (professor)
Run `manage/delete_dataproducts.py` to clean up.
Specify groups in `groups_to_delete` or leave empty to delete all `created` ones.
Marks deleted rows as `deleted` — students cannot resubmit a deleted data product.

## Request Table — Status Lifecycle

| Status    | Meaning                                              |
|-----------|------------------------------------------------------|
| `pending` | Submitted by student, waiting to be processed        |
| `created` | Catalog created successfully                         |
| `failed`  | Creation failed — student can resubmit               |
| `deleted` | Catalog deleted by professor — cannot be recreated   |

## Group Format
Groups follow the format `g101–g106` (section 1) or `g201–g206` (section 2).