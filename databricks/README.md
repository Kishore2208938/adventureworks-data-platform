# de_lakehouse_etl - Databricks Asset Bundle + GitHub Actions CI/CD

Metadata-driven medallion pipeline (AdventureWorks) deployed as a Databricks
Asset Bundle. `PL_Master_Load` runs five chained tasks that mirror the
original ADF pipeline:

```
setup_unity_catalog -> seed_metadata -> bronze_ingestion -> silver_transform -> gold_dimensional
```

## Repo layout

```
de_lakehouse_etl/
├── databricks.yml                 # bundle root config, dev/prod targets
├── resources/
│   └── aw_lakehouse_job.yml       # PL_Master_Load job + task chain
├── src/
│   ├── 00_setup_unity_catalog.sql
│   ├── 01_seed_metadata.py
│   ├── 02_bronze_ingestion.py
│   ├── 03_silver_transform.py
│   └── 04_gold_dimensional.py
└── .github/workflows/deploy.yml   # validate / deploy-dev / deploy-prod
```

## One-time setup

**1. Create two service principals or PATs** - one for `dev`, one for `prod`
workspaces (can be the same workspace with different catalogs, or two
separate workspaces).

**2. In GitHub: Settings -> Environments**, create `dev` and `prod`
environments (lets you require manual approval before prod deploys, add
branch protection, etc.).

**3. Add these repo/environment secrets:**

| Secret | Used in | Value |
|---|---|---|
| `DATABRICKS_HOST_DEV` | dev | e.g. `https://adb-xxxx.azuredatabricks.net` |
| `DATABRICKS_TOKEN_DEV` | dev | PAT or service principal token |
| `WAREHOUSE_ID_DEV` | dev | SQL warehouse ID for the setup SQL task |
| `DATABRICKS_HOST_PROD` | prod | prod workspace URL |
| `DATABRICKS_TOKEN_PROD` | prod | prod PAT/service principal token |
| `WAREHOUSE_ID_PROD` | prod | prod SQL warehouse ID |
| `NOTIFY_EMAIL` | both | email for job failure alerts |

**4. Branch strategy:**
- `develop` branch → auto-deploys to `dev` + runs a smoke-test job execution
- `main` branch → auto-deploys to `prod` (schedule stays paused until you
  flip `pause_status: UNPAUSED`, already set for prod in `databricks.yml`)
- Any PR → `bundle validate` only, no deploy

## Local development

```bash
# install once
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# authenticate interactively (creates a local profile - CLI resolves host
# from this profile automatically; do NOT pass --var for host, it's not
# supported for auth fields)
databricks auth login --host https://your-workspace-url

# validate (uses your default profile automatically)
databricks bundle validate -t dev

# or, if you have multiple profiles, be explicit:
databricks bundle validate -t dev --profile <your-profile-name>

# deploy to your own dev catalog
databricks bundle deploy -t dev --var="warehouse_id=<your-warehouse-id>"

# run the whole pipeline on demand
databricks bundle run pl_master_load -t dev

# tear down (deletes the deployed job, not your Delta tables)
databricks bundle destroy -t dev
```

## Adding a 6th+ table later

You don't touch the job definition. Add the table to `CSV_SCHEMAS` in
`02_bronze_ingestion.py` and add a row to `table_defs` in
`01_seed_metadata.py`, then push — the same five tasks pick it up
automatically on the next run.
