# CI/CD Implementation - de_lakehouse_etl (Databricks Asset Bundles + GitHub Actions)

## 1. Overview

The AdventureWorks metadata-driven medallion pipeline (Raw → Bronze → Silver → Gold)
is deployed as a **Databricks Asset Bundle (DAB)** and released through
**GitHub Actions**. Every notebook lives in source control; deployments to
Databricks happen via CI, not manual notebook uploads.

```
Push to develop  ──▶  validate  ──▶  deploy to dev catalog   ──▶  smoke-test run
Push to main     ──▶  validate  ──▶  deploy to prod catalog  ──▶  (manual run to confirm)
```

Environment: **Databricks Free Edition** (single workspace, serverless
compute only - no classic clusters). `dev` and `prod` are logical
separations within the same workspace, distinguished by **catalog name**
(`de_lakehouse_dev` vs `de_lakehouse`), not by separate workspaces.

---

## 2. Repository structure

```
de_lakehouse_etl/
├── databricks.yml                 # bundle root: variables, dev/prod targets
├── resources/
│   └── aw_lakehouse_job.yml       # PL_Master_Load job definition (5 chained tasks)
├── src/
│   ├── 00_setup_unity_catalog.sql # schema/table DDL (notebook-formatted SQL)
│   ├── 01_seed_metadata.py        # seeds metadata.* config tables
│   ├── 02_bronze_ingestion.py     # Raw CSV -> Bronze Delta (metadata-driven loop)
│   ├── 03_silver_transform.py     # Bronze -> Silver (dedup, conform, merge)
│   └── 04_gold_dimensional.py     # Silver -> Gold star schema
├── .github/workflows/deploy.yml   # validate / deploy-dev / deploy-prod
├── .gitignore                     # excludes .databricks/ local sync state
└── README.md
```

### Job task chain (`resources/aw_lakehouse_job.yml`)

Mirrors the original ADF `PL_Master_Load` pipeline:

| Task | Type | Depends on | Purpose |
|---|---|---|---|
| `setup_unity_catalog` | `notebook_task` | - | Creates schemas + metadata/control/logging Delta tables |
| `seed_metadata` | `notebook_task` | setup_unity_catalog | Seeds `metadata.table_load_configuration` etc. |
| `bronze_ingestion` | `notebook_task` | seed_metadata | Raw CSV → Bronze, metadata-driven loop |
| `silver_transform` | `notebook_task` | bronze_ingestion | Bronze → Silver conformance |
| `gold_dimensional` | `notebook_task` | silver_transform | Silver → Gold star schema |

All tasks run on **serverless compute** (no `job_clusters` block - Free
Edition doesn't support classic clusters). `00_setup_unity_catalog.sql`
runs as a `notebook_task`, not `sql_task`, because the file carries the
`-- Databricks notebook source` header (workspace-exported format), which
the CLI treats as a notebook rather than a flat SQL file.

---

## 3. One-time local setup

### 3.1 Install the Databricks CLI (Windows)

```powershell
winget install Databricks.DatabricksCLI
```
Close and reopen the terminal after install (PATH doesn't refresh in the
current session). Verify:
```powershell
databricks --version
```

### 3.2 Authenticate

```powershell
databricks auth login --host https://<your-workspace-id>.cloud.databricks.com
```
Accept the bracketed default profile name when prompted (press Enter), or
supply your own - just note it down, since every later command references
it via `--profile`.

Check saved profiles at any time:
```powershell
databricks auth profiles
```

---

## 4. Bundle commands used

All run from the folder containing `databricks.yml`.

```powershell
# 1. Validate config (no deployment, just checks the YAML + references)
databricks bundle validate -t dev --profile <profile-name>

# 2. Deploy - uploads src/*.py and *.sql to the workspace, creates the job
databricks bundle deploy -t dev --profile <profile-name>

# 3. Run the full pipeline end-to-end
databricks bundle run pl_master_load -t dev --profile <profile-name>

# 4. Tear down (deletes the deployed job/workspace files, NOT the Delta tables)
databricks bundle destroy -t dev --profile <profile-name>
```

Swap `-t dev` for `-t prod` to target the production catalog.

`--var="warehouse_id=..."` was used earlier while `00_setup_unity_catalog.sql`
still ran as a `sql_task` against a SQL warehouse; it's no longer required
now that step runs as a `notebook_task` on serverless compute, but is
harmless to leave in commands (unused vars are silently ignored).

---

## 5. GitHub Actions setup

### 5.1 GitHub Environments
Repo → **Settings → Environments** → create `dev` and `prod`.

### 5.2 Secrets (per environment, or repo-level since Free Edition = one workspace)

| Secret | Value |
|---|---|
| `DATABRICKS_HOST_DEV` / `DATABRICKS_HOST_PROD` | workspace URL (same value both, single workspace) |
| `DATABRICKS_TOKEN_DEV` / `DATABRICKS_TOKEN_PROD` | PAT from Databricks Settings → Developer → Access tokens |
| `WAREHOUSE_ID_DEV` / `WAREHOUSE_ID_PROD` | SQL warehouse ID (currently unused by the job, kept for future sql_task use) |
| `NOTIFY_EMAIL` | email for job failure notifications |

### 5.3 Workflow triggers (`.github/workflows/deploy.yml`)

| Event | Job that runs |
|---|---|
| Any PR to `main`/`develop` | `validate` only |
| Push to `develop` | `validate` → `deploy-dev` → smoke-test run of `pl_master_load` |
| Push to `main` | `validate` → `deploy-prod` (no auto-run - schedule stays paused until confirmed manually) |
| Manual `workflow_dispatch` | choose `dev` or `prod` explicitly |

Host is **not** passed via `--var` in CI - the CLI resolves it from the
`DATABRICKS_HOST` environment variable, set per-step from the matching
secret. (See Section 6.2 - variable interpolation is not supported for
auth fields.)

---

## 6. Issues hit during setup, and the fix

| Symptom | Root cause | Fix |
|---|---|---|
| `databricks : term not recognized` in PowerShell right after install | New shell session hadn't picked up the updated PATH | Open a **new** terminal window (not tab) after install |
| `curl \| sh` failed in PowerShell | That install command is Bash-only | Use `winget install Databricks.DatabricksCLI` on Windows instead |
| `unknown command "\\"` on multi-line `--var` command | PowerShell doesn't use `\` for line continuation (Bash does) | Use a single line, or PowerShell's backtick `` ` `` for continuation |
| `invalid character "{" in host name` on `workspace.host: ${var.databricks_host}` | Databricks CLI does not support variable interpolation for **auth fields** (`host`/`token`) | Removed `host` from `databricks.yml` entirely; resolved via local profile or `DATABRICKS_HOST` env var instead |
| `has no dbc-7e66220f-da7b profile configured` | Typed a custom name ("Kishore Kumar") at the profile-name prompt instead of accepting the bracketed default | Re-ran `databricks auth login`, pressed Enter to accept the default profile name; confirmed both profiles via `databricks auth profiles` |
| `expected a file ... but got a notebook` on the SQL setup task | `00_setup_unity_catalog.sql` carries the `-- Databricks notebook source` header, so the CLI treats it as a notebook; `sql_task.file` requires a plain flat SQL file | Changed that task from `sql_task` to `notebook_task` in `resources/aw_lakehouse_job.yml` |

---

## 7. Known follow-up (not yet fixed)

`00_setup_unity_catalog.sql` still has `CREATE CATALOG IF NOT EXISTS de_lakehouse`
**hardcoded** rather than parameterized off the `catalog_name` widget the
way the Python notebooks are. This means `dev` and `prod` targets
currently both write into the same real `de_lakehouse` catalog instead of
`dev` getting its own isolated `de_lakehouse_dev`. Fine for now while
validating the end-to-end pipeline; worth parameterizing before relying on
`dev` as a true sandbox separate from prod data.

---

## 8. Day-to-day workflow going forward

```powershell
# edit a notebook in src/, then:
git add .
git commit -m "describe the change"
git checkout develop
git merge <your-branch>   # if working off a feature branch
git push                  # -> triggers validate + deploy-dev + smoke test

# once confirmed in dev:
git checkout main
git merge develop
git push                  # -> triggers validate + deploy-prod
```

No more manual notebook uploads through the Databricks UI - every change
flows through this pipeline.
