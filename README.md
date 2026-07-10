
# AdventureWorks Data Platform

## Overview

This repository contains all artefacts for the **AdventureWorks Data Platform** — an end-to-end Azure Data Engineering project covering ingestion, transformation, orchestration, and serving layers.

## Repository Structure

```
adventureworks-data-platform/
│
├── docs/                          # Project documentation
│   ├── FRD/                       # Functional Requirements Document
│   ├── HLD/                       # High-Level Design
│   ├── LLD/                       # Low-Level Design
│   ├── Architecture/              # Architecture diagrams
│   └── Images/                    # Supporting images
│
├── database/                      # SQL Server / Synapse SQL scripts
│   ├── framework/                 # Framework database objects
│   │   ├── 001_Create_Database.sql
│   │   ├── 002_Create_Schemas.sql
│   │   ├── metadata/              # Metadata tables
│   │   ├── control/               # Pipeline control tables
│   │   ├── audit/                 # Audit tables
│   │   ├── logging/               # Logging tables
│   │   ├── config/                # Configuration tables
│   │   ├── stored_procedures/     # Framework stored procedures
│   │   ├── views/                 # Framework views
│   │   └── seed_data/             # Seed / reference data scripts
│   │
│   ├── source/                    # Source system scripts
│   │   └── AdventureWorks/        # AdventureWorks OLTP scripts
│   │
│   └── warehouse/                 # Data Warehouse objects
│       ├── dimensions/            # Dimension table scripts
│       ├── facts/                 # Fact table scripts
│       ├── views/                 # Warehouse views
│       └── stored_procedures/     # Warehouse stored procedures
│
├── adf/                           # Azure Data Factory pipelines & ARM templates
│
├── databricks/                    # Databricks notebooks & libraries
│   ├── notebooks/                 # Jupyter/Databricks notebooks
│   ├── common/                    # Shared utility functions
│   ├── config/                    # Databricks configuration
│   ├── framework/                 # Framework notebooks
│   ├── bronze/                    # Bronze layer (raw ingestion)
│   ├── silver/                    # Silver layer (cleansed/conformed)
│   ├── gold/                      # Gold layer (aggregated/serving)
│   └── tests/                     # Databricks unit tests
│
├── config/                        # Environment configuration files
│
├── deployment/                    # Deployment automation
│   ├── github-actions/            # GitHub Actions workflows
│   ├── environments/              # Environment-specific configs
│   └── scripts/                   # Deployment helper scripts
│
├── tests/                         # Integration & end-to-end tests
│
├── .gitignore
├── LICENSE
└── README.md
```

## Technology Stack

| Layer         | Technology                        |
|---------------|-----------------------------------|
| Source        | SQL Server (AdventureWorks OLTP)  |
| Ingestion     | Azure Data Factory                |
| Storage       | Azure Data Lake Storage Gen2      |
| Processing    | Azure Databricks (PySpark)        |
| Serving       | Azure Synapse Analytics / SQL DW  |
| Orchestration | Azure Data Factory                |
| CI/CD         | GitHub Actions                    |

## Getting Started

1. Clone the repository.
2. Review `docs/HLD/` for the high-level architecture.
3. Run `database/framework/001_Create_Database.sql` followed by `002_Create_Schemas.sql` to set up the framework database.
4. Follow the setup guides in each sub-folder's README.

## Contributing

Please follow the branching strategy defined in `docs/` and raise a pull request for all changes.

## License

See [LICENSE](LICENSE) for details.
