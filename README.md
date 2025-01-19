
## Ingest data from web to google cloud and link to bigquery
- DAG:
![alt text](image-5.png)


## Ingest data from local file to local postgres

- DAG
![alt text](image-3.png)

-LOG
![alt text](image-4.png)

## Project structure

workflow_orchestration/
├── Dockerfile           
├── docker-compose.yaml   
├── airflow/
│   └── dags/
│       ├── __init__.py
│       └── dbt_dag.py
│
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   ├── core/
│   │   └── marts/
│   ├── macros/
│   ├── seeds/
│   ├── tests/
│   ├── dbt_project.yml
│   └── profiles.yml