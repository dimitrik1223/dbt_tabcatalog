import os

from dbt_tableau.dbt_metadata_api import (
    dbt_get_account_id, dbt_get_projects, dbt_get_jobs, dbt_get_models_for_job,
    # generate_dbt_exposures
)
from dbt_tableau.tableau import (
    authenticate_tableau, 
    tableau_get_databases, 
    # tableau_get_downstream_workbooks, 
    merge_dbt_tableau_tables,
    get_tableau_columns,
    publish_tableau_column_descriptions,
    # verify_column_description,
    publish_tableau_table_description,
    restore_full_model_name
)
from dotenv import load_dotenv

load_dotenv()
#### dbt ####
API_BASE_URL = os.getenv("API_BASE_URL")
METADATA_API_URL = os.getenv("METADATA_API_URL")
DBT_API_KEY = os.getenv("DBT_API_PAT")
ACCOUNT_ID = dbt_get_account_id(API_BASE_URL,DBT_API_KEY)
#### Tableau ###
TABLEAU_SERVER =  os.getenv("TABLEAU_SERVER")
TABLEAU_SITE = os.getenv("TABLEAU_SITE")
TABLEAU_PAT_NAME = os.getenv("TABLEAU_PAT_NAME")
TABLEAU_PAT = os.getenv("TABLEAU_PAT")
TABLEAU_AUTH = authenticate_tableau(TABLEAU_SERVER, TABLEAU_SITE, TABLEAU_PAT_NAME, TABLEAU_PAT)

if __name__ == "__main__":
    dbt_projects = dbt_get_projects(ACCOUNT_ID,API_BASE_URL,DBT_API_KEY)
    # Need to specify dbt cloud PROD environment
    jobs = dbt_get_jobs(ACCOUNT_ID,API_BASE_URL,DBT_API_KEY, job_environment_ids=[1939])
    # Returns metadata on all models ran in 
    # common_run: https://cloud.getdbt.com/deploy/1708/projects/1499/jobs/117857
    models = dbt_get_models_for_job(METADATA_API_URL, DBT_API_KEY, 117857)
    # The "PRODUCTION" database in the Tableau Catalog is the only one containing
    # tables associated to workbooks
    tableau_databases = tableau_get_databases(TABLEAU_SERVER, TABLEAU_AUTH, ["PRODUCTION"])
    tableau_database_tables = restore_full_model_name(tableau_databases[1])
    merged_tables = merge_dbt_tableau_tables(tableau_databases[1], tableau_database_tables, models)
    for table in merged_tables:
        table_cols = get_tableau_columns(TABLEAU_SERVER, table, TABLEAU_AUTH)
        publish_tableau_column_descriptions(TABLEAU_SERVER, table, table_cols, TABLEAU_AUTH)
        publish_tableau_table_description(TABLEAU_SERVER, table, table['description'], TABLEAU_AUTH)

    # publish_tableau_table_description(TABLEAU_SERVER, merged_tables[10], merged_tables[10]['description'], TABLEAU_AUTH)
    #risk_rentals_cols = get_tableau_columns(TABLEAU_SERVER, merged_tables[43], TABLEAU_AUTH)
    # for col in risk_rentals_cols:
    #     print(f"{col['name']} description: {col['luid']}")
    #publish_tableau_column_tags(TABLEAU_SERVER, common_cars_cols, merged_tables[10], TABLEAU_AUTH)
    # verify_column_description(TABLEAU_SERVER, '32ed54d3-34c0-4621-b167-1bcdd2c29933', 'd236ae4d-a106-4278-9960-c591f0d26d6e', 'e21ee743-73d6-461d-9aa8-0b9c1a416e6b', TABLEAU_AUTH['token'])
    # # for i,model in enumerate(merged_tables):
    #     print(f"Model {model.get('name')}, index: {i},")
    # all_downstream_workbooks = []
    # for table in merged_tables:
    #     downstream_workbooks = tableau_get_downstream_workbooks(TABLEAU_SERVER, table, TABLEAU_AUTH)
    #     all_downstream_workbooks.append(downstream_workbooks)
    # generate_dbt_exposures(all_downstream_workbooks, TABLEAU_SERVER, TABLEAU_SITE, 1)
