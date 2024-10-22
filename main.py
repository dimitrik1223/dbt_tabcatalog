import os
import json

from dbt_tableau.dbt_metadata_api import (
    dbt_get_account_id, dbt_get_projects, dbt_get_jobs, dbt_get_models_for_job,
    generate_dbt_exposures
)
from dbt_tableau.tableau_api import authenticate_tableau, tableau_get_databases, tableau_get_downstream_workbooks, merge_dbt_tableau_tables
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

def restore_full_model_name(tableau_database_tables):
    for table in tableau_database_tables['tables']:
        if table['schema'].lower() == 'common':
            table['name'] = f"common_{table['name']}"
        elif table['schema'].lower() == 'core_ng':
            table['name'] = f"core_{table['name']}"
    return tableau_database_tables

if __name__ == "__main__":
    dbt_projects = dbt_get_projects(ACCOUNT_ID,API_BASE_URL,DBT_API_KEY)
    jobs = dbt_get_jobs(ACCOUNT_ID,API_BASE_URL,DBT_API_KEY, job_environment_ids=[1939])
    ### Returns metadata on all models
    models = dbt_get_models_for_job(METADATA_API_URL, DBT_API_KEY, 117857)
    reporting_models_metadata = []
    # for model in models:
    #     if "reporting" in model.get("schema"):
    #         print(f"Model: {model.get('name')}, dbt discovery API metadata: {model}")
    
    Three production databases in tableau
    tableau_databases = tableau_get_databases(TABLEAU_SERVER, TABLEAU_AUTH)
    tableau_database_tables = restore_full_model_name(tableau_databases[1])
    merged_tables = merge_dbt_tableau_tables(tableau_databases[1], tableau_database_tables, models)
    # for i,model in enumerate(merged_tables):
    #     print(f"Model {model.get('name')}, index: {i},")
    all_downstream_workbooks = []
    for table in merged_tables:
        downstream_workbooks = tableau_get_downstream_workbooks(TABLEAU_SERVER, table, TABLEAU_AUTH)
        all_downstream_workbooks.append(downstream_workbooks)
    generate_dbt_exposures(all_downstream_workbooks, TABLEAU_SERVER, TABLEAU_SITE, 1)
