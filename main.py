import os

from dbt_tableau.dbt_metadata_api import dbtClient
from dbt_tableau.tableau import tableauClient
from dotenv import load_dotenv

load_dotenv()
#### dbt ####
API_BASE_URL = os.getenv("API_BASE_URL")
METADATA_API_URL = os.getenv("METADATA_API_URL")
DBT_API_KEY = os.getenv("DBT_API_PAT")
#### Tableau ###
TABLEAU_SERVER_URL =  os.getenv("TABLEAU_SERVER")
TABLEAU_SITE_NAME = os.getenv("TABLEAU_SITE")
TABLEAU_PAT_NAME = os.getenv("TABLEAU_PAT_NAME")
TABLEAU_PAT = os.getenv("TABLEAU_PAT")

def restore_full_model_name(tables_json):
    """
    Adds aliases back to FQN tables found in a Tableau catalog database.
    """
    for tab in tables_json['tables']:
        if tab["schema"].lower() == "common":
            tab["name"] = f"common_{tab['name']}"
        elif tab["schema"].lower() == 'core_ng':
            tab["name"] = f"core_{tab['name']}"

    return tables_json

if __name__ == "__main__":
    # Initialize dbt API client
    dbt_client = dbtClient(API_BASE_URL, DBT_API_KEY, METADATA_API_URL)
    # Initialize Tableau API client
    tableau_client = tableauClient(
        TABLEAU_SERVER_URL,
        TABLEAU_SITE_NAME,
        TABLEAU_PAT_NAME,
        TABLEAU_PAT
    )
    account_id = dbt_client.get_account_id()
    # Need to specify dbt cloud PROD environment ID 1939
    jobs = dbt_client.get_jobs(account_id, job_environment_ids=[1939])
    # Returns metadata on all models ran in
    # common_run: https://cloud.getdbt.com/deploy/1708/projects/1499/jobs/117857
    models = dbt_client.get_models_for_job(117857)
    # The "PRODUCTION" database in the Tableau Catalog is the only one containing
    # tables associated to workbooks
    tableau_creds = tableau_client.authenticate()
    tableau_databases = tableau_client.get_databases(tableau_creds, ["PRODUCTION"])
    tableau_database_tables = restore_full_model_name(tableau_databases[1])
    merged_tables = tableau_client.merge_table_metadata(
        tableau_databases[1],
        tableau_database_tables,
        models
    )
    for table in merged_tables:
        table_cols = tableau_client.get_column_metadata(table, tableau_creds)
        tableau_client.publish_column_descriptions(table, table_cols, tableau_creds)
        tableau_client.publish_table_description(
            table, 
            table["description"], 
            tableau_creds
        )

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
