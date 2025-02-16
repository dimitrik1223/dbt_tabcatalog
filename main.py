import os
import requests
import logging

from dbt_tableau.dbt_metadata_api import get_models_for_job
from dbt_tableau.tableau import tableauClient
# from dbt_tableau.extract_job_runs import get_dbt_jobs
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

def verify_column_description(
    tableau_server,
    site_id: str,
    table_id: str,
    column_id: str,
    token: str
    ):
    """
    Verifies that a column description was actually updated in Tableau.
    Returns the current description or None if verification fails.
    """
    try:
        url = (
            f"{tableau_server}/api/3.23/sites/"
            f"{site_id}/tables/{table_id}/columns/{column_id}"
        )
        headers = {
            "X-Tableau-Auth": token,
            "Accept": "application/xml"
        }
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        # Parse response XML to get description
        # You'll need to implement XML parsing based on the response format
        # Return the description if found
        print(response.text)
    except Exception as e:
        logging.error("Error verifying column description: %s", str(e))
        return None

def restore_full_model_name(tables_json):
    """
    Adds aliases back to FQN tables found in a Tableau catalog database.
    """
    for tab in tables_json["tables"]:
        tab["fullName"] = tab["fullName"].replace("[", "").replace("]", "")
        fqn_split = tab["fullName"].split(".")
        if len(fqn_split) == 1:
            tab["fullName"] = f"PRODUCTION.{tab['schema']}.{tab['fullName']}"
        elif len(fqn_split) == 2:
            tab["fullName"] = f"PRODUCTION.{tab['fullName']}"
        else:
            tab["fullName"] = tab["fullName"]
    return tables_json

if __name__ == "__main__":
    # Initialize Tableau API client
    tableau_client = tableauClient(
        TABLEAU_SERVER_URL,
        TABLEAU_SITE_NAME,
        TABLEAU_PAT_NAME,
        TABLEAU_PAT
    )
    # Need to specify dbt cloud PROD environment ID 1939
    # jobs = get_dbt_jobs(account_id)
    # Returns metadata on all models ran in
    models = get_models_for_job(METADATA_API_URL, DBT_API_KEY, )

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
    # for table in merged_tables:
    #     table_cols = tableau_client.get_column_metadata(table, tableau_creds)
    #     tableau_client.publish_column_descriptions(table, table_cols, tableau_creds)
    #     tableau_client.publish_table_description(table, table["description"], tableau_creds)

