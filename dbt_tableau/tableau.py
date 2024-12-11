import requests
import json
from collections import defaultdict
from operator import itemgetter
from typing import List, Dict, Any, Optional
import xml.sax.saxutils as saxutils
import logging
import html
import xml.etree.ElementTree as ET
import xml.etree.ElementTree as ET # For parsing XML responses
from urllib.request import urlopen, Request
import requests
import json
from collections import defaultdict
from operator import itemgetter
import logging

TABLEAU_API_VERSION='3.17'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def authenticate_tableau(tableau_server: str, tableau_site_name: str, tableau_pat_name: str, tableau_pat: str) -> json:
    """
    Authenticates with Tableau server and returns authentication object containing a API token
    as well as the site ID.
    args:
        tableau_server: the Tableau sites base URL. 
        tableau_site_name: The name of the Tableau site (drivybusinessintelligence).
        tableau_pat_name: The name of the PAT (Personal Access Token) generated in Tableau.
        tableau_pat: The corresponding token.
    """
    url = f"{tableau_server}/api/{TABLEAU_API_VERSION}/auth/signin"
    logger.info("Authenticating with Tableau server url: %s", url)
    payload = json.dumps({
        "credentials": {
            "personalAccessTokenName": tableau_pat_name,
            "personalAccessTokenSecret": tableau_pat,
            "site": {
                "contentUrl": tableau_site_name
            }
        }
    })
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    try:
        response = requests.post(url, headers=headers, data=payload)
        response_json = response.json()
        if "errors" in response_json:
            raise Exception(response_json["errors"][0]["message"])

        tableau_creds = response_json["credentials"]
        logger.info("Tableau user ID: %s", str(tableau_creds["user"]["id"]))
    except Exception as e:
        logger.error("Error authenticating with Tableau. Error: %s", e)

    return tableau_creds

def restore_full_model_name(tableau_database_tables):
    """
    Adds aliases back to FQN tables found in a Tableau catalog database.
    """
    for table in tableau_database_tables['tables']:
        if table["schema"].lower() == "common":
            table["name"] = f"common_{table['name']}"
        elif table["schema"].lower() == 'core_ng':
            table["name"] = f"core_{table['name']}"

    return tableau_database_tables

def tableau_get_databases(tableau_server: str, tableau_auth: dict, databases: list) -> List[Dict[str, Any]]:
    """
    Retrieves the metadata of all the tables within specified databases from the Tableau Catalog 
    using the Tableau metadata API. 
    args:
        databases: list of databases you'd like to return the table name, schema, id, and luid of.
    """
    db_type = 'connectionType: "snowflake"'
    snowflake_database = f"{db_type}, nameWithin: {json.dumps(databases)}"
    query = """
    query get_databases {
        databases(filter: {%s}) {
            name
            id
            tables {
                name
                schema
                id
                luid
            }
        }
    }
    """ % snowflake_database
    headers = {
        "accept": "application/json", 
        "content-type": "application/json",
        "x-tableau-auth": tableau_auth["token"]
    }

    try:
        response = requests.post(
            f"{tableau_server}/api/metadata/graphql",
            headers=headers,
            json={"query": query},
            verify=True,
            timeout=60
        )
        response.raise_for_status()
        response_json = response.json()

        if "data" not in response_json or "databases" not in response_json["data"]:
            raise KeyError(
                f"Response missing required data structure for Graph QL query: {query}"
            )
        tableau_databases = response_json["data"]["databases"]
        logger.info("Retrieved %s Tableau databases", str(len(tableau_databases)))
        return tableau_databases
    
    except requests.exceptions.Timeout as e:
        logger.error("Timeout error connecting to Tableau metadata API: %s", str(e))
        raise
    except requests.exceptions.RequestException as e:
        logging.error("API request failed: %s", str(e))
        raise
    except json.JSONDecodeError as e:
        logging.error("Failed to parse API response: %s", str(e))
        raise
    except KeyError as e:
        logger.error("Invalid response structure: %s", str(e))
        raise
    except Exception as e:
        logger.error("Unexpected error: %s", str(e))
        raise

def merge_dbt_tableau_tables(
    tableau_database:list,
    tableau_database_tables: list,
    dbt_models: list
    ) -> list:
    """
    Combines the metatdata of dbt models retrieved from the dbt cloud API and the metadata
    of all the Tableau tables within a specified Tableau Catalog database.
    
    args:
        tableau_database: List of all tables within a Tableau Catalog database retrieved with the 
        function tableau_get_databases .
        tableau_database_tables: List of tables in a Tableau Catalog database 
    
    Returns: a list of merged (i.e. matched database/schema/table name) 
        Tableau database tables and dbt models
    """
    table_model_map = defaultdict(dict)
    for table in tableau_database_tables["tables"]:
        table_database = tableau_database["name"].lower()
        table_schema = table["schema"].lower()
        table_name = table["name"].lower()
        tableau_table_fqn = f"{table_database}.{table_schema}.{table_name}"
        for model in dbt_models:
            model_database = model["database"].lower()
            model_schema = model["schema"].lower()
            model_name = model["name"].lower()
            dbt_table_fqn = f"{model_database}.{model_schema}.{model_name}"
            if tableau_table_fqn == dbt_table_fqn:
                table_model_map[model["name"].lower()].update(table)
                table_model_map[table["name"].lower()].update(model)
    merged_tables = sorted(table_model_map.values(), key=itemgetter("name"))

    logger.info(
        "Merged %s dbt and Tableau tables in Tableau database: %s", 
        str(len(merged_tables)), tableau_database["name"]
    )

    return merged_tables

def format_table_references_tableau(merged_table: dict) -> str:
    """
    Formats fully qualified table relations into the format used
    by Tableau internally: [DATABASE].[SCHEMA].[TABLE]
    """
    database = merged_table["database"].upper()
    schema = merged_table["schema"].upper()
    table = merged_table["name"].upper()
    full_table_name = f"{database}].[{schema}].[{table}]"

    return full_table_name

#returns a list of downstream workbooks (filter using database_type_filter and database_name_filter)
def tableau_get_downstream_workbooks(
        tableau_server: str,
        merged_table: dict,
        tableau_creds: dict
    ) -> list:
    """
    Fetches list of metadata for each Tableau workbook that sits
    downstream of a specified table.
    args:
        merged_table (dict): Dictionary containing metadata from both 
            dbt and Tableau for a given table within a database in the
            Tableau catalog.
    """
    fqn_tableau = format_table_references_tableau(merged_table)
    logger.info("Getting downstream workbooks for table: %s", fqn_tableau)
    table_filter = f'luid: "{merged_table["luid"]}"'

    mdapi_query = """
    query get_downstream_workbooks {
        databaseTables(filter: {%s}) {
            name
            id
            luid
            downstreamWorkbooks {
                id
                luid
                name
                description
                projectName
                vizportalUrlId
                tags {
                    id
                    name
                }
                owner {
                    id
                    name
                    username
                }
                upstreamTables
                {
                    id
                    luid
                    name
                }
            }
        }
    }
    """ % table_filter

    auth_headers = {
        "accept": "application/json", 
        "content-type": "application/json",
        "x-tableau-auth": tableau_creds["token"]
    }

    try:
        response = requests.post(
            f"{tableau_server}/api/metadata/graphql", 
            headers=auth_headers, 
            verify=True,
            json={"query": mdapi_query},
            timeout=60
        )
        response.raise_for_status()
        response_json = response.json()
        downstream_workbooks = response_json["data"]["databaseTables"][0]["downstreamWorkbooks"]
        logger.info("Retrieved %s downstream Tableau workbooks.", str(len(downstream_workbooks)))

        return downstream_workbooks

    except requests.exceptions.Timeout as e:
        logger.error("Timeout error connecting to Tableau metadata API: %s", str(e))
        raise
    except requests.exceptions.RequestException as e:
        logging.error("API request failed: %s", str(e))
        raise
    except json.JSONDecodeError as e:
        logging.error("Failed to parse API response: %s", str(e))
        raise
    except KeyError as e:
        logger.error("Invalid response structure: %s", str(e))
        raise
    except Exception as e:
        logger.error("Unexpected error: %s", str(e))
        raise

def get_tableau_columns(tableau_server: str, merged_table: dict, tableau_creds: dict) -> list:
    """
    Retrieves all columns for a table within the Tableau catalog.
    """

    headers = {
        "X-tableau-Auth": tableau_creds["token"],
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    # JSON object to pass luid of a column to the graphQL
    variables = {"luid": merged_table["luid"]}
    mdapi_query = """
    query getColumns($luid: String!) {
        databaseTables(filter: {luid: $luid}) {
            columns {
                name
                id
                description
                luid
                remoteType
                isNullable
            }
        }
    }
    """

    try:
        response = requests.post(
            f"{tableau_server}/api/metadata/graphql",
            headers=headers,
            json={"query": mdapi_query, "variables": variables},
            timeout=60
        )
        response.raise_for_status()
        response_json = response.json()
        columns = response_json["data"]["databaseTables"][0]["columns"]
        logger.info("Retrieved %s columns for table %s", str(len(columns)), merged_table["name"])

        return columns

    except requests.exceptions.Timeout as e:
        logger.error("Timeout error connecting to Tableau metadata API: %s", str(e))
        raise
    except requests.exceptions.RequestException as e:
        logging.error("API request failed: %s", str(e))
        raise
    except json.JSONDecodeError as e:
        logging.error("Failed to parse API response: %s", str(e))
        raise
    except KeyError as e:
        logger.error("Invalid response structure: %s", str(e))
        raise
    except Exception as e:
        logger.error("Unexpected error: %s", str(e))
        raise

def publish_tableau_column_descriptions(
    tableau_server: str,
    merged_table: dict,
    tableau_columns: dict,
    tableau_creds: dict
    ) -> int:
    """
    Publishes column descriptions to Tableau catalog using the REST API.
    """

    full_table_name = format_table_references_tableau(merged_table)
    site_id = tableau_creds["site"]["id"]
    logger.info("Publishing Tableau column descriptions for table: %s", full_table_name)

    # Merge column information so that descriptions and 
    # Tableua metadata (luid, id) are available in one dict
    tableau_dbt_column_map = defaultdict(dict)
    # Iterates through lists sequentially. First list of Tableau columns 
    # and their metadata, then dbt columns.
    for l in (tableau_columns, merged_table["columns"]):
        for elem in l:
            # For each column in both list respectively, update the defaultdict to
            # contain the metadata as its values. The first pass does this for
            # Tableau columns, the second adds the metadata for dbt to each value in the dict
            if isinstance(elem, dict):
                # First pass in the loop: {
                    # "TABLE_NAME:", {"NAME": "TABLE_NAME", "luid": "123"}, ...
                # }
                # Second pass in the loop: {
                    # "TABLE_NAME:", {
                        # "NAME": "TABLE_NAME", "luid": "123",
                        # "description": "dbt documentation"
                    # },
                # }
                tableau_dbt_column_map[elem.get("name", "").upper()].update(elem)
    # Sorts values by alphabetical order of table name
    merged_columns = sorted(tableau_dbt_column_map.values(), key=itemgetter("name"))
    success_count = 0
    failure_count = 0

    for column in merged_columns:
        try:
            # Create list of metadata fields that are required to update a column description
            required_fields = ["description", "luid", "name"]
            if not all(field in column and column[field] is not None for field in required_fields):
                logger.warning(
                    "Skipping column %s: Missing required fields", column.get('name', 'Unknown')
                )
                # Skip column if it doesn't have a description to publish
                continue

            # Clean and encode description
            description = column["description"]
            # Replace smart quotes and other problematic characters
            quote_map = {
                '\u201c': '"',  # opening curly quote
                '\u201d': '"',  # closing curly quote
                '\u2018': "'",  # opening curly apostrophe
                '\u2019': "'"   # closing curly apostrophe
            }
            description = description.translate(str.maketrans(quote_map))
            # Escape for XML
            description = html.escape(description, quote=True)

            # Construct URL ensuring no double slashes
            url = (
                f"{tableau_server}/api/{TABLEAU_API_VERSION}/sites/{site_id}"
                f"/tables/{merged_table['luid']}/columns/{column['luid']}"
            )

            # Construct payload with proper XML formatting
            payload = f"""<?xml version="1.0" encoding="UTF-8"?>
            <tsRequest>
            <column description="{description}"/>
            </tsRequest>"""

            headers = {
                "X-Tableau-Auth": tableau_creds["token"],
                "Content-Type": "application/xml",
                "Accept": "application/xml"
            }

            logger.debug("Making request to URL: %s", url)
            logger.debug("Payload: %s", payload)
            logger.debug("Headers: %s", headers)

            # Make request with explicit encoding
            response = requests.put(
                url,
                headers=headers, 
                data=payload.encode('utf-8'),
                verify=True,  # Enable SSL verification
                timeout=60
            )

            # Log the full response
            logger.debug("Response status: %s", response.status_code)
            logger.debug("Response headers: %s", response.headers)
            logger.debug("Response text: %s", response.text)

            response.raise_for_status()

            # Verify the response indicates success
            if response.status_code == 200:
                logger.info("Successfully updated column %s", column["name"])
                success_count += 1
            else:
                logger.warning(
                    "Unexpected status code %s for column %s", response.status_code, column["name"]
                )
                failure_count += 1

        except requests.exceptions.RequestException as e:
            failure_count += 1
            logger.error("Error updating column %s: %s", column.get("name", "Unknown"), str(e))

    # Log final results
    logger.info("Completed publishing descriptions for %s", full_table_name)
    logger.info("Success: %s, Failures: %s", success_count, failure_count)

    # Return counts for monitoring
    return success_count, failure_count

def publish_tableau_table_description(
    tableau_server: str,
    merged_table: dict,
    description_text: str,
    tableau_creds: dict
) -> str:
    """
    Updates a table description in Tableau's catalog via REST API with dbt documentatin description.
    Args:
        description_text: dbt docs description to update in Tableau   
    """
    url = (
        f"{tableau_server}/api/{TABLEAU_API_VERSION}/sites/"
        f"{tableau_creds['site']['id']}/tables/{merged_table['luid']}"
    )

    payload = f'<tsRequest><table description="{description_text}"></table></tsRequest>'
    headers = {
        "X-tableau-Auth": tableau_creds["token"],
        "Content-Type": "text/plain"
    }

    try:
        response = requests.put(
            url,
            headers=headers,
            data=payload,
            timeout=60
        )
        response.raise_for_status()
        if response.status_code == 200:
            logger.info("Successfully updated the description for %s", merged_table["name"])
        else:
            logger.warning(
                "Unexpected status code %s for table %s", response.status_code, merged_table["name"]
            )
        logger.info(
            "Updated description for table %s", format_table_references_tableau(merged_table)
        )

        return response.text

    except requests.exceptions.RequestException as e:
        logger.error("Failed to update table description: %s", str(e))


def verify_column_description(
        tableau_server: str,
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
            f"{tableau_server}/api/{TABLEAU_API_VERSION}/sites/"
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

#publishes tableau column tags for a given table and list of columns
# def publish_tableau_column_tags(tableau_server, tableau_columns, merged_table, tableau_creds):
#     tag = merged_table['packageName']
#     full_table_name = get_full_table_name(merged_table)
#     print('publishing tableau column tags: ' + tag + ' for table: ' + full_table_name + '...')
#     headers = {
#         'X-tableau-Auth': tableau_creds['token'],
#         'Content-Type': 'text/plain'
#     }
#     for tableau_column in tableau_columns:
#         url = tableau_server + "/api/" + TABLEAU_API_VERSION + "/sites/" + tableau_creds['site']['id'] + "/columns/" + tableau_column['id'] + "/tags"
#         payload = "<tsRequest>\n  <tags>\n <tag label=\"" + tag + "\"/>\n  </tags>\n</tsRequest>"

#         try:
#             column_tags_response = requests.request("PUT", url, headers=headers, data=payload).text
#             print(column_tags_response)
#         except Exception as e:
#             print('Error publishing tableau column tags ' + str(e))
#     #print('published tableau column tags: ' + tag + ' for table: ' + full_table_name)
#     return column_tags_response
