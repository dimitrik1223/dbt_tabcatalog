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

def get_tableau_columns(tableau_server, merged_table, tableau_creds):
    site_id = tableau_creds['site']['id']
    headers = {
        'X-tableau-Auth': tableau_creds['token'],
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    # GraphQL query for columns
    mdapi_query = '''
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
    '''
    
    variables = {
        "luid": merged_table['luid']
    }
    
    try:
        response = requests.post(
            f"{tableau_server}/api/metadata/graphql",
            headers=headers,
            json={"query": mdapi_query, "variables": variables}
        )
        response.raise_for_status()
        
        data = response.json()
        columns = data['data']['databaseTables'][0]['columns']
        print(f'Retrieved {len(columns)} columns for table {merged_table["name"]}')
        return columns
        
    except Exception as e:
        print(f'Error getting columns from tableau metadata API: {str(e)}')
        return []

# def get_tableau_columns(tableau_server, merged_table, tableau_creds):
#     full_table_name = get_full_table_name(merged_table)
#     print('getting columns for tableau table ' + full_table_name + '...')
#     site_id=tableau_creds['site']['id']
#     payload = ""
#     headers = {
#         'X-tableau-Auth': tableau_creds['token'],
#         'Content-Type': 'appication/json',
#         'Accept': 'application/json'
#     }
#     page_size = 100
#     page_number = 1  # 1-based, not zero based
#     total_returned = 0
#     done = False
#     tableau_columns_list = []
    
#     request_url = f"{tableau_server}/api/{TABLEAU_API_VERSION}/sites/{site_id}/tables/{merged_table['luid']}/columns"
#     paging_parameters = f'?pageSize={page_size}&pageNumber={page_number}'
#     full_url = request_url + paging_parameters
#     print(full_url)
#     try:
#         req = Request(full_url, headers=headers)
#         req = urlopen(req)
#         server_response = req.read().decode("utf8")
#         tableau_columns = json.loads(server_response)['columns']['column']
#         response_data = json.loads(server_response)
#         pagination = response_data['columns'].get('pagination', {})
#         print(pagination)
#     except Exception as e:
#         print('Error getting columns from tableau metadata API ' + str(e))
#     print('retrieved ' + str(len(tableau_columns)) + ' columns for tableau table: ' + full_table_name)
#     return tableau_columns

# def get_tableau_columns(tableau_server, merged_table, tableau_creds):
#     """Gets all columns from Tableau table using GraphQL API"""
#     full_table_name = get_full_table_name(merged_table)
#     print(f'Getting columns for tableau table {full_table_name}...')
    
#     site_id = tableau_creds['site']['id']
    
#     # Try different possible GraphQL endpoints
#     graphql_endpoints = [
#         f"{tableau_server}api/metadata/graphql",  # New metadata API endpoint
#         f"{tableau_server}api/{TABLEAU_API_VERSION}/metadata/graphql",  # Version-specific metadata endpoint
#         f"{tableau_server}metadata/graphql",  # Direct metadata endpoint
#         f"https://us-east-1.online.tableau.com/metadata/graphql"  # Hardcoded Tableau Online endpoint
#     ]
    
#     headers = {
#         'X-Tableau-Auth': tableau_creds['token'],
#         'Content-Type': 'application/json',
#         'Accept': 'application/json'
#     }
    
#     # GraphQL query for table columns
#     query = """
#     query getTableColumns($tableId: String!) {
#       tables(filter: { id: $tableId }) {
#         nodes {
#           name
#           id
#           columns {
#             totalCount
#             nodes {
#               name
#               id
#               description
#               dataType
#               upstreamColumns {
#                 totalCount
#               }
#             }
#           }
#         }
#       }
#     }
#     """
    
#     variables = {
#         "tableId": merged_table['luid']
#     }
    
#     payload = {
#         "query": query,
#         "variables": variables
#     }
    
#     for endpoint in graphql_endpoints:
#         try:
#             print(f"\nTrying endpoint: {endpoint}")
#             print("GraphQL query:", json.dumps(payload, indent=2))
            
#             response = requests.post(endpoint, headers=headers, json=payload)
#             print(f"Status code: {response.status_code}")
            
#             if response.status_code == 200:
#                 data = response.json()
#                 print("\nDEBUG: Response structure:")
#                 print(json.dumps(data, indent=2))
                
#                 if 'data' in data and 'tables' in data['data']:
#                     tables = data['data']['tables']['nodes']
#                     if tables:
#                         table = tables[0]
#                         columns = table['columns']['nodes']
#                         total_count = table['columns']['totalCount']
                        
#                         print(f"\nFound {total_count} total columns")
#                         print(f"Retrieved {len(columns)} columns")
                        
#                         # Show sample of column names
#                         print("\nSample column names:")
#                         for col in columns[:5]:
#                             print(f"- {col['name']} (ID: {col['id']})")
                        
#                         return columns
#                     else:
#                         print("No table found with the provided LUID")
#                 else:
#                     print("Unexpected response structure")
#                     continue
#             else:
#                 print(f"Error response for endpoint {endpoint}:")
#                 print(response.text)
#                 continue
                
#         except Exception as e:
#             print(f"Error with endpoint {endpoint}: {str(e)}")
#             if hasattr(e, 'response'):
#                 print(f"Response: {e.response.text}")
#             continue
    
#     print("\nFailed to get columns from any endpoint")
#     return []


def publish_tableau_column_descriptions(tableau_server, merged_table, tableau_columns, tableau_creds):
    """
    Publishes column descriptions to Tableau catalog using the REST API.
    """
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)

    # Clean up tableau_server URL
    tableau_server = tableau_server.rstrip('/')

    full_table_name = format_table_references_tableau(merged_table)
    logger.info("Publishing Tableau column descriptions for table: %s", full_table_name)

    # Merge column information
    d = defaultdict(dict)
    for l in (tableau_columns, merged_table['columns']):
        for elem in l:
            if isinstance(elem, dict):
                d[elem.get('name', '')].update(elem)
    
    merged_columns = sorted(d.values(), key=itemgetter("name"))
    success_count = 0
    failure_count = 0
    
    for column in merged_columns:
        try:
            required_fields = ['description', 'luid', 'name']
            if not all(field in column and column[field] is not None for field in required_fields):
                logger.warning(f"Skipping column {column.get('name', 'Unknown')}: Missing required fields")
                continue

            # Clean and encode description
            description = column['description']
            # Replace smart quotes and other problematic characters
            description = description.replace('"', '"').replace('"', '"').replace(''', "'").replace(''', "'")
            # Escape for XML
            description = html.escape(description, quote=True)
            
            # Construct URL ensuring no double slashes
            url = f"{tableau_server}/api/{TABLEAU_API_VERSION}/sites/{tableau_creds['site']['id']}/tables/{merged_table['luid']}/columns/{column['luid']}"
            
            # Construct payload with proper XML formatting
            payload = f"""<?xml version="1.0" encoding="UTF-8"?>
            <tsRequest>
            <column description="{description}"/>
            </tsRequest>"""

            headers = {
                'X-Tableau-Auth': tableau_creds['token'],
                'Content-Type': 'application/xml',
                'Accept': 'application/xml'
            }

            logger.debug(f"Making request to URL: {url}")
            logger.debug(f"Payload: {payload}")
            logger.debug(f"Headers: {headers}")
            
            # Make request with explicit encoding
            response = requests.put(
                url, 
                headers=headers, 
                data=payload.encode('utf-8'),
                verify=True  # Enable SSL verification
            )
            
            # Log the full response
            logger.debug(f"Response status: {response.status_code}")
            logger.debug(f"Response headers: {response.headers}")
            logger.debug(f"Response text: {response.text}")
            
            response.raise_for_status()
            
            # Verify the response indicates success
            if response.status_code == 200:
                logger.info(f"Successfully updated column {column['name']}")
                success_count += 1
            else:
                logger.warning(f"Unexpected status code {response.status_code} for column {column['name']}")
                failure_count += 1
                
        except requests.exceptions.RequestException as e:
            failure_count += 1
            logger.error(f"Error updating column {column.get('name', 'Unknown')}: {str(e)}")
            if hasattr(e.response, 'text'):
                logger.error(f"Response text: {e.response.text}")
                
        except Exception as e:
            failure_count += 1
            logger.error(f"Unexpected error updating column {column.get('name', 'Unknown')}: {str(e)}")
            
    # Log final results
    logger.info(f"Completed publishing descriptions for {full_table_name}")
    logger.info(f"Success: {success_count}, Failures: {failure_count}")
    
    # Return counts for monitoring
    return success_count, failure_count

#publishes tableau column tags for a given table and list of columns
def publish_tableau_column_tags(tableau_server, tableau_columns, merged_table, tableau_creds):
    tag = merged_table['packageName']
    full_table_name = get_full_table_name(merged_table)
    print('publishing tableau column tags: ' + tag + ' for table: ' + full_table_name + '...')
    headers = {
        'X-tableau-Auth': tableau_creds['token'],
        'Content-Type': 'text/plain'
    }
    for tableau_column in tableau_columns:
        url = tableau_server + "/api/" + TABLEAU_API_VERSION + "/sites/" + tableau_creds['site']['id'] + "/columns/" + tableau_column['id'] + "/tags"
        payload = "<tsRequest>\n  <tags>\n <tag label=\"" + tag + "\"/>\n  </tags>\n</tsRequest>"

        try:
            column_tags_response = requests.request("PUT", url, headers=headers, data=payload).text
            print(column_tags_response)
        except Exception as e:
            print('Error publishing tableau column tags ' + str(e))
    #print('published tableau column tags: ' + tag + ' for table: ' + full_table_name)
    return column_tags_response

#publishes tableau table description for a given table
def publish_tableau_table_description(tableau_server: str, merged_table: dict, description_text: str, tableau_creds: dict) -> str:
   """
   Updates a table description in Tableau's catalog via REST API.

   Args:
       tableau_server: Base URL of Tableau server
       merged_table: Dictionary containing table metadata including LUID 
       description_text: New description to set
       tableau_creds: Dictionary with authentication details (site ID and token)

   Returns:
       Response from Tableau API
       
   Raises:
       Exceptions from failed API requests are logged but not re-raised
   """
   logging.basicConfig(level=logging.DEBUG)
   logger = logging.getLogger(__name__)
   
   url = f"{tableau_server.rstrip('/')}/api/{TABLEAU_API_VERSION}/sites/{tableau_creds['site']['id']}/tables/{merged_table['luid']}"
   
   payload = f'<tsRequest><table description="{description_text}"></table></tsRequest>'
   headers = {
       'X-tableau-Auth': tableau_creds['token'],
       'Content-Type': 'text/plain'
   }

   try:
       response = requests.put(url, headers=headers, data=payload)
       response.raise_for_status()
       if response.status_code == 200:
           logger.info(f"Successfully updated the description for {merged_table['name']}")
       else:
           logger.warning(f"Unexpected status code {response.status_code} for table {merged_table['name']}")
       logger.info(f"Updated description for table {get_full_table_name(merged_table)}")
       return response.text
   except Exception as e:
       logger.error(f"Failed to update table description: {str(e)}")
       return str(e)

def verify_column_description(tableau_server, site_id, table_id, column_id, token):
    """
    Verifies that a column description was actually updated in Tableau.
    Returns the current description or None if verification fails.
    """
    try:
        url = f"{tableau_server.rstrip('/')}/api/{TABLEAU_API_VERSION}/sites/{site_id}/tables/{table_id}/columns/{column_id}"
        headers = {
            'X-Tableau-Auth': token,
            'Accept': 'application/xml'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # Parse response XML to get description
        # You'll need to implement XML parsing based on the response format
        # Return the description if found
        print(response.text)
    except Exception as e:
        logging.error(f"Error verifying column description: {str(e)}")
        return None

# def publish_tableau_column_descriptions(tableau_server, merged_table, tableau_columns, tableau_creds):
#     full_table_name = get_full_table_name(merged_table)
#     site_id = tableau_creds['site']['id']
    
#     print(f'Publishing Tableau column descriptions for table: {full_table_name}...')
    
#     # Create mapping of column names to descriptions from merged_table
#     dbt_descriptions = {}
#     print("\nDEBUG: Building column descriptions...")
#     for col in merged_table['columns']:
#         name = col['name'].upper()
#         description = col.get('description')
#         if description is not None and description.strip() != '':
#             dbt_descriptions[name] = description
    
#     updated_count = 0
#     skipped_count = 0
    
#     for column in tableau_columns:
#         column_name = column['name'].upper()
        
#         description = dbt_descriptions.get(column_name)
#         if not description:
#             print(f"Skipping column {column_name}: No description found")
#             skipped_count += 1
#             continue
            
#         # Clean up the description
#         description = description.strip().strip('"')
        
#         # Clean and escape the description for XML
#         description = (description
#             .replace('&', '&amp;')  # Must be first
#             .replace('"', '&quot;')
#             .replace("'", '&apos;')
#             .replace(''', '&apos;')  # Handle smart quotes
#             .replace(''', '&apos;')  # Handle other smart quotes
#             .replace('"', '&quot;')  # Handle smart double quotes
#             .replace('"', '&quot;')  # Handle other smart double quotes
#             .replace("<", '&lt;')
#             .replace(">", '&gt;')
#             .replace("\n", " ")
#         )
        
#         url = f"{tableau_server}api/{TABLEAU_API_VERSION}/sites/{site_id}/tables/{merged_table['luid']}/columns/{column['id']}"
#         print(url)
#         # Create XML payload with explicit UTF-8 encoding
#         payload = f"""<?xml version="1.0" encoding="UTF-8"?>
#         <tsRequest>
#         <column description="{description}"/>
#         </tsRequest>"""
        
#         headers = {
#             'X-Tableau-Auth': tableau_creds['token'],
#             'Content-Type': 'application/xml; charset=utf-8'  # Specify UTF-8 encoding
#         }
        
#         try:
#             print(f"\nProcessing column: {column_name}")
#             print(f"Raw description: {description}")
            
#             # Encode payload as UTF-8 bytes
#             payload_bytes = payload.encode('utf-8')
            
#             response = requests.put(url, headers=headers, data=payload_bytes)
#             print(f"Status Code: {response.status_code}")
            
#             if response.status_code == 200:
#                 print(f"Successfully updated description for column: {column_name}")
#                 updated_count += 1
#             else:
#                 print(f"Failed to update description for column: {column_name}")
#                 print(f"Error: Status code {response.status_code}")
#                 print(f"Response: {response.text}")
#                 skipped_count += 1
                
#         except Exception as e:
#             print(f"Error publishing description for column {column_name}: {str(e)}")
#             if hasattr(e, 'response'):
#                 print(f"Response: {e.response.text}")
#             skipped_count += 1
    
#     print(f'\nSummary:')
#     print(f'Total columns: {len(tableau_columns)}')
#     print(f'Successfully updated: {updated_count}')
#     print(f'Skipped/Failed: {skipped_count}')
#     print(f'Finished processing column descriptions for table {full_table_name}')
#     return

