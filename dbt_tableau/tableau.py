import requests
import json
from collections import defaultdict
from operator import itemgetter
import xml.sax.saxutils as saxutils
import logging
import html
import xml.etree.ElementTree as ET
import xml.etree.ElementTree as ET # For parsing XML responses
from urllib.request import urlopen, Request

TABLEAU_API_VERSION='3.17'

def authenticate_tableau(tableau_server, tableau_site_name, tableau_token_name, tableau_token):
    url = tableau_server + "/api/" + TABLEAU_API_VERSION + "/auth/signin"
    print('authenticating with tableau server url: ' + url + '...')
    payload = json.dumps({
        "credentials": {
            "personalAccessTokenName": tableau_token_name,
            "personalAccessTokenSecret": tableau_token,
            "site": {
                "contentUrl": tableau_site_name
            }
        }
    })
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        response_json = json.loads(response.text)
        if 'error' in response_json.keys():
            raise Exception(response_json['error'])

        tableau_creds = response_json['credentials']
        print('tableau user id: ' + str(tableau_creds['user']['id']))
    except Exception as e:
        print('Error authenticating with tableau. Servername: ' + tableau_server + ' Site: ' + tableau_site_name + ' ' +  str(e))
    return tableau_creds

def tableau_get_databases(tableau_server, tableau_auth, databases = ["PRODUCTION"]):
    db_type = 'connectionType: "snowflake"'
    snowflake_database = f"{db_type}, nameWithin: {json.dumps(databases)}"
    mdapi_query = '''query get_databases {
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
        }''' % snowflake_database
    auth_headers = {'accept': 'application/json', 'content-type': 'application/json',
                                   'x-tableau-auth': tableau_auth['token']}
    try:
        metadata_query = requests.post(tableau_server + '/api/metadata/graphql', headers=auth_headers, verify=True, json={"query": mdapi_query})
        tableau_databases = json.loads(metadata_query.text)['data']['databases']
    except Exception as e:
        print('Error getting databases from tableau metadata API ' + str(e))
    print('retrieved tableau databases')
    return tableau_databases

#returns a list of merged (i.e. matched database/schema/table name) tableau database tables and dbt models
def merge_dbt_tableau_tables(tableau_database,tableau_database_tables,dbt_models):
    table_model_map = defaultdict(dict)
    for table in tableau_database_tables['tables']:
        for model in dbt_models:
            # Aliases make Snowflake
            if model['name'].lower() == table['name'].lower() and model['schema'].lower() == table['schema'].lower() and model['database'].lower() == tableau_database['name'].lower(): #if table/schema/database/hostname match
                table_model_map[model['name'].lower()].update(table)
                table_model_map[table['name'].lower()].update(model)
    merged_tables = sorted(table_model_map.values(), key=itemgetter("name"))
    print('merged ' + str(len(merged_tables)) + ' dbt models and tableau tables in tableau database: ' + tableau_database['name'])
    return merged_tables

#helper function to get full table name in the format [DATABASE].[SCHEMA].[TABLE]
def get_full_table_name(merged_table):
    full_table_name = '[' + merged_table['database'].upper() + '].[' + merged_table['schema'].upper() + '].[' + merged_table['name'].upper() + ']'
    return full_table_name

#returns a list of downstream workbooks (filter using database_type_filter and database_name_filter)
def tableau_get_downstream_workbooks(tableau_server, merged_table, tableau_creds):
    full_table_name = get_full_table_name(merged_table)
    print('getting downstream workbooks for table: ' + full_table_name + '...')
    filter = 'luid: "' + merged_table['luid'] + '"'

    mdapi_query = '''query get_downstream_workbooks {
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
    }''' % filter

    auth_headers = {'accept': 'application/json', 'content-type': 'application/json',
                                   'x-tableau-auth': tableau_creds['token']}
    try:
        metadata_query = requests.post(tableau_server + '/api/metadata/graphql', headers=auth_headers, verify=True,
                                       json={"query": mdapi_query})
        downstream_workbooks = json.loads(metadata_query.text)['data']['databaseTables'][0]['downstreamWorkbooks']
    except Exception as e:
        print('Error getting downstream workbooks from tableau metadata API ' + str(e))
    print('retrieved ' + str(len(downstream_workbooks)) + ' downstream tableau workbooks')
    return downstream_workbooks

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
    
    full_table_name = get_full_table_name(merged_table)
    logger.info(f'Publishing Tableau column descriptions for table: {full_table_name}')

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

# def verify_column_description(tableau_server, site_id, table_id, column_id, token):
#     """
#     Verifies that a column description was actually updated in Tableau.
#     Returns the current description or None if verification fails.
#     """
#     try:
#         url = f"{tableau_server.rstrip('/')}/api/{TABLEAU_API_VERSION}/sites/{site_id}/tables/{table_id}/columns/{column_id}"
#         headers = {
#             'X-Tableau-Auth': token,
#             'Accept': 'application/xml'
#         }
#         response = requests.get(url, headers=headers)
#         response.raise_for_status()
        
#         # Parse response XML to get description
#         # You'll need to implement XML parsing based on the response format
#         # Return the description if found
#         return response.text
#     except Exception as e:
#         logging.error(f"Error verifying column description: {str(e)}")
#         return None

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

