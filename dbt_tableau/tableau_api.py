import requests
import json
from collections import defaultdict
from operator import itemgetter

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
    d = defaultdict(dict)
    m = defaultdict(dict)
    for table in tableau_database_tables['tables']:
        d[table['name'].lower()].update(table)
        for model in dbt_models:
            # Aliases make Snowflake
            if model['name'].lower() == table['name'].lower() and model['schema'].lower() == table['schema'].lower() and model['database'].lower() == tableau_database['name'].lower(): #if table/schema/database/hostname match
                m[model['name'].lower()].update(table)
                m[table['name'].lower()].update(model)
    merged_tables = sorted(m.values(), key=itemgetter("name"))
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
