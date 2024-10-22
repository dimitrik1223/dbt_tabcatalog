import requests
import json
import yaml
from yaml.loader import SafeLoader
from itertools import groupby
import base64

def dbt_get_account_id(dbt_cloud_api, dbt_token):
    print('getting dbt Cloud account id from dbt Cloud API: ' + dbt_cloud_api + '...')
    url = dbt_cloud_api
    payload = {}
    headers = {
        'Content-Type': 'appication/json',
        'Authorization': 'Token ' + dbt_token
    }
    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        response_json = json.loads(response.text)
        if 'errors' in response_json.keys():
            raise Exception(response_json['errors'][0]['message'])

        dbt_account_id = response_json['data'][0]['id']
        print('dbt Cloud account Id: ' + str(dbt_account_id))
    except Exception as e:
        print('Error getting account id from dbt Cloud: ' + str(e))
    return dbt_account_id

def dbt_get_projects(dbt_account_id, dbt_cloud_api, dbt_token):
    print('getting dbt projects for account id ' + str(dbt_account_id) + '...')
    url = dbt_cloud_api + str(dbt_account_id) +"/projects"
    payload={}
    headers = {
      'Content-Type': 'appication/json',
      'Authorization': 'Token '+ dbt_token
    }
    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        response_json = json.loads(response.text)
        if 'errors' in response_json.keys():
            raise Exception(response_json['errors'][0]['message'])

        dbt_projects = response_json['data']
    except Exception as e:
        print('Error getting projects from dbt Cloud: ' + str(e))
    return dbt_projects

def dbt_get_jobs(dbt_account_id, dbt_cloud_api, dbt_token, job_environment_ids: list):
    print('getting dbt jobs for account id ' + str(dbt_account_id) + '...')
    url = dbt_cloud_api + str(dbt_account_id) +"/jobs"
    payload={}
    headers = {
      'Content-Type': 'appication/json',
      'Authorization': 'Token '+ dbt_token
    }
    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        response_json = json.loads(response.text)
        if 'errors' in response_json.keys():
            raise Exception(response_json['errors'][0]['message'])

        dbt_jobs = response_json['data']
        print('retrieved: ' + str(len(dbt_jobs)) + ' dbt jobs')
    except Exception as e:
        print('Error getting jobs from dbt Cloud: ' + str(e))
    jobs_in_environment = []
    if job_environment_ids:
        for job in dbt_jobs:
            if job.get("environment_id") in job_environment_ids:
                jobs_in_environment.append(job)
    return jobs_in_environment

def dbt_get_models_for_job(dbt_metadata_api, dbt_token, job_id):
    print('getting dbt models for jobId: ' + str(job_id) + '...')
    url = dbt_metadata_api
    dbt_models=[]
    payload = '{\"query\":\"{\\n  models(jobId: ' + str(job_id) + ') {\\n    uniqueId\\n    packageName\\n    runId\\n    accountId\\n    projectId\\n    environmentId\\n    jobId\\n    executionTime\\n    status\\n    executeCompletedAt\\n    database\\n    schema\\n\\n   name\\n\\n  description\\n\\n meta\\n\\n  stats {\\n        id\\n        value\\n    }\\n\\n   columns {\\n        name\\n        description\\n    }\\n\\n  }\\n}\",\"variables\":{}}'
    headers = {
      'Authorization': 'Token ' + dbt_token,
      'Content-Type': 'application/json'
    }
    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        response_json = json.loads(response.text)
        if 'errors' in response_json.keys():
            raise Exception(response_json['errors'][0]['message'])
        dbt_models = response_json['data']['models']
        print('retreived ' + str(len(dbt_models)) + ' dbt models for jobId: ' + str(job_id))
    except Exception as e:
        print('Error getting dbt models for job id: ' + str(job_id) + ' ' + str(e))
    return dbt_models

def parse_fully_qualified_relation_name(models_api_response, model_names: list):
    # Iterate through models in JSON list returned by discovery API
    fully_qualified_relation_names = []
    model_full_relation_map = dict()
    for model in models_api_response:
        if model.get("uniqueId").split(".")[2] in model_names:
            relation = f'{model["database"].upper()}.{model["schema"].upper()}.{model["name"].upper()}'
            fully_qualified_relation_names.append(relation)
    if len(fully_qualified_relation_names) > 0:
        for model in model_names:
            model_full_relation_map[model] = next(
                (rel for rel in fully_qualified_relation_names if model.upper() in rel.split('.')),
                None
            )
            return model_full_relation_map
    return fully_qualified_relation_names


def write_exposures_file(dbt_account_id, dbt_token, dict_file, project_id):
    headers = {
        'Content-Type': 'appication/json',
        'Authorization': 'Token ' + dbt_token
    }
    url = 'https://cloud.getdbt.com/api/v3/accounts/' + str(dbt_account_id) + '/projects/' + str(project_id)

    try:
        response = requests.request("get", url, headers=headers)
        response_json=json.loads(response.text)
        repository = response_json['data']['repository']

        # #update github repo
        # headers = {
        #     'Content-Type': 'application/json',
        #     'Authorization': 'Bearer ' + github_token
        # }
        filename = 'models/tab_exposures.yml'
        # git_url = 'https://api.github.com/repos/' + repository['full_name'] + '/contents/' + filename
        message_bytes = yaml.dump(dict_file).encode('utf-8')
        base64_bytes = base64.b64encode(message_bytes)

        # print('uploading dbt exposures file to github repo ' + git_url)
        payload = json.loads('{"message": "auto generated by tableau dbt integration", "content":"' + base64_bytes.decode('utf-8') + '"}')

        # #check if exposures file already exists
        # response = requests.request("get", git_url, headers=headers)

        #if exposures file exists in github then update headers to includ sha
        # if response.status_code == 200:
        #     response_json = json.loads(response.text)
        #     sha = response_json['sha']
        #     payload['sha'] = sha

        response = requests.put(git_url, headers=headers, data=json.dumps(payload))
        print(response.text)

    except Exception as e:
        print(e)
    return

def write_dbt_project_exposures_file(dict_file, project_name):
    print('writing dbt exposures to file for project: ' + project_name + '...')
    try:
        filename = '.\\exposures\\' + project_name + '_tab_exposures.yml'
        with open(filename, 'w') as file:
            documents = yaml.dump(dict_file, file)
    except Exception as e:
        print('Error writing dbt exposures ' + str(e))
    return

def generate_dbt_exposures(downstream_workbooks, tableau_server, tableau_site, dbt_exposure_maturity):
    print('generating dbt exposures for downstream workbooks...')
    exposures_list = []
    for i, workbook in enumerate(downstream_workbooks):
        if i != 0:
            for exposure in workbook:
                url=tableau_server + '/#/site/' + tableau_site + '/workbooks/'+ exposure['vizportalUrlId']
                workbook_name = exposure['name']
                description = exposure['description']
                owner = exposure['owner']['name']
                owner_username = exposure['owner']['username']
                depends_on = []
                for upstreamTable in exposure['upstreamTables']:
                    # if upstreamTable['schema'] == 'core_ng':
                    #     depends_on.append("ref(core_'"+upstreamTable['name'].lower()+"')")
                    # elif upstreamTable['schema'] == 'common':
                    #     depends_on.append("ref(common_'"+upstreamTable['name'].lower()+"')")
                    depends_on.append("ref('"+upstreamTable['name'].lower()+"')")

                exposures_list.append({'name': workbook_name,'type':'dashboard','maturity': dbt_exposure_maturity,'url': url,'description': description,'depends_on': depends_on,'owner':{'name':owner,'email':owner_username}})

    dict_file = {'version': 2,'exposures': exposures_list}
    write_dbt_project_exposures_file(dict_file, 'dbt-pipelines')
        #write_github_exposures_file(dbt_account_id, dbt_cloud_api, dbt_token, github_token, dict_file, str(project[0]['dbt_projectId']))
    return
