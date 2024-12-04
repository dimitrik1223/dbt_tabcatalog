import json
import yaml
import base64
import logging
from typing import List, Dict, Any, Optional
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def dbt_get_account_id(dbt_cloud_api: str, dbt_token: str) -> int:
    """Retrieve the dbt Cloud account ID using the dbt cloud API."""
    logger.info("Getting dbt Cloud account ID using from API: %s", dbt_cloud_api)

    headers = {
        "Content-Type": "appication/json",
        "Authorization": f"Token {dbt_token}"
    }
    try:
        response = requests.get(dbt_cloud_api, headers=headers)
        response_json = response.json()
        if "errors" in response_json:
            raise Exception(response_json["errors"][0]["message"])

        dbt_account_id = response_json["data"][0]["id"]
        logger.info("dbt Cloud account Id: %d", dbt_account_id)
        return dbt_account_id

    except Exception as e:
        logger.error("Error getting account id from dbt Cloud: %s", str(e))

def dbt_get_projects(dbt_account_id: str, dbt_cloud_api: str, dbt_token: str) -> List[Dict[str, Any]]:
    """Retrieve all projects for a given dbt Cloud account."""
    logger.info("Getting dbt project from account id %s", dbt_account_id)
    url = f"{dbt_cloud_api}{dbt_account_id}/projects"
    headers = {
      "Content-Type": "appication/json",
      "Authorization": f"Token {dbt_token}"
    }

    try:
        response = requests.get(url, headers=headers)
        response_json = response.json()

        if "errors" in response_json:
            raise Exception(response_json["errors"][0]["message"])

        return response_json["data"]

    except Exception as e:
        logger.error("Error getting projects from dbt Cloud: %s", str(e))
        raise

def dbt_get_jobs(
    dbt_account_id: int, 
    dbt_cloud_api: str,
    dbt_token: str,
    job_environment_ids: list
) -> List[Dict[str, Any]]:
    """
    Retrieve jobs for a given dbt Cloud account, optionally filtered by environment IDs.

    Args:
        dbt_account_id: The dbt Cloud account ID
        dbt_cloud_api: Base URL for the dbt Cloud API
        dbt_token: Authentication token for dbt Cloud
        job_environment_ids: Optional list of environment IDs to filter jobs

    Returns:
        List[Dict[str, Any]]: List of job dictionaries containing job details
    """
    logger.info("Getting dbt jobs for account id %s", dbt_account_id)
    url = f"{dbt_cloud_api}{dbt_account_id}/jobs"
    headers = {
      "Content-Type": "appication/json",
      "Authorization": f"Token {dbt_token}"
    }
    try:
        response = requests.get(url, headers=headers)
        response_json = response.json()
        
        if "errors" in response_json:
            raise Exception(response_json["errors"][0]["message"])

        all_jobs = response_json["data"]
        logger.info("Retrieved %s dbt jobs", str(len(all_jobs)))
        
        if not job_environment_ids:
            return all_jobs
        
        # Filter jobs by environment IDs
        filtered_jobs = [
            job for job in all_jobs
            if job.get("environment_id") in job_environment_ids
        ]
        return filtered_jobs

    except Exception as e:
        logger.error("Error getting jobs from dbt Cloud: %s", str(e))
        return []

def dbt_get_models_for_job(dbt_metadata_api: str, dbt_token: str, job_id: int) -> List[Dict[str, Any]]:
    """
    Retrieve all dbt models associated with a specific dbt job using the dbt Metadata API.
    """
    logger.info("Getting dbt models for job id: %s", str(job_id))

    headers = {
      "Authorization": f"Token  {dbt_token}",
      "Content-Type": "application/json"
    }

    # GraphQL query to retrieve model details
    query = """
    query {
        models(jobId: %d) {
            uniqueId
            packageName
            runId
            accountId
            projectId
            environmentId
            jobId
            executionTime
            jobId
            executionTime
            status
            executeCompletedAt
            database
            schema
            name
            description
            meta
            stats {
                id
                value
            }
            columns {
                name
                description
            }
        }
    }
    """ % job_id

    payload = {
        "query": query,
        "variables": {}
    }
    try:
        response = requests.post(
            dbt_metadata_api, 
            headers=headers, 
            json=payload,
            timeout=60
        )
        response_json = response.json()

        if "errors" in response_json:
            raise Exception(response_json["errors"][0]["message"])


        models = response_json["data"]["models"]
        logger.info("Retrieved %d dbt models for job id: %d", len(models), job_id)
        return models

    except Exception as e:
        logger.error("Error getting dbt models for job id: %s - %s", job_id, str(e))

def generate_dbt_exposures(
    downstream_workbooks: List[List[Dict[str, Any]]], 
    tableau_server: str, 
    tableau_site: str, 
    dbt_exposure_maturity: int
) -> None:
    """
    Generate dbt exposure YAML file based on downstream Tabeleau workbooks.
    """
    logger.info("Generating dbt exposures for downstream workbooks")
    
    exposures_list = []
    
    # Skip the first empty list in downstream_workbooks
    for workbook_list in downstream_workbooks[1]:
        for workbook in workbook_list:
            url = f"{tableau_server}/#/{tableau_site}/workbooks/{workbook['vizportalUrlId']}"

            # Generate deponds_on refs for upstream tables
            depends_on = [
                f"ref('{table['name'].lower()}')"
                for table in workbook["upstreamTables"]
            ]
            
            exposure = {
                "name": workbook["name"],
                "type": "dashboard",
                "maturity": dbt_exposure_maturity,
                "url": url,
                "description": workbook["description"],
                "depends_on": depends_on,
                "owner": {
                    "name": workbook["owner"]["name"],
                    "email": workbook["owner"]["username"]
                }
            }
            exposures_list.append(exposure)
    # Create final YAML structure
    exposures_yaml = {
        "version": 2,
        "exposures": exposures_list
    }

    # Write to file
    project_name = "dbt-pipelines"
    write_dbt_project_exposures_file(exposures_yaml, project_name)

def write_dbt_project_exposures_file(dict_file: Dict[str, Any], project_name: str) -> None:
    """
    Write dbt exposures to a YAML file.

    Args:
        dict_file: Dictionary containing exposures data
        project_name: Name of the dbt project
    """
    print(f"Writing dbt exposures to file for project: {project_name}...")

    try:
        filename = f".\\exposures\\{project_name}_tab_exposures.yml"
        with open(filename, "w") as file:
            yaml.dump(dict_file, file)
    except Exception as e:
       print(f"Error writing dbt exposures: {str(e)}")
