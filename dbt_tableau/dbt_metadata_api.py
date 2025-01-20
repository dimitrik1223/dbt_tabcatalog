import json
import logging
from typing import List, Dict, Any
import requests

def get_models_for_job(
    discovery_api_url: str,
    api_key: str,
    job_id: int) -> List[Dict[str, Any]]:
    """
    Retrieve all dbt models associated with a specific dbt job using the dbt Metadata API.
    """
    logging.info("Getting dbt models for job id: %s", str(job_id))
    headers = {"Content-Type": "application/json", "Authorization": f"Token {api_key}"}
    # GraphQL query to retrieve model details
    query = (
    """
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
            alias
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
    """
         % job_id
    )

    payload = {"query": query, "variables": {}}
    try:
        response = requests.post(
            discovery_api_url, headers=headers, json=payload, timeout=3600
        )
        response_json = response.json()
        models = response_json["data"]["models"]
        logging.info("Retrieved %d dbt models for job id: %d", len(models), job_id)
        return models

    except requests.exceptions.Timeout:
        logging.error("Timeout connecting to dbt Cloud API")
        raise  # Re-raise the exception
    except requests.exceptions.RequestException as e:
        logging.error("Error connecting to dbt Cloud API: %s", str(e))
        raise
    except json.JSONDecodeError as e:
        logging.error("Invalid JSON response from dbt Cloud API: %s", str(e))
        raise
