import logging
import pandas as pd
from sqlalchemy import exc
from .utils import dbt_cloud_api_request

logger = logging.getLogger(__name__)

DBT_JOB_RUN_KEYS = [
    "id",
    "trigger_id",
    "environment_id",
    "project_id",
    "status",
    "dbt_version",
    "git_branch",
    "git_sha",
    "status_message",
    "owner_thread_id",
    "executed_by_thread_id",
    "deferring_run_id",
    "artifacts_saved",
    "artifact_s3_path",
    "has_docs_generated",
    "has_sources_generated",
    "notifications_sent",
    "blocked_by",
    "created_at",
    "updated_at",
    "dequeued_at",
    "started_at",
    "finished_at",
    "last_checked_at",
    "last_heartbeat_at",
    "should_start_at",
    "status_humanized",
    "in_progress",
    "is_complete",
    "is_success",
    "is_error",
    "is_cancelled",
    "href",
    "duration",
    "queued_duration",
    "run_duration",
    "job_id",
    "is_running",
]


def get_dbt_jobs(account_id):
    """return dbt cloud jobs"""

    dbt_jobs = dbt_cloud_api_request(f"/accounts/{account_id}/jobs").get("data")
    return dbt_jobs


def get_dbt_job_runs(account_id, job_id, limit=10, offset=0):
    """
    return dbt cloud job runs; order by created_at desc

    Args:
    limit(int): number of runs to fetch
    offset(int): specifies the number of runs to skip before fetching runs set in limit

    """

    params = {
        "job_definition_id": job_id,
        "order_by": "-created_at",
        "limit": limit,
        "offset": offset,
    }
    runs = dbt_cloud_api_request(f"/accounts/{account_id}/runs", params=params).get("data")
    return runs


def get_dbt_jobs_filtered(jobs, filters):
    """keep jobs that are relevant for dbt monitoring"""

    job_list = pd.DataFrame([{"id": job["id"], "job_name": job["name"]} for job in jobs])
    job_list = job_list[job_list["id"].isin(filters)].reset_index(drop=True)
    return job_list


def snowflake_get_last_run_ids(connector, snowflake_table):
    """fetch last_run_id for each job_id in snowflake"""

    query = f"select max(runid), job_id \
        from {snowflake_table} \
        group by job_id"

    try:
        last_runs = connector.execute(query).fetchall()
        last_run_ids = pd.DataFrame(
            [{"last_run_id": run[0], "job_id": run[1]} for run in last_runs]
        )

    except exc.ProgrammingError:
        last_run_ids = pd.DataFrame(columns=["last_run_id", "job_id"])

    logger.info("Last run ids: %s", last_run_ids)
    return last_run_ids


def get_dbt_job_runs_after_last_id(account_id, job_id, last_run_id_sf):
    """fetch data for a given job_id if run_id is newer than last_run_id in snowflake"""

    job_runs = []
    offset = 0
    limit = 50
    while True:
        runs = get_dbt_job_runs(account_id, job_id, limit=50, offset=offset)
        for run in runs:
            if run.get("id") > last_run_id_sf:
                job_runs.append(run)
            else:
                return job_runs
        offset += limit


def get_all_dbt_job_runs(connector, snowflake_table, account_id, jobs):
    """
    function is to loop all job_ids (that we'd like to monitor) to get data on job_runs:
    - if job_id is in snowflake db, only fetch latest data on job_runs
    - if not, do a full fetch on job_runs

    """

    sf = snowflake_get_last_run_ids(connector, snowflake_table)
    job_runs = []
    for job_id in jobs["id"]:
        last_run_id = None
        if not sf.empty:
            # get last_run_id in snowflake if table not empty
            last_run_id_sf = sf.loc[sf["job_id"] == job_id]["last_run_id"]
            if len(last_run_id_sf) != 0:
                last_run_id = last_run_id_sf.item()
        # incremental fetch if a given job already exists in snowflake
        if last_run_id is not None:
            incremental_fetch = get_dbt_job_runs_after_last_id(account_id, job_id, last_run_id)
            job_runs.extend(incremental_fetch)
        # full new import if job doesn't exist in snowflake
        else:
            full_fetch = get_dbt_job_runs(account_id, job_id, limit=2000, offset=0)
            job_runs.extend(full_fetch)

    runs = [{key: run[key] for key in DBT_JOB_RUN_KEYS} for run in job_runs]
    runs = pd.DataFrame(runs, columns=DBT_JOB_RUN_KEYS).rename(columns={"id": "runid"})
    logger.info("Runs: %s", runs)
    return runs


def get_id_for_all_dbt_job_runs(runs) -> pd.DataFrame:
    job_run_ids = runs[["job_id", "runid"]]

    return job_run_ids
