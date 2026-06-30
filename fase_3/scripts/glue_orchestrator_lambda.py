import json
import logging
import os
import time

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue = boto3.client("glue")

JOB_INGESTION = os.environ.get("JOB_INGESTION", "fraude-ingestion-bronze")
JOB_TRANSFORM = os.environ.get("JOB_TRANSFORM", "fraude-transform-silver-gold")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", 10))
TIMEOUT = int(os.environ.get("TIMEOUT", 600))


def start_and_wait(job_name: str, timeout: int) -> dict:
    response = glue.start_job_run(JobName=job_name)
    run_id = response["JobRunId"]
    logger.info("Started %s — JobRunId: %s", job_name, run_id)

    elapsed = 0
    while elapsed < timeout:
        status = glue.get_job_run(JobName=job_name, RunId=run_id)
        state = status["JobRun"]["JobRunState"]
        logger.info("%s: %s (%ds)", job_name, state, elapsed)
        if state in ("SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT"):
            return {
                "job_name": job_name,
                "run_id": run_id,
                "state": state,
                "execution_time": status["JobRun"].get("ExecutionTime"),
                "dpu_seconds": status["JobRun"].get("DPUSeconds"),
            }
        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL

    return {
        "job_name": job_name,
        "run_id": run_id,
        "state": "TIMEOUT",
    }


def lambda_handler(event, context):
    logger.info("Event: %s", json.dumps(event))

    ingestion = start_and_wait(JOB_INGESTION, TIMEOUT)
    logger.info("Ingestion result: %s", json.dumps(ingestion))

    if ingestion["state"] != "SUCCEEDED":
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": "Ingestion failed",
                "ingestion": ingestion,
            }),
        }

    transform = start_and_wait(JOB_TRANSFORM, TIMEOUT)
    logger.info("Transform result: %s", json.dumps(transform))

    all_succeeded = transform["state"] == "SUCCEEDED"

    return {
        "statusCode": 200 if all_succeeded else 500,
        "body": json.dumps({
            "ingestion": ingestion,
            "transform": transform,
            "pipeline_status": "SUCCESS" if all_succeeded else "FAILED",
        }),
    }
