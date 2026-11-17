"""Run an unlabeled Dataflow baseline followed by ten separately labeled DAG runs."""

import re
from datetime import datetime, timedelta, timezone

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
try:
    from airflow.sdk import DAG, get_current_context, task
except ImportError:
    from airflow import DAG
    from airflow.decorators import task
    from airflow.operators.python import get_current_context
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
else:
    from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

REGION = "us-central1"
SOURCE_SYSTEMS = ("oracle", "gcs", "sap")
WORKLOAD_DAG_ID = "finops_dataflow_job"
START_DATE = datetime(2026, 1, 1, tzinfo=timezone.utc)


@task
def prepare_launch():
    dag_run = get_current_context()["dag_run"]
    config = dag_run.conf
    experiment = config["experiment_id"]
    sequence = int(config["sequence"])
    pipeline = config["pipeline_id"]
    project = config["project_id"]
    bucket = config["bucket_name"]
    version = config["template_version"]
    source_system = config["source_system"]

    if not re.fullmatch(r"[0-9]{14}", experiment) or not 0 <= sequence <= 10:
        raise ValueError("Expected a 14-digit experiment ID and sequence 0 through 10")
    if not re.fullmatch(r"[a-z][a-z0-9_-]{0,62}", pipeline):
        raise ValueError("Invalid pipeline_id")
    if not re.fullmatch(r"[a-z][a-z0-9-]{4,28}[a-z0-9]", project):
        raise ValueError("Invalid project_id")
    if not re.fullmatch(r"finops-poc-[0-9]+", bucket):
        raise ValueError("Expected the bucket created by poc.sh setup")
    if not re.fullmatch(r"latest|[0-9]{4}-[0-9]{2}-[0-9]{2}-[A-Za-z0-9_]+", version):
        raise ValueError("Invalid template version")
    if source_system not in SOURCE_SYSTEMS:
        raise ValueError("Invalid source_system")
    if len(f"{pipeline}_finops-after-{experiment}-10") > 63:
        raise ValueError("The combined pipeline_id label value exceeds 63 characters")

    phase = "before" if sequence == 0 else "after"
    run_id = dag_run.run_id
    if run_id != f"finops-{phase}-{experiment}-{sequence:02d}":
        raise ValueError("Trigger this workload through finops_dataflow_billing_experiment")

    root = f"gs://{bucket}"
    table = f"orders_dag_{phase}_{experiment}_{sequence:02d}"
    environment = {
        "numWorkers": 1,
        "maxWorkers": 1,
        "serviceAccountEmail": f"finops-dataflow@{project}.iam.gserviceaccount.com",
        "network": "finops-poc",
        "subnetwork": f"regions/{REGION}/subnetworks/finops-poc-central",
        "tempLocation": f"{root}/temp/dag/{run_id}",
    }
    if sequence:
        environment["additionalUserLabels"] = {
            "pipeline_id": f"{pipeline}_{run_id}",
            "source_system": source_system,
        }

    return {
        "launchParameter": {
            "jobName": run_id,
            "containerSpecGcsPath": f"gs://dataflow-templates/{version}/flex/GCS_Text_to_BigQuery_Flex",
            "environment": environment,
            "parameters": {
                "inputFilePattern": f"{root}/input/{source_system}.csv",
                "JSONPath": f"{root}/input/schema.json",
                "javascriptTextTransformGcsPath": f"{root}/input/transform.js",
                "javascriptTextTransformFunctionName": "transformOrder",
                "outputTable": f"{project}:finops_workload.{table}",
                "bigQueryLoadingTemporaryDirectory": f"{root}/load/dag/{run_id}",
            },
        }
    }


@task
def save_job_evidence(job, body):
    dag_run = get_current_context()["dag_run"]
    config = dag_run.conf
    experiment = config["experiment_id"]
    sequence = int(config["sequence"])
    phase = "before" if sequence == 0 else "after"
    job_id = job["id"]
    started = datetime.fromisoformat(job["createTime"].replace("Z", "+00:00"))
    ended = datetime.now(timezone.utc)
    output = body["launchParameter"]["parameters"]["outputTable"].replace(":", ".")
    client = BigQueryHook(location=REGION, use_legacy_sql=False).get_client(
        project_id=config["project_id"]
    )
    try:
        client.query(
            f"ASSERT (SELECT COUNT(*) = 5 AND COUNT(DISTINCT order_id) = 5 "
            f"AND SUM(amount_cents) = 10798 FROM `{output}`) AS 'Unexpected mock orders'"
        ).result()
    finally:
        client.delete_table(output, not_found_ok=True)

    row = (
        phase,
        config["pipeline_id"],
        dag_run.run_id,
        job_id,
        str(int(started.timestamp())),
        str(int(ended.timestamp()) + 1),
        config["source_system"],
    )
    path = f"evidence/dataflow/{experiment}/{sequence:02d}.tsv"
    GCSHook().upload(
        bucket_name=config["bucket_name"],
        object_name=path,
        data="\t".join(row) + "\n",
        mime_type="text/tab-separated-values",
    )
    return {"dataflow_job_id": job_id, "run_id": dag_run.run_id, "evidence": path}


with DAG(
    dag_id=WORKLOAD_DAG_ID,
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    render_template_as_native_obj=True,
) as workload:
    launch_body = prepare_launch()
    dataflow = DataflowStartFlexTemplateOperator(
        task_id="launch_dataflow",
        project_id="{{ dag_run.conf['project_id'] }}",
        location=REGION,
        body=launch_body,
        append_job_name=False,
        wait_until_finished=True,
        deferrable=False,
        execution_timeout=timedelta(minutes=60),
    )
    save_job_evidence(dataflow.output, launch_body)


with DAG(
    dag_id="finops_dataflow_billing_experiment",
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    params={
        "experiment_id": "",
        "project_id": "data-finops-billing-data",
        "bucket_name": "",
        "pipeline_id": "mock_orders_gcs_to_bq",
        "template_version": "latest",
    },
) as experiment:
    experiment_id = "{{ params.experiment_id }}"
    previous = None
    for sequence in range(11):
        phase = "before" if sequence == 0 else "after"
        trigger = TriggerDagRunOperator(
            task_id=f"run_{phase}_{sequence:02d}",
            trigger_dag_id=WORKLOAD_DAG_ID,
            trigger_run_id=f"finops-{phase}-{experiment_id}-{sequence:02d}",
            conf={
                "experiment_id": experiment_id,
                "sequence": sequence,
                "source_system": SOURCE_SYSTEMS[(sequence - 1) % 3] if sequence else "gcs",
                "pipeline_id": "{{ params.pipeline_id }}",
                "project_id": "{{ params.project_id }}",
                "bucket_name": "{{ params.bucket_name }}",
                "template_version": "{{ params.template_version }}",
            },
            wait_for_completion=True,
            deferrable=True,
            poke_interval=30,
            execution_timeout=timedelta(minutes=90),
        )
        if previous is not None:
            previous >> trigger
        previous = trigger
