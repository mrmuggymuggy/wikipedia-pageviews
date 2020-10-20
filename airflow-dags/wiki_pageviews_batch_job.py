# -*- coding: utf-8 -*-
#
"""
Wikipedia pageview dag using the Kubernetes Executor
Hourly run schedule
"""
import os, json, yaml
from datetime import datetime, timedelta

import airflow
from airflow.models import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

DAG_NAME = "wiki_pageviews_batch_job"

docker_image = "mrmuggymuggy/wikipedia_pageviews:0.1.0"

# spark properties for the task
spark_properties = """
spark.master=local[8]
spark.driver.memory=4g
spark.driver.maxResultSize=2g
#file download and dispatch to executor time is long
spark.sql.autoBroadcastJoinThreshold=-1
spark.hadoop.fs.s3a.multiobjectdelete.enable=false
spark.hadoop.fs.s3a.fast.upload=true
spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2
spark.speculation=false
spark.serializer=org.apache.spark.serializer.KryoSerializer
#enable for local run to access s3, don't use with k8s pod(use kubeiam)
#spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain
"""

# environment variable for dag task run
# EXECUTION_DATETIME using airflow macro ts: https://airflow.apache.org/docs/stable/macros-ref
envs = {
    "APP_NAME": DAG_NAME,
    "SOURCE_URL_PREFIX": "https://dumps.wikimedia.org/other/pageviews",
    "BLACKLISG_URL_IN": "https://s3.amazonaws.com/dd-interview-data/data_engineer/wikipedia/blacklist_domains_and_pages",
    "PATH_OUT_PREFIX": "s3a://xxxxxxxxxxxxxxx/outputs",
    "EXECUTION_DATETIME": "{{ ts }}",
    "HOURLY": "True",
    "FORCE_REPROCESS": "false",
    "SPARK_PROPERTIES": spark_properties,
    "AWS_IAM_ARN": "arn:aws:iam::xxxxxxxxxxxxxxx:role/xxxxxxxxxxxxxxx",
}

datadog_autodiscovery_conf = """
init_config: {}
logs: []
instances:
  - spark_url: "http://%%host%%:4040"
    spark_cluster_mode: "spark_driver_mode"
    cluster_name: k8s-spark
"""

datadog_conf_dict = yaml.safe_load(datadog_autodiscovery_conf)

args = {
    "owner": "Airflow",
    "start_date": airflow.utils.dates.days_ago(2),
    "retries": 3,
    "retry_delay": timedelta(seconds=600),
    "annotations": {
        "iam.amazonaws.com/role": envs["AWS_IAM_ARN"],
        f"ad.datadoghq.com/{DAG_NAME}.init_configs": json.dumps(
            datadog_conf_dict["init_config"]
        ),
        f"ad.datadoghq.com/{DAG_NAME}.logs": json.dumps(datadog_conf_dict["logs"]),
        f"ad.datadoghq.com/{DAG_NAME}.check_names": '["spark"]',
        f"ad.datadoghq.com/{DAG_NAME}.check_names": json.dumps(
            datadog_conf_dict["instances"]
        ),
    },
}

with DAG(
    dag_id=DAG_NAME,
    default_args=args,
    concurrency=3,
    max_active_runs=3,
    schedule_interval="30 * * * *",
) as dag:
    spark_batch_job_kubespark = KubernetesPodOperator(
        namespace=os.environ["AIRFLOW__KUBERNETES__NAMESPACE"],
        name=DAG_NAME,
        task_id=f"{DAG_NAME}_kubespark",
        image=docker_image,
        image_pull_policy="IfNotPresent",
        service_account_name="airflow",
        env_vars=envs,
        resources={"request_memory": "4024Mi", "request_cpu": "100m"},
        is_delete_operator_pod=True,
        in_cluster=True,
        hostnetwork=False,
    )
    spark_batch_job_kubespark
