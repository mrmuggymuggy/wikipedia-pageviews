# -*- coding: utf-8 -*-
#
"""
This is an example dag for using the Kubernetes Executor.
"""
import os

import airflow
from airflow.models import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

DAG_NAME = "wikipedia-pageviews"
ENV = os.environ.get("ENV")

docker_image = "mrmuggymuggy/wikipedia_pageviews:5d2d671"

spark_properties = """
spark.master=local[8]
spark.driver.memory=14g
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

envs = {
    "APP_NAME": DAG_NAME,
    "SOURCE_URL_PREFIX": "https://dumps.wikimedia.org/other/pageviews",
    "BLACKLISG_URL_IN": "https://s3.amazonaws.com/dd-interview-data/data_engineer/wikipedia/blacklist_domains_and_pages",
    "PATH_OUT_PREFIX": "/mnt/out",
    "EXECUTION_DATETIME": "{{ ts }}",
    "HOURLY": "True",
    "FORCE_REPROCESS": "false",
    "PYTHON_FILE": "/workspace/jobs/wiki_pageviews_to_s3.py",
    "SPARK_PROPERTIES": spark_properties,
}

args = {
    "owner": "Airflow",
    "start_date": airflow.utils.dates.days_ago(2),
}

with DAG(dag_id=DAG_NAME, default_args=args, schedule_interval="30 0 * * *") as dag:

    # Limit resources on this operator/task with node affinity & tolerations
    spark_batch_job_kubespark = KubernetesPodOperator(
        namespace=os.environ["AIRFLOW__KUBERNETES__NAMESPACE"],
        name=DAG_NAME,
        task_id=f"{DAG_NAME}-kubespark",
        image=docker_image,
        image_pull_policy="IfNotPresent",
        env_vars=envs,
        resources={"request_memory": "4024Mi", "request_cpu": "100m"},
        is_delete_operator_pod="True",
        in_cluster="True",
        hostnetwork="False",
    )

    spark_batch_job_kubespark
