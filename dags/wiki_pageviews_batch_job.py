# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
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

envs = {
    "SERVICE_NAME": DAG_NAME,
    "PYTHON_FILE": "/workspace/jobs/wiki_pageviews_to_s3.py",
    "SPARK_MODE": "local",
    "SERVICE_NAME": DAG_NAME,
    "APP_NAME": DAG_NAME,
    "SOURCE_URL_PREFIX": "https://dumps.wikimedia.org/other/pageviews",
    "BLACKLISG_URL_IN": "https://s3.amazonaws.com/dd-interview-data/data_engineer/wikipedia/blacklist_domains_and_pages",
    "PATH_OUT": "/tmp/out",
    "EXECUTION_DATETIME": "{{ ts }}",
    "HOURLY": "True",
    "PYTHON_FILE": "/workspace/jobs/wiki_pageviews_to_s3.py",
}

args = {
    "owner": "Airflow",
    "start_date": airflow.utils.dates.days_ago(2),
}

with DAG(dag_id=DAG_NAME, default_args=args, schedule_interval="30 0 * * *") as dag:

    # Limit resources on this operator/task with node affinity & tolerations
    spark_batch_job_local_mode = KubernetesPodOperator(
        namespace=os.environ["AIRFLOW__KUBERNETES__NAMESPACE"],
        name=DAG_NAME,
        task_id=f"{DAG_NAME}_local_mode",
        image=docker_image,
        image_pull_policy="IfNotPresent",
        env_vars=envs,
        resources={"request_memory": "4024Mi", "request_cpu": "100m"},
        is_delete_operator_pod="True",
        in_cluster="True",
        hostnetwork="False",
    )

    spark_batch_job_local_mode
