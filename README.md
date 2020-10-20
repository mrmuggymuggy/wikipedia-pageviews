# Wikipedia pageviews TOP ranks
This Project runs Spark job to compute wikipedia toprank pages for each domain

Tech stack
* Spark SQL(Spark 3.0.1) -> ETL library
* Python 3.8
* docker-compose 1.27.0 -> for local run, unit-test and jupyter-notebook
* Airflow 1.10.10 -> for scheduling spark job run
* Kubernetes -> Airflow schedules spark task to kubernetes
* Helm 3 -> Airflow is deployed with helm chart
* Taskfile -> A makefile alike build tool

## Run the app locally
Make sure you have `docker-compose` and `task`(https://taskfile.dev/#/) installed

Application local run and unit tests are in docker, so no other dependencies needed

Local run: ``` task local.run ```

Unit-tests run: ```task unit-tests```

For daily run, make sure you have enough resource(cpu/RAM) on your machine, have a look at `docker-compose` file, and **Customize parameters** section, the daily run require 8G memory, takes around 25 minutes to finish process

## Customize parameters, outputs path and format
The app read configurations from environment variables, see code in `./jobs/wiki_pageviews/wiki_pageviews_config.py`

| Environment variables 	| Default                                                                                        	| Meaning                                                                                                                                  	|
|-----------------------	|------------------------------------------------------------------------------------------------	|------------------------------------------------------------------------------------------------------------------------------------------	|
| APP_NAME              	| wikipedia_pageviews                                                                            	| Spark session app name                                                                                                                   	|
| SOURCE_URL_PREFIX     	| https://dumps.wikimedia.org/other/pageviews                                                    	| Where to get wikipedia pagesviews                                                                                                        	|
| BLACKLIST_URL_IN      	| https://s3.amazonaws.com/dd-interview-data/data_engineer/wikipedia/blacklist_domains_and_pages 	| List of blacklisted domain page_title pairs                                                                                              	|
| PATH_OUT_PREFIX       	| /mnt/out                                                                                       	| Top ranks output folder prefix<br> Can also be a S3 location                                                                             	|
| EXECUTION_DATETIME    	| 2020-01-22T02:00:00                                                                            	| Given isoformat Datetime to process The same format as <br>`ts` airflow macro: http://airflow.apache.org/docs/stable/macros-ref.html         	|
| HOURLY                	| true                                                                                           	| Whether to process hourly or daily, daily when hourly is false                                                                           	|
| FORCE_REPROCESS       	| false                                                                                          	| Whether to force reprocess even the given date is processed                                                                              	|
| SPARK_PROPERTIES      	| spark.master=local[8]<br>spark.driver.memory=14g<br>spark.driver.maxResultSize=2g                    	| Spark properties used by the applications, for details:<br> https://spark.apache.org/docs/latest/configuration.html#application-properties 	|

With the default local run(write to local file system) on an given hour, the partitioned output path is in:

```
/tmp/out/format=csv/year=xxx/month=xx/day=xx/hour=xx
```

I mount the host `/tmp` to the docker `/mnt`, thus the result can be found on you host `/tmp`, see `docker-compose.yml` volumes, `wikipedia_pageviews` service section for details.

The output support S3 target as well, you will get same partitioned output path as S3 key, Make sure that you have assumed AWS IAM role on your local machine which allows to access to s3, I mount `"${HOME}/.aws` folder to the docker image, with spark property : `spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain`, it can write to S3
> :Note: output file is in **gzipped csv** format, read with `zcat`

## Design And architecture
it's a simple spark ETL job. I choose Spark SQL for its simplicity, Spark SQL underline uses spark API, it has no impact on the app performance. All fonctional codes(Data transformation) are in SQL, it makes easier to share data transformation logic with data analysts and business intelligence people(they might have no programming knowledge), it also makes data transformation logic portable. e.g if we want to simply dump pageviews data into datalake as parquet files, we can still use the same SQL code to calculate top ranks.

I choose to use Airflow's [kubernetes operator](https://airflow.apache.org/docs/stable/kubernetes.html) to schedule Spark tasks to process a range of dates/hours, it is simple, clear and transparent. I have considered to use kubernetes cronjob for automatical schedule run, but this solution don't support running the app for a specified range of date in the past. With an Airflow dag, you can `backfill` for a specified range of date.

## How did I dev this app ?
I used a [jupyter notebook image](https://hub.docker.com/r/jupyter/pyspark-notebook) with pyspark support as code scatch pad, the notebook is in `./jupyter-notebook` folder, you can see my dev process by run:
```
task jupyter
```
it starts the jupyter notebook with docker-compose and mount the notebook folder into docker

## OPEN QUESTIONS
For many domains, the pageviews counts are low, thus have many ties for the same ranks, it brings potentially noises to the end result. e.g All pages of domain `x` have only 1 view counts, then all pages will be found in the toprank dataframe.

I use SQL `RANK()` function to compute toprank, ties entries are assigned to the same rank, with the next ranking(s) skipped, see ```compute_ranks()``` function in ```./jobs/wiki_pageviews/wiki_pageviews_model.py```

we might consider to use `ROW_NUMBER()` to reduce noise, or to use `DENSE_RANK()` to keep all tied ranks, see : https://codingsight.com/similarities-and-differences-among-rank-dense_rank-and-row_number-functions/ for more details

## Run for a date range locally
Execute task with a specified data range as :

`START_DATE=2020-01-22T02:00 END_DATE=2020-01-22T03:00 task local.daterange.run`

it simply loops over a time range and execute docker-compose run sequentially, if you get a `500` error on fetching data then you are done, it doesn't support retries and concurrency runs, for more sophisticated run, See next section: **Run for a date range with Airflow on EKS**.

## Run for a date range with Airflow on Kubernetes
I provide a Airflow dag in the project `./airflow-dag` folder, for scheduling a date range run with Airflow.

The Dag uses Airflow's [`KubernetesPodOperator`](https://airflow.apache.org/docs/stable/kubernetes.html) to schedule tasks to kubernetes, thus you need to have a working kubernetes cluster.

If you don't have Airflow in your cluster, you can deploy it with ```task airflow.deploy```, this task checkout a airflow helm chart(`mrmuggymuggy/airflow`) from my personal public helm repo, you can check the helm chart source code [here](https://github.com/mrmuggymuggy/helm-charts/tree/main/helm-chart-sources/airflow), it uses by default an Airflow image with Kubernetesoperator support from my docker hub [here](https://hub.docker.com/repository/docker/mrmuggymuggy/airflow).

make sure that you have :
1. Admin right on your working kubernetes namespace(where you are going to deploy Airflow), the Airflow helm chart will create a serviceaccount with admin rolebinding, it is needed for Airflow to launch task in kubernetes pod
2. Make sure that your kubernetes cluster has [kube2iam](https://github.com/jtblin/kube2iam) deployed, the task run as kubernetes task need access to S3
3. Bonus: if you have Datadog daemon-set deployed in your cluster, you will have automatically spark metrics in Datadog :) see `airflow-dags/wiki_pageviews_batch_job.py` `datadog_autodiscovery_conf`
4. In the Dag change AWS_IAM_ARN and PATH_OUT_PREFIX variables to your own IAM role and S3 bucket

To deploy the dag, run `task dags:deploy`, it packages the dag into a zip file and copy it to the Airflow pod

To run for a date range, execute: `START_DATE=2020-01-11T02:00:00 END_DATE=2020-01-11T07:00:00 task dags:backfill`

The docker image used in the Dag can be built with `task docker.build`, you can also juste leave docker image reference as it is, as I pushed the app image to my docker hub [here](https://hub.docker.com/repository/docker/mrmuggymuggy/wikipedia_pageviews)

I tested it on a AWS EKS cluster, with output results to S3 for a given date range.

### Assignment Requirements
Build a simple application that we can run to compute the top 25 pages on Wikipedia for each of the Wikipedia sub-domains:
* Accept input parameters for the date and hour of data to analyze (default to the current date/hour - 24 hours if not passed, i.e. previous day/hour).

	**Yes**, see **Customize parameters** section
* Download the page view counts from wikipedia for the given date/hour from https://dumps.wikimedia.org/other/pageviews/ More information on the format can be found here: https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Traffic/Pageviews

	**Yes**, see file `./jobs/wiki_pageviews_topranks.py` function : `extract_data()`
* Eliminate any pages found in this blacklist: https://s3.amazonaws.com/dd-interview-data/data_engineer/wikipedia/blacklist_domains_and_pages

	**Yes**, see file `./jobs/wiki_pageviews/wiki_pageviews_model.py` function :  `data_clean()`
* Compute the top 25 articles for the given day and hour by total pageviews for each unique domain in the remaining data.

	**Yes**, see file `./jobs/wiki_pageviews/wiki_pageviews_model.py` function : `compute_ranks()`
* Save the results to a file, either locally or on S3, sorted by domain and number of pageviews for easy perusal.

	**Yes**, see file `./jobs/wiki_pageviews_topranks.py` function : `load_data()`, I save it in a gziped csv format.
* Only run these steps if necessary; that is, not rerun if the work has already been done for the given day and hour.

	**Yes**, see file `./jobs/wiki_pageviews/utils.py` function : `pageviews_are_processed()`, if a `_SUCCESS` file in the output path is present and `FORCE_REPROCESS` turned to false, then don't rerun.
* Be capable of being run for a range of dates and hours; each hour within the range should have its own result file.

	**Yes**, See **Run for a date range locally** and **Run for a date range with Airflow** sections, it is not managed in the application code, as the resource requirements are different between hourly/daily/multiple days process.

For your solution, explain:
1. **Q**: What additional things would you want to operate this application in a production setting?
    * Setup monitoring base on spark metrics.
    *  CI/CD pipeline for code release and rollback.
    *  Tuning Spark SQL query plan to find more optimize query.

2. **Q**: What might change about your solution if this application needed to run automatically for each hour of the day?
    * I need to adjust Airflow dag to use sensor to detect the availability of the new pageview files instead of the schedule run, as the pageviews files might be released with hours delays.
    * I will consider to output Parquet files instead of CSV files, it is more optimal for later aggregation to get daily/monthly/yearly page topranks or other analysis by a Big query engine.
3. **Q**: How would you test this application?
    * unit tests, see `./jobs/tests`
    * May test airflow dag too
4. **Q**: How you’d improve on this application design?
    * Would consider to install backfill UI pluggin to Airflow to avoid to run backfill on command line
    * Would consider to make the Spark stack transparent, so business users or data analysts with only SQL knowledge are able to schedule Spark ETL without python programming and OPS knowledge.
    * (On General Data architecture level)Would consider to simply dump raw pageviews data as day/hours partitioned parquet files to S3(Datalake), use a 3rd party payed big query engine such as AWS Athena or Snowflake to compute and schedule SQL idiom top ranks jobs.
    * (On General Data architecture level)Would consider to use a payed 3rd party cloud data plateform services such as Databricks/Cloudera, so no need to manage infrastructure(AWS/EKS etc) for kubernetes/Airfow maintenance.

### My words
This project is in my private Github repository, don't worry that the code will be seen by other candidates.

The roject architecture/code are adapted from my two other projects:

[k8s-spark-example](https://github.com/flix-tech/k8s-spark-example)

And

[k8s-airflow](https://github.com/flix-tech/k8s-airflow)

You can have look at my (personal tech blog)[https://mrmuggymuggy.github.io/] to have some ideas about what I am doing.
