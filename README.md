# Wikipedia pageviews TOP ranks
This Project runs Spark job to compute wikipedia toprank pages for each domain

Tech stack
* Spark SQL(Spark 3.0.1) -> ETL library
* Python 3.8
* docker-compose -> for local run, unit-test and jupyter-notebook
* Airflow 1.10.10 -> for scheduling spark job run
* Kubernetes -> Airflow schedules spark task to kubernetes
* Helm -> Airflow is deployed with helm chart
* Taskfile -> A makefile like build tool

## Run the app locally
Make sure you have `docker-compose` and `task`(https://taskfile.dev/#/) installed

Application local run and unit tests are in docker, so no other dependencies needed

Local run: ``` task local.run ```

Unit-tests run: ```task unit-tests```

For daily run, make sure you have enough resource(cpu/RAM) on your machine, have a look at `docker-compose` file, the daily run require 8G memory, takes around 25 minutes to finish process

## Customize parameters and check outputs
The app read configurations from environment variables, see code `./jobs/wiki_pageviews/wiki_pageviews_config.py`

| Environment variables 	| Default                                                                                        	| Meaning                                                                                                                                  	|
|-----------------------	|------------------------------------------------------------------------------------------------	|------------------------------------------------------------------------------------------------------------------------------------------	|
| APP_NAME              	| wikipedia_pageviews                                                                            	| Spark session app name                                                                                                                   	|
| SOURCE_URL_PREFIX     	| https://dumps.wikimedia.org/other/pageviews                                                    	| Where to get wikipedia pagesview                                                                                                         	|
| BLACKLIST_URL_IN      	| https://s3.amazonaws.com/dd-interview-data/data_engineer/wikipedia/blacklist_domains_and_pages 	| List of domain page_title pairs                                                                                                          	|
| PATH_OUT_PREFIX       	| /mnt/out                                                                                       	| Top ranks output csv folder prefix Can also be a s3 location                                                                             	|
| EXECUTION_DATETIME    	| 2020-01-22T02:00:00                                                                            	| Given isoformat Datetime to process The same format as `ts` airflow macro: http://airflow.apache.org/docs/stable/macros-ref.html         	|
| HOURLY                	| true                                                                                           	| Whether to process hourly or daily, daily when hourly is false                                                                           	|
| FORCE_REPROCESS       	| false                                                                                          	| Whether to force reprocess even the given date is processed                                                                              	|
| SPARK_PROPERTIES      	| spark.master=local[8] spark.driver.memory=14g spark.driver.maxResultSize=2g                    	| Spark properties taken by the applications, for details : https://spark.apache.org/docs/latest/configuration.html#application-properties 	|

With the default local run on an given hour, the partitioned output path can be found in

```
/tmp/out/format=csv/year=xxx/month=xx/day=xx/hour=xx
```

I mounted the host `/tmp` to the docker `/mnt`, thus the result can be found on you host `/tmp`, see `docker-compose.yml` volumes section of wikipedia_pageviews service

The output support S3 target as well, you will get same partitioned output path as S3 key, Make sure that you have assumed AWS IAM role which allows you to access to s3, I mount your `"${HOME}/.aws` folder to the docker image, with `spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain` as spark property, it can write to S3
> :Note: output file is in **gzipped csv** format, read with `zcat`

## Design And architecture
it's a simple spark ETL job. I choose Spark Sql for its simplicity, Spark sql underline uses spark API, it has no impact on the app performance. All fonctional codes(Data transformation) are in SQL, it makes easier to share data transformation logics with Data analysts and Business intellegence people, it also makes data transformation logic portable. e.g if we want to simply dump pageviews data into datalake, we can still use the same SQL code to calculate top ranks

I choose to use Airflow's [kubernetes operator](https://airflow.apache.org/docs/stable/kubernetes.html) to schedule Spark tasks to process a range of dates/hours, it is simple, clear and transparent. I have considered to use kubernetes cronjob for automatical scheduled run, but this solution don't support running the app for a specified range of date in the past. With airflow dag, you can `backfill` for a specified range of date

## How did I dev this app ?
I used a jupyter notebook(https://hub.docker.com/r/jupyter/pyspark-notebook) as code scatch pad, the notebook is in `./jupyter-notebook` folder, you can see my dev process by run:
```
task jupyter
```
it starts the jupyter notebook with docker-compose and mount the notebook folder into docker

## OPEN QUESTIONS
For many domains; pageviews counts are low, thus have many ties for same ranks, it brings potentially noises to the result. e.g all pages of domain `x` has only 1 count_views, then all pages will be found in the toprank dataframe.

I use RANK() function to compute toprank, ties entries are assigned the same rank, with the next ranking(s) skipped, see ```compute_ranks()``` function in ```./jobs/wiki_pageviews/wiki_pageviews_model.py```

we might consider to use ROW_NUMBER() to reduce noise, or to use DENSE_RANK() to keep all tied ranks, see : https://codingsight.com/similarities-and-differences-among-rank-dense_rank-and-row_number-functions/ for more details

## Run for a date range with Airflow on EKS
I provide a Airflow dag in the project `airflow-dag` folder, for scheduling a date range run.

The Dag uses Airflow's [`KubernetesPodOperator`](https://airflow.apache.org/docs/stable/kubernetes.html) to schedule tasks to kubernetes, thus you need to have a working kubernetes cluster.

If you don't have airflow in your cluster, you can deploy it with ```task airflow.deploy``` this task checkout a airflow helm chart(`mrmuggymuggy/airflow`) from my personal public helm repo, you can check the helm chart source code [here](https://github.com/mrmuggymuggy/helm-charts/tree/main/helm-chart-sources/airflow), it uses a default airflow image from my docker hub [here](https://hub.docker.com/repository/docker/mrmuggymuggy/airflow).

make sure that you have :
1. Admin right on your working kubernetes namespace(where you are going to deploy Airflow), the Airflow helm chart will create a serviceaccount with admin rolebinding, it is neccessary for Airflow to launch task as kubernetes pod
2. make sure that your kubernetes cluster has [kube2iam](https://github.com/jtblin/kube2iam) deployed, the task run as kubernetes pod need access to S3
3. Bonus: if you have Datadog agent deployed in your cluster, you will have automatically spark metrics in Datadog :)

To deploy the dag, run `task dags:deploy`, it packages the dag into a zip file and copy it to the Airflow pod

to run for a date range, execute: `START_DATE=2020-01-11T02:00:00 END_DATE=2020-01-11T07:00:00 task dags:backfill`

The docker image used in the Dag can be built with `docker.build`, you can also juste leave docker image reference as it is, as I pushed the app image to my docker hub [here](https://hub.docker.com/repository/docker/mrmuggymuggy/wikipedia_pageviews)

I tested it on my personal EKS cluster, with output results to S3 for a given date range.

### Assignment Requirements
Build a simple application that we can run to compute the top 25 pages on Wikipedia for each of the Wikipedia sub-domains:
* Accept input parameters for the date and hour of data to analyze (default to the current date/hour - 24 hours if not passed, i.e. previous day/hour).
**Yes**, see Customize parameters and check outputs section
* Download the page view counts from wikipedia for the given date/hour from https://dumps.wikimedia.org/other/pageviews/ More information on the format can be found here: https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Traffic/Pageviews
**Yes** see file `./jobs/wiki_pageviews_topranks.py` function : `extract_data()`
* Eliminate any pages found in this blacklist: https://s3.amazonaws.com/dd-interview-data/data_engineer/wikipedia/blacklist_domains_and_pages
**Yes** see file `./jobs/wiki_pageviews/wiki_pageviews_model.py` function :  `data_clean()`
* Compute the top 25 articles for the given day and hour by total pageviews for each unique domain in the remaining data.
**Yes** see file `./jobs/wiki_pageviews/wiki_pageviews_model.py` function : `compute_ranks()`
* Save the results to a file, either locally or on S3, sorted by domain and number of pageviews for easy perusal.
**Yes** see file `./jobs/wiki_pageviews_topranks.py` function : `load_data()`
* Only run these steps if necessary; that is, not rerun if the work has already been done for the given day and hour.
**Yes** see file `./jobs/wiki_pageviews/utils.py` function : `pageviews_are_processed()`, if `_SUCCESS` is present end `FORCE_REPROCESS` is off, then don't rerun
* Be capable of being run for a range of dates and hours; each hour within the range should have its own result file.
it should not be managed in application code, as the resource requirements are different between hourly/daily/multiple day process, use Airflow scheduler for a date range run, see **Run for a date range with Airflow** section

For your solution, explain:
1. **Q**: What additional things would you want to operate this application in a production setting?
    * Setup monitoring base on spark metrics
    *  CI/CD pipeline for code release or rollback
    *  Tuning spark sql query plan to find more optimize query

2. **Q**: What might change about your solution if this application needed to run automatically for each hour of the day?
    * I need to adjust airflow dag to use sensor to detect the availability of new files instead of the schedule run, as the pageviews files might not be available right away
    * I will consider to output parquet files, it is more optimized for later aggregation to get daily/monthly toprank by Big query engine
3. **Q** How would you test this application?
    * unit tests, see `./jobs/tests`
    * May test airflow dag too
4. **Q** How you’d improve on this application design?
    * Would consider to install backfill UI pluggin to Airflow to not run backfill on command-line
    * Would consider to make the spark stack transparent, so business users or Data analysts with only SQL knowledge are able to schedule Spark ETL without python programming and OPS knowledge
    * (On General Data architecture level)Would consider to simply dump raw pageviews data as partitioned day/hours parquet files to S3(Datalake), use a 3rd party payed big query engine such as AWS Athena or Snowflake to compute and schedule top ranks SQL jobs.
    * (On General Data architecture level)Would consider to use a payed 3rd party cloud data plateform services such as databricks/cloudera, so no need to manage infrastructure(AWS/EKS etc) for Airfow/kubernetes deployments
