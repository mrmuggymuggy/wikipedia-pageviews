# Spark on Kubernetes - example app

This is an example project running a Spark app on Kubernetes. It runs a PySpark job as spark driver deployment on Kubernetes. More information [here](https://spark.apache.org/docs/latest/running-on-kubernetes.html).

This example contains two spark deployment mode:
* local mode(helm-values/k8s-spark-local-example
)
* client mode(helm-values/k8s-spark-client-example
)

## run app locally

Install local Kubernetes cluster first. Use [minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/).

Install `task` build tools [task](https://taskfile.dev/#/installation).

Run locally:
```shell script
task run.local
```
Undeploy locally:
```shell script
task spark.helm.undeploy
```
![](./k8s-spark.gif)

## deploy on EKS Kubernetes cluster
Edit `.gitlab-ci.yml` file to adapt it to deploy to your own k8s namespace, please read team plateform documentation for details about gitlab runner and Kubernetes.

Create a branch will automatically deploy it on ew1d2 cluster, data-flux-dev namespace. Merge branch will deploy code on data-flux-stg and then to ew1p3 data-flux namespace.

## OPEN QUESTIONS
About rank, not quite sure if it's RANK, DENSE_RANK or row_number

## How I dev this app
I use a jupyter notebook to scatch the application, the notebook is in jupyter-notebook folder, you can see my dev process there by run it:
```
task jupyter
```

## requirements
Build a simple application that we can run to compute the top 25 pages on Wikipedia for each of the Wikipedia sub-domains:

* Accept input parameters for the date and hour of data to analyze (default to the current date/hour - 24 hours if not passed, i.e. previous day/hour).
Yes
* Download the page view counts from wikipedia for the given date/hour from https://dumps.wikimedia.org/other/pageviews/ More information on the format can be found here: https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Traffic/Pageviews
YES
* Eliminate any pages found in this blacklist: https://s3.amazonaws.com/dd-interview-data/data_engineer/wikipedia/blacklist_domains_and_pages
YES
* Compute the top 25 articles for the given day and hour by total pageviews for each unique domain in the remaining data.
YES
* Save the results to a file, either locally or on S3, sorted by domain and number of pageviews for easy perusal.
YES
* Only run these steps if necessary; that is, not rerun if the work has already been done for the given day and hour.
YES
* Be capable of being run for a range of dates and hours; each hour within the range should have its own result file.
it should not be managed in code, use a scheduler such as airflow

For your solution, explain:

* What additional things would you want to operate this application in a production setting?
Setup monitoring base on spark metrics
CI/CD pipeline for release or rollback
Tuning spark sql query plan to find more optimize query
could try use pandas for more optimized dataframe operations

* What might change about your solution if this application needed to run automatically for each hour of the day?
nothing much, as the application is not responsible for job schedule
* How would you test this application?
unit tests, integration tests in staging
* How you’d improve on this application design?
would consider to make backfilling using UI instead of command-line
consider to make spark stack transparent, so business users with only sql knowledge are able to run spark ETL without programming knowledge
may consider to find a third party payed solution in market to spare engineer work
