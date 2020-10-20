# Wikipedia pageviews ranking app
This Project runs Spark job to compute wikipedia toprank pages for each domain
Tech stack
* Spark SQL (Spark 3.0.1)
* Python 3.8
* docker-compose
* Airflow 1.10.10
* Kubernetes
* Taskfile(makefile like build tool)

## Run the app locally
Make sure you have docker-compose and task(https://taskfile.dev/#/) installed
Application run and unit tests are in docker, so no other dependencies needed
Local run:
```shell script
task local.run
```
Unit-tests local run:
```shell script
task unit-tests
```
## Customized parameters and check outputs
## How and Why I choose the tech tech stack
1) spark sql
## How I dev this app
I use a jupyter notebook as code scatch pad, the notebook is in `jupyter-notebook` folder, you can see my dev process by run:
```
task jupyter
```
it will start a jupyter notebook in docker and mount my notebook

## OPEN QUESTIONS
About rank, not quite sure if it's RANK, DENSE_RANK or row_number
run for daily run demand lot of resources
https://codingsight.com/similarities-and-differences-among-rank-dense_rank-and-row_number-functions/
## How I dev this app
I use a jupyter notebook to scatch the application, the notebook is in jupyter-notebook folder, you can see my dev process there by run it:
```
task jupyter
```
considered to deploy as kubernetes cronjob but it doesn't allow backfilling, so discard it

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

* What might change about your solution if this application needed to run automatically for each hour of the day?
I need to adjust airflow dag to use sensor to detect the availability of new files instead of the schedule run, as the pageviews files might not be available right away
will consider output parquet files
* How would you test this application?
unit tests, integration tests in staging
* How you’d improve on this application design?
may consider to install backfilling UI pluggin to airflow to not run backfill on command-line
may consider to make spark stack transparent, so business users with only sql knowledge are able to schedule spark ETL without programming and ops knowledge
may consider to find a third party payed solution in market to spare engineer work
may consider simply dump raw data as partitioned day/hours parquet files to data-lake(snowflake) and schedule sql jobs, probably more expensive but more transparent for data department
