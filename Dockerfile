FROM python:3.6 as builder

WORKDIR /mnt

COPY . /mnt

RUN apt-get update && apt-get install -y zip

RUN zip -r dependencies.zip jobs \
	-x "*/__pycache__/*" \
	-x "*/tests/*"

### same spark image as our juyter-notebook use spark 3.0.1 and python 3.6.12
FROM jupyter/pyspark-notebook:feacdbfc2e89

WORKDIR /workspace

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

#hadoop-aws for read/write s3 so don't need to download it everytime on job's spark-submit
RUN echo "System.exit(0)" > /tmp/dependencies.scala; \
	$SPARK_HOME/bin/spark-shell --packages \
	org.apache.hadoop:hadoop-aws:3.2.0 \
	-i /tmp/dependencies.scala; \
	rm /tmp/dependencies.scala;

# copy Python dependecies
COPY --from=builder /mnt/dependencies.zip /workspace/dist/dependencies.zip
# copy Spark jobs
COPY ./jobs/ /workspace/jobs/

# provide entrypoint file
COPY ./scripts/entrypoint.sh /workspace/scripts/entrypoint.sh

CMD ["/workspace/scripts/entrypoint.sh"]
