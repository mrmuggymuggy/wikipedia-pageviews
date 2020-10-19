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

# copy Python dependecies
COPY --from=builder /mnt/dependencies.zip /workspace/dist/dependencies.zip
# copy Spark jobs
COPY ./jobs/ /workspace/jobs/

# provide entrypoint file
COPY ./scripts/entrypoint.sh /workspace/scripts/entrypoint.sh

CMD ["/workspace/scripts/entrypoint.sh"]
