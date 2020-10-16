FROM python:3.6 as builder

WORKDIR /mnt

COPY . /mnt

RUN apt-get update && apt-get install -y zip

RUN mkdir -p dist &&\
	zip -r ../dependency.zip python \
	-x "*/__pycache__/*" \
	-x "*/tests/*"

### spark image
FROM bitnami/spark:3.0.1

#USER root
WORKDIR /workspace

COPY requirements.txt .
#RUN pip install --no-cache-dir -r requirements.txt

# copy Python dependecies and libs
COPY --from=builder /mnt/dist/dependencies.zip /workspace/dist/dependencies.zip

# copy all Spark jobs
COPY ./python/ /workspace/python/
COPY ./configs /workspace/configs

#USER 1001
# provide entrypoint file
COPY ./scripts/entrypoint.sh /workspace/scripts/entrypoint.sh

CMD ["/workspace/scripts/entrypoint.sh"]
