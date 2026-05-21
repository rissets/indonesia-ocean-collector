FROM airbyte/python-connector-base:1.1.0

WORKDIR /airbyte/integration_code

COPY source_kkp_datamart ./source_kkp_datamart
COPY setup.py ./
COPY main.py ./

RUN pip install --no-cache-dir .

ENV AIRBYTE_ENTRYPOINT "python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]
