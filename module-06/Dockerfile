FROM python:3.10.9-slim

RUN pip install -U pip & pip install pipenv

COPY [ "Pipfile", "Pipfile.lock", "./" ]

RUN pipenv install --system --deploy

COPY [ "batch/batch.py", "batch.py" ]
#COPY [ "model.bin", "model.bin" ]

COPY [ "integration_test.py", "integration_test.py" ]

COPY [ "entrypoint.sh", "entrypoint.sh" ]
RUN chmod +x entrypoint.sh

ENTRYPOINT [ "./entrypoint.sh" ]
