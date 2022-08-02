FROM python:3.10-slim

WORKDIR /app

RUN python -m pip install --upgrade pip

COPY requirements.txt /app
RUN pip install -r requirements.txt

COPY src/*.py /app

ENTRYPOINT [ "python3" ]
CMD [ "kafka-backup.py", "--help" ]
