FROM phidata/airflow:2.2.5

COPY requirements.txt /
COPY airflow-requirements.txt /

RUN pip install --upgrade pip
RUN pip install -r /requirements.txt
RUN pip install -r /airflow-requirements.txt
