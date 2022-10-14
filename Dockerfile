FROM python:3.10-slim

ENV PIP_NO_CACHE_DIR=yes

ENV PYTHONDONTWRITEBYTECODE 1

ENV PYTHONPATH "${PYTHONPATH}:/code/"

WORKDIR /code

COPY xetra ./xetra
COPY Pipfile ./Pipfile
COPY Pipfile.lock ./Pipfile.lock
COPY run.py ./run.py

RUN pip install pipenv
RUN pipenv install --ignore-pipfile --system
