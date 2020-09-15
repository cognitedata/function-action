FROM python:3-slim

RUN pip3 install poetry
RUN poetry config virtualenvs.create false

COPY poetry.lock .
COPY pyproject.toml .

RUN poetry install --no-dev

COPY src src
CMD python src/index.py
