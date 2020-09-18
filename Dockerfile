FROM python:3-slim

WORKDIR action

RUN pip3 install poetry
RUN poetry config virtualenvs.create false

COPY poetry.lock .
COPY pyproject.toml .

RUN poetry install --no-dev

COPY src src

RUN ls
RUN ls src

CMD ls && python src/index.py
