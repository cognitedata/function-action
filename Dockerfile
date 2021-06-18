# gcr.io/distroless/python3-debian10 (runtime env is using 3.8 and that's important for native dependencies)
FROM python:3.8-slim AS builder

ADD src /app
WORKDIR /

# Poetry setup
RUN pip3 install poetry
RUN poetry config virtualenvs.create false

COPY action.yaml /app
COPY poetry.lock .
COPY pyproject.toml .

# by default poetry does NOT export dev dependencies here
RUN poetry export -f requirements.txt --output /requirements.txt

# We are installing a dependency here directly into our app source dir
RUN pip3 install --target=/app -r /requirements.txt --upgrade

# A distroless container image with Python and some basics like SSL certificates
# https://github.com/GoogleContainerTools/distroless
FROM gcr.io/distroless/python3-debian10
COPY --from=builder /app /app
ENV PYTHONPATH /app
CMD ["/app/index.py"]
