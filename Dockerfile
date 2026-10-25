# One faultline node. docker-compose.yml runs three of them as a cluster.
FROM python:3.12-slim

WORKDIR /app
# faultline needs nothing outside the standard library, so a plain pip
# install of the package is all it takes
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install --no-cache-dir .

# 6379: Redis clients. 7000: the other nodes.
EXPOSE 6379 7000
VOLUME /data
ENTRYPOINT ["faultline"]
CMD ["--help"]
