FROM python:3.12-slim

WORKDIR /app
COPY archive_index.py README.md start.sh ./

RUN useradd -m app && mkdir -p /data && chown -R app:app /app /data
USER app

EXPOSE 8787
CMD ["./start.sh"]
