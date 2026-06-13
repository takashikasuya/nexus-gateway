FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir uv
RUN uv pip install --system --no-cache bacpypes3

COPY sim/ sim/

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "sim/device.py"]
