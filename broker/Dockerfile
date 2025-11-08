FROM python:3.11-slim
WORKDIR /app
COPY broker.py .
RUN pip install pyzmq
CMD ["python", "broker.py"]
