FROM python:3.11-slim
WORKDIR /app
COPY monitor.py .
RUN pip install flask pyzmq
CMD ["python", "monitor.py"]
