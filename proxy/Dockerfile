FROM python:3.11-slim
WORKDIR /app
COPY proxy-pub.py .
RUN pip install pyzmq
CMD ["python", "proxy-pub.py"]
