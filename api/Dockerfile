FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt /tmp/req.txt
RUN pip install --no-cache-dir -r /tmp/req.txt
CMD ["uvicorn", "api.chat:app", "--host", "0.0.0.0", "--port", "8000"]
