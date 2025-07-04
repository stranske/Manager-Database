FROM python:3.12-slim
WORKDIR /app
COPY ../requirements.txt /tmp/req.txt
RUN pip install --no-cache-dir -r /tmp/req.txt
CMD ["streamlit", "run", "daily_report.py", "--server.port=8501", "--server.headless=true"]
