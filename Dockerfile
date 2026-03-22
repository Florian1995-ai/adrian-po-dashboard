FROM python:3.12-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY .streamlit/ .streamlit/
COPY execution/adrian_dashboard.py execution/adrian_db_manager.py execution/adrian_po_processor.py execution/
COPY resources/sample_po_data.json resources/
EXPOSE 8501
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1
CMD ["streamlit", "run", "execution/adrian_dashboard.py", "--server.port=8501", "--server.headless=true"]