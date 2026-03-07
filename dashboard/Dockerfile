FROM apache/spark:3.5.3-python3

USER root

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

#Copy application
COPY app.py .
COPY templates/ templates/

ENV FLASK_APP=app.py
ENV SPARK_NAMESPACE=spark

EXPOSE 5000

CMD [ "gunicorn", "--bind", "0.0.0.0:5000", "--workers", "2", "--threads", "4", "app:app"]
