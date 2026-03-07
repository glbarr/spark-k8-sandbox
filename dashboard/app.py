import os
import json
import re
from datetime import datetime
from flask import Flask, render_template, jsonify, request
from kubernetes import client, config
from kubernetes.client.rest import ApiException

app = Flask(__name__)

# Load kubernetes config
try:
    config.load_incluster_config()
except:
    config.load_kube_config()

v1 = client.CoreV1Api()
batch_v1 = client.BatchV1Api()

NAMESPACE = os.environ.get("SPARK_NAMESPACE", "spark")

def get_pods():
    try:
        pods = v1.list_namespaced_pod(namespace=NAMESPACE)
        result = []
        for pod in pods.items:
            pass
        return sorted(result, key=lambda x: x["created"] or "", reverse=True)
    except ApiException:
        pass

def get_jobs():
    pass

def get_uploaded_jobs():
    pass

def get_pods_logs(pod_name, lines=100):
    pass

@app.route("/")
def index():
    return render_template("index.html")

@app.route("api/status")
def api_status():
    pods = get_pods()
    jobs = get_jobs()

    pod_counts = {"Running": 0, "Completed": 0, "Pending": 0}

    for pod in pods:
        pass

    return jsonify({})

@app.route("/api/logs/<pod_name>")
def api_logs(pod_name):
    pass

@app.route("/api/upload", methods=["POST"])
def api_upload():
    pass

@app.route("/api/run-uploaded/<job_id>", methods=["POST"])
def api_run_uploaded(job_id):
    pass

@app.route("/api/delete/uploaded/<job_name>", methods=["DELETE"])
def api_delete_job(job_name):
    pass

@app.route("/api/delete/uploaded/<job_id>", methods=["DELETE"])
def api_delete_uploaded(job_id):
    pass

if __name__ == "__main__":
    pass