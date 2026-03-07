import os
import re
import requests as _http
from datetime import datetime, timezone
from flask import Flask, render_template, jsonify, request, Response, stream_with_context
from kubernetes import client, config
from kubernetes.client.rest import ApiException

app = Flask(__name__)

# Load kubernetes config
try:
    config.load_incluster_config()
except Exception:
    config.load_kube_config()

v1 = client.CoreV1Api()
batch_v1 = client.BatchV1Api()

NAMESPACE = os.environ.get("SPARK_NAMESPACE", "spark")
SPARK_JOB_IMAGE = os.environ.get("SPARK_JOB_IMAGE", "spark-sandbox:latest")

UPLOADED_JOB_LABEL = "spark-dashboard/type"
UPLOADED_JOB_LABEL_VALUE = "uploaded-job"


def get_pods():
    try:
        pods = v1.list_namespaced_pod(namespace=NAMESPACE)
        result = []
        for pod in pods.items:
            created = pod.metadata.creation_timestamp.isoformat() if pod.metadata.creation_timestamp else None
            result.append({
                "name": pod.metadata.name,
                "status": pod.status.phase or "Unknown",
                "created": created,
                "labels": pod.metadata.labels or {},
                "node": pod.spec.node_name,
            })
        return sorted(result, key=lambda x: x["created"] or "", reverse=True)
    except ApiException:
        return []


def get_jobs():
    try:
        jobs = batch_v1.list_namespaced_job(namespace=NAMESPACE)
        result = []
        for job in jobs.items:
            status = "Running"
            for condition in (job.status.conditions or []):
                if condition.type == "Complete" and condition.status == "True":
                    status = "Completed"
                    break
                elif condition.type == "Failed" and condition.status == "True":
                    status = "Failed"
                    break
            created = job.metadata.creation_timestamp.isoformat() if job.metadata.creation_timestamp else None
            result.append({
                "name": job.metadata.name,
                "status": status,
                "created": created,
                "active": job.status.active or 0,
                "succeeded": job.status.succeeded or 0,
                "failed": job.status.failed or 0,
            })
        return sorted(result, key=lambda x: x["created"] or "", reverse=True)
    except ApiException:
        return []


def get_uploaded_jobs():
    try:
        cms = v1.list_namespaced_config_map(
            namespace=NAMESPACE,
            label_selector=f"{UPLOADED_JOB_LABEL}={UPLOADED_JOB_LABEL_VALUE}"
        )
        result = []
        for cm in cms.items:
            created = cm.metadata.creation_timestamp.isoformat() if cm.metadata.creation_timestamp else None
            annotations = cm.metadata.annotations or {}
            result.append({
                "id": cm.metadata.name,
                "filename": annotations.get("spark-dashboard/filename", cm.metadata.name),
                "created": created,
            })
        return sorted(result, key=lambda x: x["created"] or "", reverse=True)
    except ApiException:
        return []


def get_pod_logs(pod_name, lines=100):
    try:
        return v1.read_namespaced_pod_log(
            name=pod_name,
            namespace=NAMESPACE,
            tail_lines=lines
        )
    except ApiException as e:
        if e.status == 400:
            return "(Container not ready yet — try again in a moment)"
        return f"Error fetching logs: {e.reason}"


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/status")
def api_status():
    pods = get_pods()
    jobs = get_jobs()

    pod_counts = {"Running": 0, "Completed": 0, "Pending": 0, "Failed": 0, "Unknown": 0}
    for pod in pods:
        phase = pod["status"]
        pod_counts[phase] = pod_counts.get(phase, 0) + 1

    return jsonify({
        "pods": pods,
        "jobs": jobs,
        "pod_counts": pod_counts,
    })


@app.route("/api/uploaded-jobs")
def api_uploaded_jobs():
    return jsonify(get_uploaded_jobs())


@app.route("/api/logs/<pod_name>")
def api_logs(pod_name):
    lines = request.args.get("lines", 100, type=int)
    logs = get_pod_logs(pod_name, lines)
    return jsonify({"pod": pod_name, "logs": logs})


@app.route("/api/upload", methods=["POST"])
def api_upload():
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files["file"]
    if not file.filename or not file.filename.endswith(".py"):
        return jsonify({"error": "Only .py files are allowed"}), 400

    filename = file.filename
    content = file.read().decode("utf-8")

    # Sanitize filename into a valid ConfigMap name
    base = filename[:-3] if filename.endswith(".py") else filename
    job_id = re.sub(r"[^a-z0-9-]", "-", base.lower().replace("_", "-"))
    job_id = re.sub(r"-+", "-", job_id).strip("-")
    job_id = f"spark-job-{job_id}"

    cm = client.V1ConfigMap(
        metadata=client.V1ObjectMeta(
            name=job_id,
            namespace=NAMESPACE,
            labels={UPLOADED_JOB_LABEL: UPLOADED_JOB_LABEL_VALUE},
            annotations={"spark-dashboard/filename": filename},
        ),
        data={filename: content},
    )

    try:
        v1.create_namespaced_config_map(namespace=NAMESPACE, body=cm)
    except ApiException as e:
        if e.status == 409:
            return jsonify({"error": f"A job named '{job_id}' already exists"}), 409
        return jsonify({"error": f"Failed to upload job: {e.reason}"}), 500

    return jsonify({"id": job_id, "filename": filename}), 201


@app.route("/api/run-uploaded/<job_id>", methods=["POST"])
def api_run_uploaded(job_id):
    try:
        cm = v1.read_namespaced_config_map(name=job_id, namespace=NAMESPACE)
    except ApiException as e:
        if e.status == 404:
            return jsonify({"error": f"Uploaded job '{job_id}' not found"}), 404
        return jsonify({"error": f"Failed to fetch job: {e.reason}"}), 500

    annotations = cm.metadata.annotations or {}
    filename = annotations.get("spark-dashboard/filename", list(cm.data.keys())[0])
    run_name = f"{job_id}-{int(datetime.now(timezone.utc).timestamp())}"

    job = client.V1Job(
        metadata=client.V1ObjectMeta(
            name=run_name,
            namespace=NAMESPACE,
            labels={"app": "spark-job", "spark-dashboard/source-job": job_id},
        ),
        spec=client.V1JobSpec(
            backoff_limit=0,
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(
                    labels={"app": "spark-job", "spark-dashboard/source-job": job_id},
                ),
                spec=client.V1PodSpec(
                    service_account_name="spark",
                    restart_policy="Never",
                    containers=[
                        client.V1Container(
                            name="spark-job",
                            image=SPARK_JOB_IMAGE,
                            image_pull_policy="Never",
                            command=[
                                "/opt/spark/bin/spark-submit",
                                "--master", "local[*]",
                                "--conf", "spark.eventLog.enabled=true",
                                "--conf", "spark.eventLog.dir=/tmp/spark-events",
                                f"/tmp/jobs/{filename}",
                            ],
                            volume_mounts=[
                                client.V1VolumeMount(name="job-files", mount_path="/tmp/jobs"),
                                client.V1VolumeMount(name="spark-events", mount_path="/tmp/spark-events"),
                            ],
                            resources=client.V1ResourceRequirements(
                                requests={"memory": "512Mi", "cpu": "500m"},
                                limits={"memory": "1Gi", "cpu": "1"},
                            ),
                        )
                    ],
                    volumes=[
                        client.V1Volume(
                            name="job-files",
                            config_map=client.V1ConfigMapVolumeSource(name=job_id),
                        ),
                        client.V1Volume(
                            name="spark-events",
                            host_path=client.V1HostPathVolumeSource(
                                path="/tmp/spark-events",
                                type="DirectoryOrCreate",
                            ),
                        ),
                    ],
                ),
            ),
        ),
    )

    try:
        batch_v1.create_namespaced_job(namespace=NAMESPACE, body=job)
    except ApiException as e:
        return jsonify({"error": f"Failed to create job: {e.reason}"}), 500

    return jsonify({"job_name": run_name, "source_job": job_id}), 201


@app.route("/spark-ui/<pod_name>/", defaults={"subpath": ""})
@app.route("/spark-ui/<pod_name>/<path:subpath>")
def spark_ui_proxy(pod_name, subpath):
    try:
        pod = v1.read_namespaced_pod(name=pod_name, namespace=NAMESPACE)
    except ApiException as e:
        return ("Pod not found", 404) if e.status == 404 else (f"Error: {e.reason}", 500)

    pod_ip = pod.status.pod_ip
    if not pod_ip:
        return "Pod not ready yet", 503

    qs = request.query_string.decode("utf-8")
    target = f"http://{pod_ip}:4040/{subpath}"
    if qs:
        target = f"{target}?{qs}"

    try:
        upstream = _http.get(
            target,
            headers={k: v for k, v in request.headers if k.lower() != "host"},
            timeout=10,
            stream=True,
        )
    except _http.exceptions.ConnectionError:
        return "Spark UI not available — job may not be running yet", 503
    except _http.exceptions.Timeout:
        return "Spark UI timed out", 504

    content_type = upstream.headers.get("Content-Type", "")
    prefix = f"/spark-ui/{pod_name}".encode()

    if "text/html" in content_type:
        body = upstream.content
        for old, new in [
            (b'href="/', b'href="' + prefix + b"/"),
            (b"href='/", b"href='" + prefix + b"/"),
            (b'src="/', b'src="' + prefix + b"/"),
            (b"src='/", b"src='" + prefix + b"/"),
            (b'action="/', b'action="' + prefix + b"/"),
        ]:
            body = body.replace(old, new)
        return Response(body, status=upstream.status_code, content_type=content_type)

    return Response(
        stream_with_context(upstream.iter_content(chunk_size=8192)),
        status=upstream.status_code,
        content_type=content_type,
    )


@app.route("/api/delete/job/<job_name>", methods=["DELETE"])
def api_delete_job(job_name):
    try:
        batch_v1.delete_namespaced_job(
            name=job_name,
            namespace=NAMESPACE,
            body=client.V1DeleteOptions(propagation_policy="Foreground"),
        )
    except ApiException as e:
        if e.status == 404:
            return jsonify({"error": f"Job '{job_name}' not found"}), 404
        return jsonify({"error": f"Failed to delete job: {e.reason}"}), 500

    return jsonify({"deleted": job_name})


@app.route("/api/delete/uploaded/<job_id>", methods=["DELETE"])
def api_delete_uploaded(job_id):
    try:
        v1.delete_namespaced_config_map(name=job_id, namespace=NAMESPACE)
    except ApiException as e:
        if e.status == 404:
            return jsonify({"error": f"Uploaded job '{job_id}' not found"}), 404
        return jsonify({"error": f"Failed to delete uploaded job: {e.reason}"}), 500

    return jsonify({"deleted": job_id})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)