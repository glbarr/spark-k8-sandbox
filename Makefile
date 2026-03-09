NS := spark

# ── Images ────────────────────────────────────────────────────────────────────

.PHONY: build build-sandbox build-dashboard

build: build-sandbox build-dashboard

build-sandbox:
	docker build -t spark-sandbox:latest .

build-dashboard:
	docker build -t spark-dashboard:latest dashboard/

# ── Apply K8s manifests ───────────────────────────────────────────────────────

.PHONY: apply apply-infra apply-all

# Bootstrap: namespace + RBAC only (run once before anything else)
apply-infra:
	kubectl apply -f k8s/namespace.yml
	kubectl apply -f k8s/rbac.yml -n $(NS)

# Apply everything (assumes namespace already exists)
apply:
	kubectl apply -f k8s/data-warehouse.yml -n $(NS)
	kubectl apply -f k8s/spark-history-server.yml -n $(NS)
	kubectl apply -f k8s/jupyter.yml -n $(NS)
	kubectl apply -f k8s/dashboard.yml -n $(NS)

# Full first-time setup
setup: apply-infra apply

# ── Deploy (build + apply) ────────────────────────────────────────────────────

.PHONY: deploy deploy-sandbox deploy-dashboard

deploy: build apply

deploy-sandbox: build-sandbox
	kubectl rollout restart deployment/jupyter -n $(NS)

deploy-dashboard: build-dashboard
	kubectl rollout restart deployment/spark-dashboard -n $(NS)

# ── Restart deployments ───────────────────────────────────────────────────────

.PHONY: restart restart-dashboard restart-jupyter restart-history

restart:
	kubectl rollout restart deployment/spark-dashboard -n $(NS)
	kubectl rollout restart deployment/jupyter -n $(NS)
	kubectl rollout restart deployment/spark-history-server -n $(NS)

restart-dashboard:
	kubectl rollout restart deployment/spark-dashboard -n $(NS)

restart-jupyter:
	kubectl rollout restart deployment/jupyter -n $(NS)

restart-history:
	kubectl rollout restart deployment/spark-history-server -n $(NS)

# ── Observe ───────────────────────────────────────────────────────────────────

.PHONY: status logs logs-jupyter logs-history

status:
	kubectl get pods -n $(NS)

logs:
	kubectl logs -f -n $(NS) -l app=spark-dashboard

logs-jupyter:
	kubectl logs -f -n $(NS) -l app=jupyter

logs-history:
	kubectl logs -f -n $(NS) -l app=spark-history-server

# ── Cleanup ───────────────────────────────────────────────────────────────────

.PHONY: clean-jobs clean

clean-jobs:
	kubectl delete jobs -n $(NS) --all

# Tear down the entire namespace (destructive — deletes PVC data too)
clean:
	kubectl delete namespace $(NS)