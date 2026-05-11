# CI/CD Workflows

Three GitHub Actions workflows that automatically validate code quality, test services, and run the full streaming pipeline.

---

## Workflows

```
.github/workflows/
├── ci-code-quality.yml     # Lint and syntax checks
├── ci-docker-test.yml      # Docker services integration test
└── cd-run-pipeline.yml     # Full pipeline run
```

---

## When Each Workflow Runs

| Workflow | Trigger |
|---|---|
| `ci-code-quality.yml` | Every `push` and `pull_request` |
| `ci-docker-test.yml` | `pull_request` only |
| `cd-run-pipeline.yml` | `push` to `feature/CI` branch only |

---

## 1. CI — Code Quality (`ci-code-quality.yml`)

Checks only the files that changed — not the entire project.

| Step | What it does |
|---|---|
| Setup Python 3.10 | Installs runtime and caches pip dependencies |
| Python Syntax Check | Fails on any `.py` syntax error |
| Lint Python | Style check (PEP8 via `flake8`) — non-blocking |
| YAML Validation | Validates all `.yml` files via `pyyaml` |
| Lint SQL | Style check via `sqlfluff` — non-blocking |
| SQL Parse Check | Ensures SQL files are parseable by Flink |

> Syntax errors block the workflow. Linting warnings do not.

---

## 2. CI — Docker Services Test (`ci-docker-test.yml`)

Spins up the full Docker environment and verifies every service is healthy before merging.

| Step | What it does |
|---|---|
| Create `.env` | Generates a `.env` with non-sensitive test values |
| Build & start services | Runs `docker compose build` then `up` |
| Wait for Flink | Polls Flink REST API until ready |
| Check Kafka | Verifies broker is up and lists topics |
| Check MinIO | Verifies object storage is reachable |
| Dump logs on failure | Prints container logs if any step fails |
| Cleanup | Stops containers and removes volumes; deletes `.env` |

---

## 3. CD — Run Streaming Pipeline (`cd-run-pipeline.yml`)

Runs the end-to-end pipeline using real secrets and verifies data lands in Iceberg.

| Step | What it does |
|---|---|
| Create `.env` | Populates from GitHub Secrets |
| Build & start services | Full `docker compose up -d` |
| Wait for Flink | Up to 180s |
| Wait for MinIO | Up to 120s |
| Run pipeline | Executes `scripts/run_pipeline.sh` |
| Check Iceberg data | Confirms data was written to MinIO |
| Cleanup | `docker compose down -v`; deletes `.env` |

---

## CI vs CD at a Glance

| | CI | CD |
|---|---|---|
| Uses real secrets? | No | Yes |
| Runs full pipeline? | No | Yes |
| Verifies Iceberg data? | No | Yes |
| Runs on every push? | Yes (code quality) / PR only (docker) | No — `feature/CI` branch only |

---


## Viewing Results

1. Go to the repository on GitHub
2. Click the **Actions** tab
3. Select a workflow run to see step-by-step logs
4. If a step failed, expand **Dump logs** for container output