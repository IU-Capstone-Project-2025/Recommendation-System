# Recommendation System for Book Discovery

[![CI/CD Pipeline](https://github.com/IU-Capstone-Project-2025/Recommendation-System/actions/workflows/pipeline.yml/badge.svg?branch=dev)](https://github.com/IU-Capstone-Project-2025/Recommendation-System/actions/workflows/pipeline.yml)
[![Docker Compose](https://img.shields.io/badge/Docker-Compose-blue)](https://docs.docker.com/compose/)
[![FastAPI](https://img.shields.io/badge/Framework-FastAPI-green)](https://fastapi.tiangolo.com/)
[![License](https://img.shields.io/badge/License-MIT-orange)](LICENSE)

## Features
- Personalized book recommendations
- Reading list management (Read/Reading/Planned)
- 1-5 star rating system
- Weekly trending books
- Books Top


**Deployment Guide**:

- The official Keycloak and LDAP containers are run simultaneously via Docker using official guides, and are manually synchronized via user interfaces.
- Before starting all other components rename file .env.example to .env
- All other components may be built via one of the following orchestrators: docker/kubernetes. For docker-compose orchestration use ```docker compose up --build -d tester``` 




## Access Points

| Environment | Service                        | URL                           | Credentials       |
|-------------|--------------------------------|-------------------------------|-------------------|
| Local       | Web UI                         | http://localhost:8000         | -                 |
| Local       | Airflow                        | http://localhost:8080         | admin/admin       |
| Local       | Keycloak                       | http://localhost:8081         | admin/admin       |
| Local       | Postgres (backend)             | http://localhost:5432         | admin/admin       |
| Local       | Postgres (airflow)             | http://localhost:5433         | admin/admin       |
| Local       | Clickhouse   (HTTP connection) | http://localhost:8124         | admin/admin       |
| Local       | Clickhouse   (TCP connection)  | http://localhost:9001         | admin/admin       |
| Production  | Live Site                      | https://capstone2.cybertoad.ru | -                 |
| Production  | Authentication servises        | https://capstone.cybertoad.ru | -                 |
## Architecture

| Component           | Technology               |
|---------------------|--------------------------|
| Backend             | FastAPI                  |
| Frontend            | Jinja2 + HTML + CSS + JS |
| Auth                | Keycloak + LDAP          |
| Orchestration       | Airflow                  |
| Transactional DB    | PostgreSQL               |
| Analytical DB       | ClickHouse               |
| Infrastructure      | Docker/Kubernetes        |

## Testing
```bash
pytest --cov=src tests/
```
![Code Coverage](https://raw.githubusercontent.com/BogGoro/IU-Capstone-Project-2025/refs/heads/main/code_coverage.jpg)

## Team
| Member            | Role                                                    |
|-------------------|---------------------------------------------------------|
| Denis Troegubov   | Data Engineer, Team Lead                                |
| Timur Garifullin  | Full-Stack Developer                                    |
| Peter Zavadskii   | DevOps Engineer, Backend Developer, Algorithm Developer |
| Adelina Karavaeva | Frontend Developer                                      |
| Grigorii Belyaev  | Frontend Developer                                      |
