# Golang Templ HTMX App

This project is a web application built with **Go**, **Templ**, **HTMX**, and **Tailwind CSS**. It uses **PostgreSQL** for data storage and **SeaweedFS** for file storage.

## Prerequisites

- **Docker** & **Docker Compose**
- **Go** (for local development)
- **Node.js** & **npm** (for local development)

## Quick Start (Docker)

To run the entire application stack (App, Postgres, SeaweedFS):

```bash
docker-compose up --build
```

The application will be available at: `http://localhost:8081`

### Database Migrations
Migrations are applied **automatically** on application startup. You don't need to run them manually.

## Local Development

If you want to run the application locally (outside Docker) but keep dependencies in Docker:

1.  Start only infrastructure services:
    ```bash
    docker-compose up -d postgres seaweedfs-master seaweedfs-volume seaweedfs-filer
    ```
2.  Install dependencies:
    ```bash
    go mod download
    cd web && npm install && cd ..
    ```
3.  Run the application with hot reload:
    ```bash
    make watch
    ```

## Project Structure

- **cmd/myapp**: Entry point.
- **internal**: Application logic.
- **web**: Frontend assets and build config.
- **docker-compose.yml**: Full stack definition.
- **Dockerfile**: Application container definition.
