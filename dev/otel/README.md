# OpenTelemetry Stack
## Overview
This repository contains a pre-configured OpenTelemetry stack for observing your applications. It includes components for collecting, storing, and visualizing metrics, logs, and traces.

## Quick Start
To start the stack, run:

```bash
docker-compose up
```
This will spin up all components and make them available locally.

## Components
1. OpenTelemetry Collector
   - Acts as a central agent for collecting and exporting metrics, logs, and traces to different backends.
   - Configured to:
     - Push logs to Loki.
     - Push traces to Tempo.
     - Enable Prometheus to scrape metrics.
2. Prometheus
   - Responsible for scraping and storing application metrics.
   - Configured to scrape metrics from the OpenTelemetry Collector.
3. Loki
   - A log aggregation system for collecting, indexing, and querying logs.
   - Receives logs from the OpenTelemetry Collector.
4. Tempo
   - A distributed tracing backend for storing and querying traces.
   - Receives trace data from the OpenTelemetry Collector.
5. Grafana
   - A visualization and analytics tool that integrates with Prometheus, Loki, and Tempo.
   - Configured to use:
     - Prometheus for metrics.
     - Loki for logs.
     - Tempo for traces.
   - Viewable on http://localhost:9090

## Accessing the Dashboard
Once the stack is running, Grafana will be accessible at:

URL: http://localhost:3000
Default Credentials:
- Username: admin
- Password: admin

- You can change the default password upon logging in for the first time.
