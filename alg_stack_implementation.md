# Log Aggregation: The ALG Stack (Alloy, Loki, Grafana)

This document outlines the steps to implement a modern log aggregation stack using **Grafana Alloy** (the successor to Promtail/Agent), **Loki**, and **Grafana**. 

Since your Go application already emits perfectly structured JSON via `zerolog`, we require **zero code changes** to your application. We simply need to update your infrastructure.

---

## 1. How the Stack Works
1. **Application (Go):** Writes JSON strings to `stdout`. Docker captures this and saves it to the host machine's hard drive.
2. **Grafana Alloy:** A highly efficient telemetry collector. It mounts the Docker socket, reads the log streams, parses your JSON, and converts your `level` and `component` keys into queryable labels.
3. **Loki:** A time-series database specifically optimized for logs. It receives the streams from Alloy.
4. **Grafana:** The visualizer that reads from Loki to display the logs and generate metric dashboards.

---

## 2. Configuration Files Required

You will need to create two configuration files in your root directory before updating Docker Compose.

### A. `loki-config.yaml`
This configures Loki to run locally in a lightweight, single-binary mode without requiring complex cloud storage.

```yaml
auth_enabled: false

server:
  http_listen_port: 3100

ingester:
  lifecycler:
    address: 127.0.0.1
    ring:
      kvstore:
        store: inmemory
      replication_factor: 1
    final_sleep: 0s
  chunk_idle_period: 5m
  chunk_retain_period: 30s

schema_config:
  configs:
    - from: 2020-05-15
      store: boltdb
      object_store: filesystem
      schema: v11
      index:
        prefix: index_
        period: 168h

storage_config:
  boltdb:
    directory: /tmp/loki/index
  filesystem:
    directory: /tmp/loki/chunks

limits_config:
  enforce_metric_name: false
  reject_old_samples: true
  reject_old_samples_max_age: 168h
```

### B. `config.alloy`
Grafana Alloy uses the "River" configuration language. This script tells Alloy to find all running Docker containers, parse their logs as JSON, extract your custom zerolog fields, and push them to Loki.

```river
// 1. Scrape logs from the local Docker socket
loki.source.docker "local_containers" {
  host       = "unix:///var/run/docker.sock"
  targets    = [{"__address__" = "localhost"}]
  forward_to = [loki.process.parse_json.receiver]
}

// 2. Process the logs (Extract JSON fields into Labels)
loki.process "parse_json" {
  forward_to = [loki.write.local_loki.receiver]

  // Standardize docker labels (like container name)
  stage.docker { }

  // Parse the zerolog JSON structure
  stage.json {
    expressions = {
      level     = "level",
      component = "component",
      video_id  = "video_id",
    }
  }

  // Convert the extracted JSON fields into native Loki labels.
  // This makes `{component="transcode_worker"}` a highly optimized query in Grafana.
  stage.labels {
    values = {
      level     = "",
      component = "",
    }
  }
}

// 3. Send the processed logs to our local Loki container
loki.write "local_loki" {
  endpoint {
    url = "http://loki:3100/loki/api/v1/push"
  }
}
```

---

## 3. Docker Compose Updates
You will need to append these three services to your existing `docker-compose.yml`. 

*Note: Alloy requires access to the Docker socket to read the container logs automatically.*

```yaml
services:
  # ... (Keep your existing minio, redis, backend-api, backend-worker, nginx) ...

  loki:
    image: grafana/loki:latest
    container_name: loki
    ports:
      - "3100:3100"
    command: -config.file=/etc/loki/local-config.yaml
    volumes:
      - ./loki-config.yaml:/etc/loki/local-config.yaml
    restart: unless-stopped

  alloy:
    image: grafana/alloy:latest
    container_name: alloy
    volumes:
      # Mount docker socket so Alloy can auto-discover containers
      - /var/run/docker.sock:/var/run/docker.sock:ro
      # Mount Alloy configuration
      - ./config.alloy:/etc/alloy/config.alloy:ro
    command: run --server.http.listen-addr=0.0.0.0:12345 /etc/alloy/config.alloy
    ports:
      - "12345:12345" # Alloy UI for debugging pipelines
    restart: unless-stopped
    depends_on:
      - loki

  grafana:
    image: grafana/grafana:latest
    container_name: grafana
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    depends_on:
      - loki
```

---

## 4. How to Use the Stack
1. Start your stack: `docker compose up -d`
2. Go to Grafana at `http://localhost:3000` (Login: `admin` / `admin`).
3. Add a Data Source:
   - Type: **Loki**
   - URL: `http://loki:3100`
4. Go to the "Explore" tab and try these queries:
   - `{component="transcode_worker"}` (Shows all worker logs)
   - `{level="error"}` (Shows only errors across the whole app)
   - `{container="/backend-api"}` (Shows all logs from the API container)
