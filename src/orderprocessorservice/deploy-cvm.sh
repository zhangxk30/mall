#!/usr/bin/env bash
# deploy-cvm.sh — install / update orderprocessorservice on a Tencent Cloud CVM
#
# Usage:
#   bash deploy-cvm.sh install    # first-time setup (creates user, service unit)
#   bash deploy-cvm.sh update     # pull new image and restart (rolling deploy)
#   bash deploy-cvm.sh status     # show service status and last 30 log lines
#   bash deploy-cvm.sh uninstall  # stop and remove everything
#
# Prerequisites:
#   - Docker installed on the CVM (yum install -y docker / apt-get install -y docker.io)
#   - CVM can reach <IMAGE_REGISTRY> (VPC route or public IP)
#   - Fill in REQUIRED SETTINGS below before running

set -euo pipefail

# ──────────────────────────────────────────────────────────────────────────────
# REQUIRED SETTINGS  (fill these in before running)
# ──────────────────────────────────────────────────────────────────────────────

IMAGE="<IMAGE_REGISTRY>/boutique-orderprocessorservice:v0.10.8-apm-otel"

MYSQL_HOST="<MYSQL_HOST>"
MYSQL_PORT="3306"
MYSQL_USER="root"
MYSQL_DATABASE="orders"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-}"   # export MYSQL_PASSWORD=... before running, or set inline here

KAFKA_BROKERS="<KAFKA_BROKERS>"
KAFKA_TOPIC="order-placed"

ENABLE_TRACING="1"
APM_ENDPOINT="<APM_ENDPOINT>:4319"
APM_SERVICE_NAME="boutique-demo-orderprocessorservice"
APM_TOKEN="${APM_TOKEN:-}"             # export APM_TOKEN=... before running, or set inline here

PORT="8081"
EXTRA_LATENCY=""           # e.g. "2s" to simulate slow processing; leave empty for normal operation

# ──────────────────────────────────────────────────────────────────────────────
# INTERNAL CONSTANTS
# ──────────────────────────────────────────────────────────────────────────────

SERVICE_NAME="orderprocessorservice"
CONTAINER_NAME="orderprocessorservice"
UNIT_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
ENV_DIR="/etc/${SERVICE_NAME}"
ENV_FILE="${ENV_DIR}/env"
LOG_DIR="/var/log/${SERVICE_NAME}"
LOG_FILE="${LOG_DIR}/app.log"
LOGROTATE_FILE="/etc/logrotate.d/${SERVICE_NAME}"

# ──────────────────────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────────────────────

die() { echo "ERROR: $*" >&2; exit 1; }
need_root() { [[ $EUID -eq 0 ]] || die "run as root (sudo bash $0 $*)"; }

# Resolve passwords from env if not set inline
: "${MYSQL_PASSWORD:?MYSQL_PASSWORD must be set (inline or via env)}"
: "${APM_TOKEN:?APM_TOKEN must be set (inline or via env)}"

# Secrets go into a 0600 env file that Docker reads with --env-file, NOT onto
# the ExecStart command line. Putting them on the command line would require
# shell-style escaping (printf %q), but systemd's ExecStart is not parsed by a
# shell and uses its own escaping rules — a password containing & or $ would
# reach the app mangled (e.g. "a&b" arriving as "a\&b"). --env-file takes
# values verbatim, so no escaping is involved at all. It also keeps the
# password out of `systemctl cat` and `ps` output.
write_env_file() {
    mkdir -p "${ENV_DIR}"
    umask 077
    cat > "${ENV_FILE}" <<EOF
PORT=${PORT}
MYSQL_HOST=${MYSQL_HOST}
MYSQL_PORT=${MYSQL_PORT}
MYSQL_USER=${MYSQL_USER}
MYSQL_DATABASE=${MYSQL_DATABASE}
MYSQL_PASSWORD=${MYSQL_PASSWORD}
KAFKA_BROKERS=${KAFKA_BROKERS}
KAFKA_TOPIC=${KAFKA_TOPIC}
ENABLE_TRACING=${ENABLE_TRACING}
APM_ENDPOINT=${APM_ENDPOINT}
APM_SERVICE_NAME=${APM_SERVICE_NAME}
APM_TOKEN=${APM_TOKEN}
APM_HOST_NAME=$(hostname)
EOF
    [[ -n "${EXTRA_LATENCY}" ]] && echo "EXTRA_LATENCY=${EXTRA_LATENCY}" >> "${ENV_FILE}"
    chmod 600 "${ENV_FILE}"
}

# Container runs in the foreground (no -d); systemd owns the process and pipes
# its stdout/stderr straight into LOG_FILE via StandardOutput=append:, so logs
# land on local disk at a fixed path without going through journald or
# Docker's json-file driver.
write_unit() {
    mkdir -p "${LOG_DIR}"

    cat > "${UNIT_FILE}" <<EOF
[Unit]
Description=orderprocessorservice (Kafka order consumer)
After=docker.service network-online.target
Requires=docker.service

[Service]
Type=simple
ExecStartPre=-/usr/bin/docker rm -f ${CONTAINER_NAME}
ExecStart=/usr/bin/docker run --rm --name ${CONTAINER_NAME} --env-file ${ENV_FILE} ${IMAGE}
ExecStop=/usr/bin/docker stop -t 10 ${CONTAINER_NAME}
Restart=on-failure
RestartSec=5
StandardOutput=append:${LOG_FILE}
StandardError=append:${LOG_FILE}
TimeoutStartSec=60

[Install]
WantedBy=multi-user.target
EOF

    cat > "${LOGROTATE_FILE}" <<EOF
${LOG_FILE} {
    daily
    rotate 14
    compress
    delaycompress
    missingok
    notifempty
    copytruncate
}
EOF
}

# ──────────────────────────────────────────────────────────────────────────────
# COMMANDS
# ──────────────────────────────────────────────────────────────────────────────

cmd_install() {
    need_root install

    # 1. Pull image
    echo "Pulling ${IMAGE} ..."
    docker pull "${IMAGE}"

    # 2. Stop any existing container
    docker rm -f "${CONTAINER_NAME}" 2>/dev/null || true

    # 3. Write env file (secrets, 0600) + systemd unit + logrotate config so the
    #    container survives reboots and logs land on local disk at LOG_FILE.
    echo "Writing env file to ${ENV_FILE} (mode 0600) ..."
    write_env_file
    echo "Writing systemd unit to ${UNIT_FILE} ..."
    echo "Logs will be written to ${LOG_FILE} ..."
    write_unit

    systemctl daemon-reload
    systemctl enable --now "${SERVICE_NAME}"

    echo ""
    echo "Installed and started. Checking health ..."
    sleep 5
    _health_check
}

cmd_update() {
    need_root update

    echo "Pulling ${IMAGE} ..."
    docker pull "${IMAGE}"

    echo "Rewriting env file and systemd unit with current settings ..."
    write_env_file
    write_unit

    systemctl daemon-reload
    systemctl restart "${SERVICE_NAME}"

    echo ""
    echo "Updated and restarted. Checking health ..."
    sleep 5
    _health_check
}

cmd_status() {
    echo "=== systemd unit ==="
    systemctl status "${SERVICE_NAME}" --no-pager || true
    echo ""
    echo "=== container ==="
    docker ps --filter "name=${CONTAINER_NAME}" --format "table {{.Names}}\t{{.Status}}\t{{.Image}}" || true
    echo ""
    echo "=== last 30 lines of ${LOG_FILE} ==="
    tail -n 30 "${LOG_FILE}" 2>/dev/null || echo "(log file not found yet: ${LOG_FILE})"
}

cmd_uninstall() {
    need_root uninstall
    echo "Stopping and removing ${SERVICE_NAME} ..."
    systemctl stop "${SERVICE_NAME}" 2>/dev/null || true
    systemctl disable "${SERVICE_NAME}" 2>/dev/null || true
    rm -f "${UNIT_FILE}" "${LOGROTATE_FILE}" "${ENV_FILE}"
    rmdir "${ENV_DIR}" 2>/dev/null || true
    systemctl daemon-reload
    docker rm -f "${CONTAINER_NAME}" 2>/dev/null || true
    echo "Done. Image ${IMAGE} and logs under ${LOG_DIR} were NOT removed"
    echo "(run 'docker rmi ${IMAGE}' / 'rm -rf ${LOG_DIR}' manually if needed)."
}

_health_check() {
    local url="http://127.0.0.1:${PORT}/healthz"
    local ok=false
    for i in $(seq 1 10); do
        if curl -sf "${url}" -o /dev/null 2>/dev/null; then
            ok=true; break
        fi
        sleep 2
    done
    if $ok; then
        echo "Health check passed: ${url}"
    else
        echo "WARNING: health check at ${url} did not respond within 20s."
        echo "Check logs with: bash $0 status"
    fi
}

# ──────────────────────────────────────────────────────────────────────────────
# DISPATCH
# ──────────────────────────────────────────────────────────────────────────────

case "${1:-}" in
    install)   cmd_install ;;
    update)    cmd_update ;;
    status)    cmd_status ;;
    uninstall) cmd_uninstall ;;
    *)
        echo "Usage: bash $0 {install|update|status|uninstall}"
        echo ""
        echo "  install   — first-time setup: pull image, create systemd service, start"
        echo "  update    — pull new image, rewrite unit, restart"
        echo "  status    — show systemd status + last 30 log lines"
        echo "  uninstall — stop, disable, remove container and unit file"
        exit 1
        ;;
esac
