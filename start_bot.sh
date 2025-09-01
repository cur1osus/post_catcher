#!/bin/bash

# === Настройки ===
PROJECT_DIR="/home/max/Desktop/post_catcher"  # где лежит pyproject.toml и bot/
MANAGER_DIR="/home/max/Desktop/post_manager"
SESSION_PATH="$1"
API_ID="$2"
API_HASH="$3"
PHONE="$4"

LOG_DIR="$MANAGER_DIR/sessions"
LOG_FILE="$LOG_DIR/${PHONE}.log"
PID_FILE="$LOG_DIR/${PHONE}.pid"

# === Проверка аргументов ===
if [ -z "$PHONE" ]; then
    echo "Usage: $0 <session_path> <api_id> <api_hash> <phone>"
    exit 1
fi

# Создаём папку для логов
mkdir -p "$LOG_DIR"

# === Проверка, запущен ли уже бот ===
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE" 2>/dev/null)
    if [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
        echo "Bot for $PHONE is already running (PID: $PID)"
        exit 0
    else
        echo "Stale PID file found. Removing..."
        rm -f "$PID_FILE"
    fi
fi

# === Переходим в папку проекта и запускаем через uv ===
cd "$PROJECT_DIR" || {
    echo "Failed to cd to $PROJECT_DIR"
    exit 1
}

# Убираем VIRTUAL_ENV, чтобы uv не ругался
unset VIRTUAL_ENV

# Запускаем в фоне с помощью nohup
nohup uv run -m bot "$SESSION_PATH" "$API_ID" "$API_HASH" \
    >> "$LOG_FILE" 2>&1 &

# Сохраняем PID фонового процесса
echo $! > "$PID_FILE"

echo "Bot started for $PHONE with PID: $!"
