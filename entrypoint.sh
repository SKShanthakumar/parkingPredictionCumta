#!/bin/sh

printenv | grep -vE "^(HOME|PATH|PWD|SHLVL|_)=.*" > /app/.env

cron
# Optional: wait a moment to ensure cron starts
sleep 1
uvicorn server:app --host 0.0.0.0 --port 5001