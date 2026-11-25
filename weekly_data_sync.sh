#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────
#  weekly_data_sync.sh   (run from cron or systemd-timer)
#  e.g. crontab:  0 3 * * 1  /path/to/EU-Contract-Hub/weekly_data_sync.sh
# ──────────────────────────────────────────────────────────────
set -euo pipefail

# Get the directory where this script is located (works on server)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKDIR="$SCRIPT_DIR"
cd "$WORKDIR"

# ---------- Single-instance lock mechanism ----------
# This prevents multiple copies of this script from running simultaneously
LOCK="/tmp/euchub_weekly.lock"
exec 9>"$LOCK"                    # Open file descriptor 9 for writing to lock file
flock -n 9 || {                   # Try to get exclusive lock (non-blocking with -n)
    echo "Daily job already running, exit."
    exit 0
}

TS=$(date +%Y-%m-%d_%H-%M)
LOGDIR="$WORKDIR/logs"
mkdir -p "$LOGDIR"

echo "=== DAILY JOB $TS started in $WORKDIR ===" | tee "$LOGDIR/daily_$TS.log"

# ── 1. XML ingestion ───────────────────────────────────────
echo "Starting XML ingestion..." | tee -a "$LOGDIR/daily_$TS.log"
python3 pipeline/xml-pipeline.py \
       >>"$LOGDIR/xml_$TS.log" 2>&1

# ── 2. Post-processing (run sequentially) ──────────────────
echo "Starting XML processing..." | tee -a "$LOGDIR/daily_$TS.log"
python3 pipeline/processing-pipeline-xml.py \
       >>"$LOGDIR/proc_xml_$TS.log" 2>&1

echo "Starting eForms processing..." | tee -a "$LOGDIR/daily_$TS.log"
python3 pipeline/processing-pipeline-eforms.py \
       >>"$LOGDIR/proc_eforms_$TS.log" 2>&1

# ── 3. Start / monitor the translator chain ────────────────
# Helper function: starts a detached screen session ONLY if it doesn't already exist
start_screen () {
    local SESSION=$1 CMD=$2
    # Screen sessions are listed as: "12345.session-name	(Detached)"
    # The grep pattern looks for: dot + session-name + whitespace
    if screen -list | grep -q "\.${SESSION}[[:space:]]"; then
        echo "Screen session '$SESSION' already running." | tee -a "$LOGDIR/daily_$TS.log"
    else
        echo "Starting screen session '$SESSION'..." | tee -a "$LOGDIR/daily_$TS.log"
        # -d = detached, -m = force new session, -S = session name
        screen -dmS "$SESSION" bash -c "$CMD"
    fi
}

# Just run both translators in parallel
start_screen "translator-contract" "cd '$WORKDIR' && python3 pipeline/translator.py >>'$LOGDIR/translator_$TS.log' 2>&1"
start_screen "translator-lot" "cd '$WORKDIR' && python3 pipeline/translator-lot.py >>'$LOGDIR/translator_lot_$TS.log' 2>&1"

echo "=== DAILY JOB finished $(date) ===" | tee -a "$LOGDIR/daily_$TS.log"