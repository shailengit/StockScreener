#!/bin/bash

# Database Update Scheduler Wrapper
# Runs all four database update scripts sequentially with retry logic

# Configuration
SCRIPT_DIR="/Users/shailendrakaushik/Documents/Python/AlgoTrading/StockScreener_2"
LOG_DIR="$HOME/Library/Logs/StockScreener"
VENV_PYTHON="$SCRIPT_DIR/venv/bin/python"

# Create log directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Generate timestamp for log file
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
LOG_FILE="$LOG_DIR/update_$TIMESTAMP.log"

# Scripts to run in order
SCRIPTS=(
    "sp1500_database_technical.py"
    "sp1500_database_metadata.py"
    "sp1500_database_fundamental_qtrly.py"
    "sp1500_database_fundamental_yearly.py"
)

# Track results
FAILED_SCRIPTS=()
SUCCESS_COUNT=0

# Function to send macOS notification
send_notification() {
    local title="$1"
    local message="$2"
    osascript -e "display notification \"$message\" with title \"$title\""
}

# Function to run a script with retry
run_script() {
    local script="$1"
    local script_path="$SCRIPT_DIR/$script"
    local max_retries=1
    local attempt=0

    while [ $attempt -le $max_retries ]; do
        attempt=$((attempt + 1))

        echo "========================================" | tee -a "$LOG_FILE"
        echo "[$(date)] Running $script (attempt $attempt of $((max_retries + 1)))" | tee -a "$LOG_FILE"
        echo "========================================" | tee -a "$LOG_FILE"

        # Run the script
        if "$VENV_PYTHON" "$script_path" 2>&1 | tee -a "$LOG_FILE"; then
            echo "[$(date)] $script completed successfully" | tee -a "$LOG_FILE"
            return 0
        else
            echo "[$(date)] $script failed on attempt $attempt" | tee -a "$LOG_FILE"
            if [ $attempt -le $max_retries ]; then
                echo "[$(date)] Retrying in 10 seconds..." | tee -a "$LOG_FILE"
                sleep 10
            fi
        fi
    done

    echo "[$(date)] $script failed after $((max_retries + 1)) attempts" | tee -a "$LOG_FILE"
    return 1
}

# Main execution
echo "========================================" | tee -a "$LOG_FILE"
echo "[$(date)] Starting database update batch" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"

for script in "${SCRIPTS[@]}"; do
    if run_script "$script"; then
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    else
        FAILED_SCRIPTS+=("$script")
        echo "[$(date)] Stopping due to failure in $script" | tee -a "$LOG_FILE"
        break
    fi
done

# Summary
echo "" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"
echo "[$(date)] Batch complete: $SUCCESS_COUNT/${#SCRIPTS[@]} succeeded" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"

# Send notification
if [ ${#FAILED_SCRIPTS[@]} -eq 0 ]; then
    send_notification "Database Update" "All 4 scripts completed successfully"
else
    send_notification "Database Update Failed" "Failed: ${FAILED_SCRIPTS[*]}"
fi

# Exit with error if any failed
if [ ${#FAILED_SCRIPTS[@]} -gt 0 ]; then
    exit 1
fi

exit 0