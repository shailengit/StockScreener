# Database Scheduler Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create a macOS launchd-based scheduler that runs all four database update scripts daily at 6PM with retry logic and macOS notifications.

**Architecture:** A shell wrapper script that orchestrates the four Python scripts sequentially, with retry logic and notification support. A launchd plist schedules this to run daily at 6PM.

**Tech Stack:** macOS launchd, shell scripting, terminal-notifier (or osascript for notifications)

---

## File Structure

- Create: `scripts/run_db_updates.sh` — wrapper script that runs all 4 Python scripts
- Create: `scripts/com.stockscreener.db-updater.plist` — launchd plist for scheduling

---

## Tasks

### Task 1: Create Wrapper Shell Script

**Files:**
- Create: `scripts/run_db_updates.sh`

- [ ] **Step 1: Create the scripts directory**

Run: `mkdir -p /Users/shailendrakaushik/Documents/Python/AlgoTrading/StockScreener_2/scripts`

- [ ] **Step 2: Write the wrapper script**

```bash
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
    local success=0

    while [ $attempt -le $max_retries ]; do
        attempt=$((attempt + 1))

        echo "========================================" | tee -a "$LOG_FILE"
        echo "[$(date)] Running $script (attempt $attempt of $((max_retries + 1)))" | tee -a "$LOG_FILE"
        echo "========================================" | tee -a "$LOG_FILE"

        # Run the script
        if "$VENV_PYTHON" "$script_path" 2>&1 | tee -a "$LOG_FILE"; then
            echo "[$(date)] $script completed successfully" | tee -a "$LOG_FILE"
            success=1
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
```

- [ ] **Step 3: Make the script executable**

Run: `chmod +x /Users/shailendrakaushik/Documents/Python/AlgoTrading/StockScreener_2/scripts/run_db_updates.sh`

Expected: No output, script is now executable

- [ ] **Step 4: Test the script runs without errors (dry run check)**

Run: `bash -n /Users/shailendrakaushik/Documents/Python/AlgoTrading/StockScreener_2/scripts/run_db_updates.sh`

Expected: No output (syntax check passes)

- [ ] **Step 5: Commit**

```bash
git add scripts/run_db_updates.sh
git commit -m "feat: add database update wrapper script with retry logic"
```

---

### Task 2: Create Launch Agent Plist

**Files:**
- Create: `scripts/com.stockscreener.db-updater.plist`

- [ ] **Step 1: Write the launchd plist**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.stockscreener.db-updater</string>

    <key>ProgramArguments</key>
    <array>
        <string>/Users/shailendrakaushik/Documents/Python/AlgoTrading/StockScreener_2/scripts/run_db_updates.sh</string>
    </array>

    <key>StartCalendarInterval</key>
    <array>
        <dict>
            <key>Hour</key>
            <integer>18</integer>
            <key>Minute</key>
            <integer>0</integer>
        </dict>
    </array>

    <key>StandardOutPath</key>
    <string>/Users/shailendrakaushik/Library/Logs/StockScreener/launchd.out.log</string>

    <key>StandardErrorPath</key>
    <string>/Users/shailendrakaushik/Library/Logs/StockScreener/launchd.err.log</string>

    <key>RunAtLoad</key>
    <false/>

    <key>KeepAlive</key>
    <false/>
</dict>
</plist>
```

- [ ] **Step 2: Commit**

```bash
git add scripts/com.stockscreener.db-updater.plist
git commit -m "feat: add launchd plist for daily 6PM scheduler"
```

---

### Task 3: Document Installation Instructions

**Files:**
- Create: `scripts/INSTALL.md`

- [ ] **Step 1: Write installation instructions**

```markdown
# Database Scheduler Installation

## One-Time Setup

1. Copy the plist to LaunchAgents:
   ```bash
   cp scripts/com.stockscreener.db-updater.plist ~/Library/LaunchAgents/
   ```

2. Load the agent:
   ```bash
   launchctl load ~/Library/LaunchAgents/com.stockscreener.db-updater.plist
   ```

## Verify It's Working

Check the next scheduled run:
```bash
launchctl list | grep stockscreener
```

View recent logs:
```bash
tail -f ~/Library/Logs/StockScreener/update_*.log
```

## Commands

- **Start manually:** `launchctl start com.stockscreener.db-updater`
- **Stop:** `launchctl stop com.stockscreener.db-updater`
- **Uninstall:** `launchctl unload ~/Library/LaunchAgents/com.stockscreener.db-updater.plist`
- **Reschedule (after edits):** `launchctl unload ... && launchctl load ...`
```

- [ ] **Step 2: Commit**

```bash
git add scripts/INSTALL.md
git commit -m "docs: add installation instructions for scheduler"
```

---

## Execution

Plan complete and saved to `docs/superpowers/plans/2026-03-21-db-scheduler-plan.md`. Two execution options:

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

**Which approach?**