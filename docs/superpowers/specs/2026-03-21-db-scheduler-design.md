# Database Scheduler Design

## Overview
A meta-script that automatically runs all four database update scripts every day at 6PM using macOS's native launchd scheduler, with retry logic and macOS notifications.

## Components

### 1. Wrapper Script (`run_db_updates.sh`)
- Shell script that executes the four Python scripts sequentially
- Handles retry logic (if a script fails, retry once, then stop)
- Sends macOS notification on completion (success or failure)
- Logs output to a timestamped log file

### 2. Launch Agent Plist (`com.stockscreener.db-updater.plist`)
- Lives in `~/Library/LaunchAgents/`
- Tells launchd to run the wrapper script daily at 6PM
- Keeps the job running in the background automatically

### 3. Log Directory
- Logs stored in `~/Library/Logs/StockScreener/`
- One log file per run: `update_YYYY-MM-DD_HH-MM-SS.log`

## How It Works

1. User installs the plist once with `launchctl load`
2. Every day at 6PM, launchd wakes up the job
3. Wrapper script runs the 4 scripts in order:
   - `sp1500_database_technical.py`
   - `sp1500_database_metadata.py`
   - `sp1500_database_fundamental_qtrly.py`
   - `sp1500_database_fundamental_yearly.py`
4. If any fails → retry once → if still fails, stop and notify failure
5. When all done → notify success with summary
6. Logs are saved for debugging

## Error Handling
- **Retry:** Failed script retried exactly once
- **Stop condition:** If retry also fails, abort remaining scripts
- **Notification:** Always notifies user of final result (success or failure)

## Dependencies
- Python 3.x with existing virtual environment
- macOS (uses launchd)
- `terminal-notifier` (or `osascript` for notifications)