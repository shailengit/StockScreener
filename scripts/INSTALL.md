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
- **Reschedule (after edits):** `launchctl unload ~/Library/LaunchAgents/com.stockscreener.db-updater.plist && launchctl load ~/Library/LaunchAgents/com.stockscreener.db-updater.plist`