# Database Scheduler Installation

## One-Time Setup (using LaunchAgent)

1. Copy the plist to LaunchAgents:
   ```bash
   cp scripts/com.stockscreener.db-updater.plist ~/Library/LaunchAgents/
   ```

2. Load the agent:
   ```bash
   launchctl load ~/Library/LaunchAgents/com.stockscreener.db-updater.plist
   ```

## Verify It's Working

Check if loaded:
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

## Notes

- Runs daily at 6PM
- Uses osascript to run with full user permissions (bypasses macOS sandbox restrictions)
- Logs saved to ~/Library/Logs/StockScreener/
- You'll receive macOS notifications on completion