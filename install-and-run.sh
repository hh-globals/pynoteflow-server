#!/usr/bin/env bash
# PyNoteFlow — One-click installer & launcher for macOS / Linux
# Run this in a terminal:
#   curl -LsSf https://raw.githubusercontent.com/hh-globals/pynoteflow-server/main/install-and-run.sh | bash
#
# What it does:
#   1. Installs 'uv' (fast Python tool runner) if not already installed
#   2. Installs pynoteflow-server via 'uv tool install'
#   3. Registers the server to start silently at login (launchd / autostart)
#   4. Starts the server now (first time)

set -e

echo ""
echo "======================================================"
echo "  PyNoteFlow — Installer & Launcher"
echo "======================================================"
echo ""

# ── Step 1: Install uv if missing ─────────────────────────────────────────────
if ! command -v uv &>/dev/null; then
    echo "[1/4] Installing uv (Python tool runner)..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
    if ! command -v uv &>/dev/null; then
        echo "  uv installed. Please restart your terminal and run this script again."
        exit 0
    fi
    echo "  uv installed OK"
else
    echo "[1/4] uv already installed — OK"
fi

UV_BIN="$(command -v uv)"

# ── Step 2: Install / upgrade pynoteflow-server ────────────────────────────────
echo ""
echo "[2/4] Installing pynoteflow-server..."
uv tool install git+https://github.com/hh-globals/pynoteflow-server --force-reinstall
echo "  pynoteflow-server installed OK"

# ── Step 3: Register auto-start at login ──────────────────────────────────────
echo ""
echo "[3/4] Registering auto-start at login..."

OS="$(uname -s)"

if [ "$OS" = "Darwin" ]; then
    # macOS — launchd user agent
    PLIST_DIR="$HOME/Library/LaunchAgents"
    PLIST_FILE="$PLIST_DIR/com.hhglobals.pynoteflow-server.plist"
    mkdir -p "$PLIST_DIR"
    cat > "$PLIST_FILE" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>             <string>com.hhglobals.pynoteflow-server</string>
  <key>ProgramArguments</key>
  <array>
    <string>$UV_BIN</string>
    <string>tool</string>
    <string>run</string>
    <string>pynoteflow-server</string>
    <string>--no-browser</string>
  </array>
  <key>RunAtLoad</key>         <true/>
  <key>KeepAlive</key>         <false/>
  <key>StandardOutPath</key>   <string>/tmp/pynoteflow-server.log</string>
  <key>StandardErrorPath</key> <string>/tmp/pynoteflow-server.log</string>
</dict>
</plist>
PLIST
    launchctl load "$PLIST_FILE" 2>/dev/null || true
    echo "  Auto-start registered via launchd (macOS)"

else
    # Linux — XDG autostart .desktop entry
    AUTOSTART_DIR="$HOME/.config/autostart"
    DESKTOP_FILE="$AUTOSTART_DIR/pynoteflow-server.desktop"
    mkdir -p "$AUTOSTART_DIR"
    cat > "$DESKTOP_FILE" <<DESKTOP
[Desktop Entry]
Type=Application
Name=PyNoteFlow Server
Comment=Starts the PyNoteFlow local kernel server at login
Exec=$UV_BIN tool run pynoteflow-server --no-browser
Hidden=false
NoDisplay=false
X-GNOME-Autostart-enabled=true
DESKTOP
    echo "  Auto-start registered via XDG autostart (Linux)"
fi

echo "  Server will start silently at every login from now on."

# ── Step 4: Start the server now ──────────────────────────────────────────────
echo ""
echo "[4/4] Starting PyNoteFlow Server on localhost:5891..."
echo ""
echo "  Open PyNoteFlow in Chrome/Edge — it will connect automatically."
echo "  From now on the server starts silently at every login."
echo ""
echo "  Press Ctrl+C to stop the server (it will restart next login)."
echo ""

uv tool run pynoteflow-server
