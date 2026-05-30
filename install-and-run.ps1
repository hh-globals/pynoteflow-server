# PyNoteFlow — One-click installer & launcher for Windows
# Run this in PowerShell:
#   irm https://raw.githubusercontent.com/hh-globals/pynoteflow-server/main/install-and-run.ps1 | iex
#
# What it does:
#   1. Installs 'uv' (fast Python tool runner) if not already installed
#   2. Installs pynoteflow-server via 'uv tool install'
#   3. Registers the server to start silently at Windows login
#   4. Starts the server now (first time)

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "  PyNoteFlow — Installer & Launcher" -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ""

# ── Step 1: Install uv if missing ─────────────────────────────────────────────
if (-not (Get-Command "uv" -ErrorAction SilentlyContinue)) {
    Write-Host "[1/4] Installing uv (Python tool runner)..." -ForegroundColor Yellow
    try {
        Invoke-RestMethod https://astral.sh/uv/install.ps1 | Invoke-Expression
        $env:PATH = [System.Environment]::GetEnvironmentVariable("PATH", "User") + ";" + $env:PATH
    } catch {
        Write-Host "  ERROR: Could not install uv automatically." -ForegroundColor Red
        Write-Host "  Please install it manually: https://docs.astral.sh/uv/getting-started/installation/" -ForegroundColor Red
        exit 1
    }
    if (-not (Get-Command "uv" -ErrorAction SilentlyContinue)) {
        Write-Host "  uv installed. Please RESTART this terminal and run the script again." -ForegroundColor Yellow
        exit 0
    }
    Write-Host "  uv installed OK" -ForegroundColor Green
} else {
    Write-Host "[1/4] uv already installed — OK" -ForegroundColor Green
}

# ── Step 2: Install / upgrade pynoteflow-server ────────────────────────────────
Write-Host ""
Write-Host "[2/4] Installing pynoteflow-server..." -ForegroundColor Yellow
uv tool install git+https://github.com/hh-globals/pynoteflow-server --force-reinstall
if ($LASTEXITCODE -ne 0) {
    Write-Host "  ERROR: Installation failed." -ForegroundColor Red
    exit 1
}
Write-Host "  pynoteflow-server installed OK" -ForegroundColor Green

# ── Step 3: Register silent auto-start at login ───────────────────────────────
Write-Host ""
Write-Host "[3/4] Registering auto-start at Windows login..." -ForegroundColor Yellow

# Locate uv executable
$uvPath = (Get-Command "uv").Source

# Create a hidden-window VBScript launcher so no terminal flashes at login
$launcherDir  = "$env:APPDATA\PyNoteFlow"
$launcherPath = "$launcherDir\start-server.vbs"
New-Item -ItemType Directory -Path $launcherDir -Force | Out-Null
$vbsContent = @"
Set WshShell = CreateObject("WScript.Shell")
WshShell.Run """$uvPath"" tool run pynoteflow-server --no-browser", 0, False
"@
Set-Content -Path $launcherPath -Value $vbsContent -Encoding UTF8

# Register in HKCU Run (no admin required)
Set-ItemProperty `
    -Path  "HKCU:\Software\Microsoft\Windows\CurrentVersion\Run" `
    -Name  "PyNoteFlowServer" `
    -Value "wscript.exe ""$launcherPath"""

Write-Host "  Auto-start registered — server will launch silently at every login" -ForegroundColor Green

# ── Step 4: Start the server now ──────────────────────────────────────────────
Write-Host ""
Write-Host "[4/4] Starting PyNoteFlow Server on localhost:5891..." -ForegroundColor Yellow
Write-Host ""
Write-Host "  Open PyNoteFlow in Chrome/Edge — it will connect automatically." -ForegroundColor Cyan
Write-Host "  From now on the server starts silently at every Windows login." -ForegroundColor Cyan
Write-Host ""
Write-Host "  Press Ctrl+C to stop the server (it will restart next login)." -ForegroundColor Gray
Write-Host ""

uv tool run pynoteflow-server
