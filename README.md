# PyNoteFlow Server

Lightweight local kernel server for the [PyNoteFlow](https://hh-globals.com) Chrome/Edge extension.

Run a real Python kernel on your own machine — PyNoteFlow connects with one click, no URL or token needed.

---

## Install

```bash
pip install pynoteflow-server
```

## Run

```bash
pynoteflow-server          # default port 5891
pynoteflow-server --port 5892
python -m pynoteflow_server
```

## Connect from PyNoteFlow

1. Open PyNoteFlow in Chrome or Edge
2. Click **⚡ Connect Kernel** in the ribbon
3. Select **PyNoteFlow Server (localhost)** — detected automatically
4. Done — full local Python with GPU support, unlimited packages, system terminal

---

## What it does

- Starts an IPython kernel on your machine
- Exposes it via a lightweight custom WebSocket protocol on `localhost:5891`
- CORS locked to `chrome-extension://` and `ms-browser-extension://` origins — nothing reachable from the internet
- Supports: execute, interrupt, restart, `input()`, autocomplete, rich outputs (charts, DataFrames, HTML, LaTeX)
- GPU detection activates automatically — PyNoteFlow reads your CUDA/PyTorch/TF/JAX setup

## Requirements

- Python 3.9+
- `aiohttp`, `jupyter-client`, `ipykernel` (installed automatically)

## Developer

[H&H Globals](https://hh-globals.com) — contact@hh-globals.com
