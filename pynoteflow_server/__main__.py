"""
PyNoteFlow Local Kernel Server
──────────────────────────────
Run with:
    python -m pynoteflow_server          # default port 5891
    python -m pynoteflow_server --port 5892

Or after pip install:
    pynoteflow-server
    pynoteflow-server --port 5892
"""
import argparse
import asyncio
import sys


def main():
    parser = argparse.ArgumentParser(
        description="PyNoteFlow local kernel server"
    )
    parser.add_argument(
        "--port", type=int, default=5891,
        help="Port to listen on (default: 5891)"
    )
    parser.add_argument(
        "--host", default="localhost",
        help="Host to bind to (default: localhost)"
    )
    parser.add_argument(
        "--no-browser", action="store_true",
        help="Do not open PyNoteFlow in the browser on start"
    )
    args = parser.parse_args()

    # Windows: ZMQ requires SelectorEventLoop; Python 3.8+ defaults to Proactor.
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Silent mode when started at login via startup registration
    if args.no_browser:
        import logging, os
        logging.disable(logging.CRITICAL)
        # Redirect stdout/stderr to null so no console window content is produced
        sys.stdout = open(os.devnull, 'w')
        sys.stderr = open(os.devnull, 'w')

    # Deferred import so startup is fast
    from .server import run_server
    try:
        asyncio.run(run_server(host=args.host, port=args.port))
    except KeyboardInterrupt:
        print("\nPyNoteFlow Server stopped.")
        sys.exit(0)


if __name__ == "__main__":
    main()
