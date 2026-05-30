"""
aiohttp server — HTTP + WebSocket endpoints.

Endpoints:
  GET  /ping          Health check → { status: 'ok', version: '1.0.0' }
  GET  /info          Kernel info  → { python_version, cwd, platform, ... }
  GET  /ws            WebSocket — single persistent connection for the extension
  POST /restart       Restart the kernel → { status: 'restarting' }
  POST /interrupt     Interrupt the kernel → { status: 'interrupted' }

CORS:
  Only chrome-extension:// and ms-browser-extension:// origins accepted.
  localhost http:// origins also allowed (for dev/testing).
"""
import asyncio
import json
import logging
import sys
from aiohttp import web, WSMsgType

from .kernel_bridge import KernelBridge, get_server_info

logger = logging.getLogger(__name__)

_ALLOWED_ORIGIN_PREFIXES = (
    "chrome-extension://",
    "ms-browser-extension://",
    "http://localhost",
    "http://127.0.0.1",
)


def _is_allowed_origin(origin: str | None) -> bool:
    if origin is None:
        return True  # Same-origin or non-browser client
    return any(origin.startswith(p) for p in _ALLOWED_ORIGIN_PREFIXES)


@web.middleware
async def cors_middleware(request: web.Request, handler):
    origin = request.headers.get("Origin")
    if not _is_allowed_origin(origin):
        raise web.HTTPForbidden(reason="Origin not allowed")

    if request.method == "OPTIONS":
        # Preflight
        resp = web.Response()
    else:
        resp = await handler(request)

    if origin:
        resp.headers["Access-Control-Allow-Origin"] = origin
        resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return resp


# ── HTTP handlers ─────────────────────────────────────────────────────────────

async def handle_ping(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok", "version": "1.0.0", "server": "pynoteflow"})


async def handle_info(request: web.Request) -> web.Response:
    bridge: KernelBridge = request.app["bridge"]
    info = get_server_info()
    try:
        ka = bridge.km.is_alive() if bridge.km is not None else False
        if asyncio.iscoroutine(ka):
            ka = await ka
        info["kernel_alive"] = bool(ka)
    except Exception:
        info["kernel_alive"] = False
    return web.json_response(info)


async def handle_restart(request: web.Request) -> web.Response:
    bridge: KernelBridge = request.app["bridge"]
    try:
        await bridge.restart()
        return web.json_response({"status": "ok"})
    except Exception as exc:
        return web.json_response({"status": "error", "message": str(exc)}, status=500)


async def handle_interrupt(request: web.Request) -> web.Response:
    bridge: KernelBridge = request.app["bridge"]
    await bridge.interrupt()
    return web.json_response({"status": "ok"})


# ── WebSocket handler ─────────────────────────────────────────────────────────

async def handle_ws(request: web.Request) -> web.WebSocketResponse:
    origin = request.headers.get("Origin")
    if not _is_allowed_origin(origin):
        raise web.HTTPForbidden(reason="Origin not allowed")

    bridge: KernelBridge = request.app["bridge"]
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)

    # Only one client at a time
    old_ws = bridge._ws
    if old_ws and not old_ws.closed:
        await old_ws.close(code=1001, message=b"New client connected")
    bridge.attach_ws(ws)

    # Send initial info
    info = get_server_info()
    info["type"] = "info"
    try:
        ka = bridge.km.is_alive() if bridge.km is not None else False
        if asyncio.iscoroutine(ka):
            ka = await ka
        info["kernel_alive"] = bool(ka)
    except Exception:
        info["kernel_alive"] = False
    await ws.send_json(info)

    logger.info("WebSocket client connected (origin=%s)", origin)

    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    await _dispatch(bridge, ws, data)
                except json.JSONDecodeError:
                    await ws.send_json({"type": "error", "msg_id": None, "ename": "ParseError",
                                        "evalue": "Invalid JSON", "traceback": []})
            elif msg.type in (WSMsgType.ERROR, WSMsgType.CLOSE):
                break
    finally:
        bridge.detach_ws()
        logger.info("WebSocket client disconnected")

    return ws


async def _dispatch(bridge: KernelBridge, ws, data: dict) -> None:
    t = data.get("type")

    if t == "execute":
        code = data.get("code", "")
        msg_id = data.get("msg_id", "")
        if not msg_id:
            await ws.send_json({"type": "error", "msg_id": None, "ename": "ProtocolError",
                                "evalue": "msg_id required", "traceback": []})
            return
        bridge.execute(code, msg_id)

    elif t == "interrupt":
        await bridge.interrupt()

    elif t == "restart":
        try:
            await bridge.restart()
            await ws.send_json({"type": "restart_reply", "status": "ok"})
        except Exception as exc:
            await ws.send_json({"type": "restart_reply", "status": "error", "message": str(exc)})

    elif t == "input_reply":
        bridge.input_reply(data.get("value", ""))

    elif t == "complete":
        code = data.get("code", "")
        cursor_pos = data.get("cursor_pos", len(code))
        msg_id = data.get("msg_id", "")
        await bridge.complete(code, cursor_pos, msg_id)

    elif t == "ping":
        await ws.send_json({"type": "pong"})


# ── App factory & runner ──────────────────────────────────────────────────────

async def create_app() -> web.Application:
    app = web.Application(middlewares=[cors_middleware])
    bridge = KernelBridge()
    app["bridge"] = bridge

    app.router.add_get("/ping", handle_ping)
    app.router.add_get("/info", handle_info)
    app.router.add_get("/ws", handle_ws)
    app.router.add_post("/restart", handle_restart)
    app.router.add_post("/interrupt", handle_interrupt)
    # Preflight
    app.router.add_route("OPTIONS", "/{path_info:.*}", lambda r: web.Response())

    async def on_startup(app):
        await app["bridge"].start()

    async def on_shutdown(app):
        await app["bridge"].stop()

    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    return app


async def run_server(host: str = "localhost", port: int = 5891) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )
    app = await create_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()

    print("=" * 60)
    print(f"  PyNoteFlow Server  v1.0.0")
    print(f"  Listening on  http://{host}:{port}")
    print(f"  Python        {sys.version.split()[0]}")
    print()
    print("  Open PyNoteFlow and click  [Connect Kernel]")
    print("  then choose  'PyNoteFlow Server (localhost)'")
    print()
    print("  Press  Ctrl+C  to stop.")
    print("=" * 60)

    # Keep running until interrupted
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        await runner.cleanup()
