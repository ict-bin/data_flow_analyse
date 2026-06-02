from __future__ import annotations

import json
import logging
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable

logger = logging.getLogger("dfa.probe")


class ThreadedProbeServer:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        payload_provider: Callable[[], dict[str, object]],
        health_paths: tuple[str, ...],
        ready_paths: tuple[str, ...],
    ) -> None:
        self._host = host
        self._port = int(port)
        self._payload_provider = payload_provider
        self._health_paths = set(health_paths)
        self._ready_paths = set(ready_paths)
        self._thread: threading.Thread | None = None
        self._httpd: ThreadingHTTPServer | None = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._last_response_at = 0.0
        self._last_error: str | None = None
        self._request_total = 0
        self._request_fail_total = 0
        self._restart_count = 0

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name=f"dfa_probe_{self._port}", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        httpd = self._httpd
        if httpd is not None:
            try:
                httpd.server_close()
            except Exception:
                pass
        thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=1.0)
        self._httpd = None
        self._thread = None

    def diagnostics(self) -> dict[str, object]:
        with self._lock:
            return {
                "probe_thread_alive": bool(self._thread and self._thread.is_alive()),
                "probe_last_response_at": self._last_response_at or None,
                "probe_last_error": self._last_error,
                "probe_restart_count": self._restart_count,
                "probe_request_total": self._request_total,
                "probe_request_fail_total": self._request_fail_total,
            }

    def _record_response(self) -> None:
        with self._lock:
            self._last_response_at = time.time()
            self._request_total += 1

    def _record_error(self, exc: Exception) -> None:
        with self._lock:
            self._last_error = str(exc)
            self._request_total += 1
            self._request_fail_total += 1

    def _run(self) -> None:
        server = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                try:
                    payload = dict(server._payload_provider())
                    if self.path in server._health_paths:
                        code = HTTPStatus.OK if bool(payload.get("liveness_ok")) and not bool(payload.get("shutting_down")) else HTTPStatus.SERVICE_UNAVAILABLE
                    elif self.path in server._ready_paths:
                        code = HTTPStatus.OK if bool(payload.get("readiness_ok")) and not bool(payload.get("shutting_down")) else HTTPStatus.SERVICE_UNAVAILABLE
                    else:
                        code = HTTPStatus.NOT_FOUND
                        payload = {"status": "not_found"}
                    payload.update(server.diagnostics())
                    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                    self.send_response(int(code))
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    server._record_response()
                except Exception as exc:
                    server._record_error(exc)
                    body = json.dumps({"status": "error", "detail": str(exc)}, ensure_ascii=False).encode("utf-8")
                    self.send_response(int(HTTPStatus.SERVICE_UNAVAILABLE))
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)

            def log_message(self, format: str, *args: object) -> None:
                return

        with self._lock:
            self._restart_count += 1
            self._last_error = None
        httpd = ThreadingHTTPServer((self._host, self._port), Handler)
        httpd.daemon_threads = True
        httpd.timeout = 1
        self._httpd = httpd
        logger.info("probe server started host=%s port=%s", self._host, self._port)
        try:
            while not self._stop_event.is_set():
                httpd.handle_request()
        except Exception as exc:
            with self._lock:
                self._last_error = str(exc)
            logger.warning("probe server stopped with error: %s", exc)
        finally:
            try:
                httpd.server_close()
            except Exception:
                pass
            logger.info("probe server stopped host=%s port=%s", self._host, self._port)
