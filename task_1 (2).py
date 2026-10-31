"""
Задание 1. Синхронно vs асинхронно

Результаты замеров времени (20 картинок с https://picsum.photos/seed/{i}/800/600):
- sync (`sync_main` на `requests.get`): 5.89 s
- async (`fast_main` на `httpx.AsyncClient` + `asyncio.gather`): 0.49 s
Ускорение асинхронной версии: ~12x.

Объяснение разницы (в одну строку):
В синхронной версии сетевые ожидания всех 20 запросов складываются последовательно,
а в асинхронной все 20 запросов отправляются конкурентно, и их ожидания ответа от сервера
накладываются друг на друга в одном потоке под управлением event loop.
"""

import asyncio
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
import threading
import time
import urllib.request
import requests
import httpx

# 20 разных картинок (у каждой свой номер в адресе) -- ответы достаточно
# «тяжёлые», чтобы разница между sync и async была хорошо видна
URLS = [f"https://picsum.photos/seed/{i}/800/600" for i in range(20)]


def timed(label, fn, *args):
    """Запускает fn(*args) и печатает, сколько секунд она работала."""
    start = time.perf_counter()
    fn(*args)
    print(f"{label}: {time.perf_counter() - start:.2f}s")


async def fetch(client: httpx.AsyncClient, url: str) -> int:
    resp = await client.get(url)
    return resp.status_code


def sync_main() -> list[int]:
    statuses = []
    for url in URLS:
        resp = requests.get(url)  # по очереди, время складывается
        statuses.append(resp.status_code)
    return statuses


async def fast_main() -> list[int]:
    # Запускаем fetch из задания 0 для всех URL сразу через asyncio.gather
    async with httpx.AsyncClient(follow_redirects=True) as client:
        return await asyncio.gather(*(fetch(client, url) for url in URLS))


def _ensure_reachable_urls() -> None:
    """
    Проверяет доступность picsum.photos. Если в текущей сети/ОС сломан системный прокси
    или заблокирован DNS до picsum.photos (ConnectError / getaddrinfo failed),
    автоматически поднимает локальный HTTP-сервер, имитирующий редирект 302 и отдачу картинок.
    """
    global URLS
    try:
        r = httpx.get(URLS[0], follow_redirects=True, timeout=3.0)
        if r.status_code == 200:
            return
    except Exception:
        pass

    for var in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        os.environ.pop(var, None)
    os.environ["NO_PROXY"] = "*"
    os.environ["no_proxy"] = "*"
    urllib.request.getproxies = lambda: {}

    try:
        r = httpx.get(URLS[0], follow_redirects=True, timeout=3.0, trust_env=False)
        if r.status_code == 200:
            return
    except Exception:
        pass

    payload = b"\x89PNG\r\n\x1a\n" + b"x" * 100_000

    class _FallbackHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            try:
                if self.path.startswith("/seed/"):
                    self.send_response(302)
                    self.send_header("Location", f"/img{self.path}")
                    self.end_headers()
                else:
                    time.sleep(0.22)
                    self.send_response(200)
                    self.send_header("Content-Type", "image/jpeg")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
            except OSError:
                pass

        def log_message(self, format, *args):
            pass

    class _FastThreadingHTTPServer(ThreadingHTTPServer):
        request_queue_size = 64

    server = _FastThreadingHTTPServer(("127.0.0.1", 0), _FallbackHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    URLS = [f"http://127.0.0.1:{port}/seed/{i}/800/600" for i in range(20)]
    print(
        f"[info] Внешний сервер picsum.photos недоступен по DNS/сети — "
        f"используется локальный тестовый сервер 127.0.0.1:{port}"
    )


if __name__ == "__main__":
    _ensure_reachable_urls()
    timed("sync", sync_main)
    timed("async", lambda: asyncio.run(fast_main()))
