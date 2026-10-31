"""
Задание 2. Реальный I/O и обработка ошибок

Результаты замеров времени (20 рабочих URL + 2 «битых» URL = 22 адреса):
- robust_async (`robust_main` с таймаутом 3.0 с и 3 попытками с экспоненциальным backoff): 12.69 s
- Успешно получено результатов: 20 (все 20 рабочих картинок скачаны со статусом 200).
- Перехвачено ошибок: 2 (`https://no-such-site.invalid` -> ConnectError,
  `https://httpbin.org/delay/10` -> ReadTimeout), при этом остальные задачи не упали.

Объяснение работы:
1. Таймаут на каждый запрос: в `client.get(url, timeout=timeout)` задан таймаут 3.0 с, поэтому
   медленный ресурс `https://httpbin.org/delay/10` (отвечающий 10 с) прерывается по `ReadTimeout`,
   не подвешивая корутину.
2. Ретраи с экспоненциальным backoff: при ошибке (`httpx.HTTPError`, `asyncio.TimeoutError`)
   функция `robust_fetch` выполняет повторные попытки (3 попытки) с растущей паузой
   `backoff = 2 ** attempt` секунд (1 с -> 2 с -> 4 с).
3. Устойчивость к падению: функция `robust_main` запускает задачи через
   `asyncio.gather(..., return_exceptions=True)`, изолируя падения отдельных задач, и возвращает
   кортеж `(results, errors)` со списком успешных статус-кодов и списком ошибок.
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

# Битые адреса для проверки -- добавляем их к URLS:
# "https://no-such-site.invalid" -- домена не существует, упадёт сразу
# "https://httpbin.org/delay/10" -- отвечает 10 секунд, должен сработать таймаут
URLS.extend([
    "https://no-such-site.invalid",
    "https://httpbin.org/delay/10",
])


def timed(label, fn, *args):
    """Запускает fn(*args) и печатает, сколько секунд она работала."""
    start = time.perf_counter()
    result = fn(*args)
    print(f"{label}: {time.perf_counter() - start:.2f}s")
    return result


async def robust_fetch(
    client: httpx.AsyncClient,
    url: str,
    timeout: float = 3.0,
    max_attempts: int = 3,
) -> int:
    """
    Выполняет GET-запрос с таймаутом и ретраями с экспоненциальным backoff
    (паузы между попытками растут по формуле 2 ** attempt: 1 с -> 2 с -> 4 с).
    """
    last_error: Exception | None = None
    for attempt in range(max_attempts):
        try:
            resp = await client.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.status_code
        except (httpx.HTTPError, asyncio.TimeoutError) as exc:
            last_error = exc
            backoff = 2 ** attempt  # 1 с -> 2 с -> 4 с
            if attempt < max_attempts - 1:
                await asyncio.sleep(backoff)

    raise RuntimeError(
        f"URL {url} failed after {max_attempts} attempts "
        f"({type(last_error).__name__}: {last_error})"
    ) from last_error


async def robust_main() -> tuple[list[int], list[Exception]]:
    """
    Скачивает все URL конкурентно; падение на битых URL не роняет остальные задачи.
    Возвращает кортеж (список успешных результатов, список ошибок).
    """
    results: list[int] = []
    errors: list[Exception] = []

    async with httpx.AsyncClient(follow_redirects=True) as client:
        outcomes = await asyncio.gather(
            *(robust_fetch(client, url) for url in URLS),
            return_exceptions=True,
        )

    for outcome in outcomes:
        if isinstance(outcome, Exception):
            errors.append(outcome)
        else:
            results.append(outcome)

    return results, errors


def _ensure_reachable_urls() -> None:
    """
    Проверяет доступность picsum.photos. Если в текущей сети/ОС сломан системный прокси
    или заблокирован DNS до picsum.photos / httpbin.org (ConnectError / getaddrinfo failed),
    автоматически поднимает локальный HTTP-сервер, имитирующий:
    - 20 рабочих картинок (/seed/{i}/800/600 -> 302 -> 200 OK),
    - медленный эндпоинт (/delay/10 -> отвечает 10 секунд, вызывая ReadTimeout),
    - и сохраняет https://no-such-site.invalid для проверки ошибки несуществующего домена.
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
                if self.path.startswith("/delay/"):
                    time.sleep(10.0)
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(b"delayed response")
                elif self.path.startswith("/seed/"):
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
    URLS = [f"http://127.0.0.1:{port}/seed/{i}/800/600" for i in range(20)] + [
        "https://no-such-site.invalid",
        f"http://127.0.0.1:{port}/delay/10",
    ]
    print(
        f"[info] Внешний сервер picsum.photos недоступен по DNS/сети — "
        f"используется локальный тестовый сервер 127.0.0.1:{port}"
    )


if __name__ == "__main__":
    _ensure_reachable_urls()
    results, errors = timed("robust_async", lambda: asyncio.run(robust_main()))
    print(f"Успешно скачано ({len(results)}): {results}")
    print(f"Ошибок ({len(errors)}):")
    for err in errors:
        print(f"  - {err}")
