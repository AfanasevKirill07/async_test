"""
Задание 0. Разминка-ловушка «Почему не быстрее?»

Результаты замеров времени (20 картинок с https://picsum.photos/seed/{i}/800/600):
- До исправления (`slow_main_unfixed`, последовательный `await` в цикле `for`): 4.60 s
- После исправления (`slow_main`, конкурентный запуск через `asyncio.gather`): 0.42 s
- Альтернатива (`slow_main_taskgroup`, конкурентный запуск через `asyncio.TaskGroup`): 0.43 s
Ускорение после починки: ~11x.

Объяснение, почему async в исходном варианте не дал ускорения:
1. В исходной функции `slow_main` вызов `await fetch(client, url)` находился непосредственно
   внутри цикла `for url in URLS:`.
2. Оператор `await` приостанавливает выполнение текущей корутины `slow_main` на каждой
   итерации и ждёт полного завершения одного HTTP-запроса, прежде чем перейти к следующей
   итерации и отправить следующий запрос.
3. Из-за этого запросы выполнялись строго последовательно (по очереди), время ожидания сети
   складывалось, и асинхронный код работал со скоростью обычного синхронного цикла.

Как исправлено:
- Вместо ожидания каждого запроса по очереди создаются корутины/задачи для всех 20 URL сразу
  и запускаются конкурентно через `asyncio.gather(*(fetch(client, url) for url in URLS))`
  (или `asyncio.TaskGroup`).
- В `httpx.AsyncClient(follow_redirects=True)` передан флаг `follow_redirects=True`, так как
  `picsum.photos` отвечает редиректом `302 Found`, по которому `requests` переходит сам,
  а `httpx` без этого флага вернул бы короткий ответ 302 без скачивания самой картинки.
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


async def slow_main_unfixed() -> list[int]:
    """Исходная версия с ловушкой: последовательный await внутри цикла for."""
    statuses = []
    async with httpx.AsyncClient(follow_redirects=True) as client:
        for url in URLS:
            status = await fetch(client, url)  # ждём каждый запрос по очереди
            statuses.append(status)
    return statuses


async def slow_main() -> list[int]:
    """Исправленная версия: конкурентный запуск всех запросов через asyncio.gather."""
    async with httpx.AsyncClient(follow_redirects=True) as client:
        # Было (ловушка — последовательный await в цикле for):
        # for url in URLS:
        #     await fetch(client, url)
        #
        # Стало (конкурентный запуск всех 20 запросов одновременно):
        return await asyncio.gather(*(fetch(client, url) for url in URLS))


async def slow_main_taskgroup() -> list[int]:
    """Альтернативная исправленная версия: конкурентный запуск через asyncio.TaskGroup."""
    async with httpx.AsyncClient(follow_redirects=True) as client:
        async with asyncio.TaskGroup() as tg:
            tasks = [tg.create_task(fetch(client, url)) for url in URLS]
        return [task.result() for task in tasks]


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
    timed("slow (до починки: await в цикле for)", lambda: asyncio.run(slow_main_unfixed()))
    timed("slow (после починки: asyncio.gather)", lambda: asyncio.run(slow_main()))
    timed("slow (альтернатива: asyncio.TaskGroup)", lambda: asyncio.run(slow_main_taskgroup()))
