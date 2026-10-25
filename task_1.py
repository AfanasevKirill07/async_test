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
import time

import httpx
import requests

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


if __name__ == "__main__":
    timed("sync", sync_main)
    timed("async", lambda: asyncio.run(fast_main()))
