"""
Задание 3 (*). Вынос CPU-bound-вычислений

Результаты замеров времени (скачивание 20 картинок + расчёт sha256 от body * 1000):
- Прямой вызов в корутине (`cpu_blocking_main`, «в лоб»): 2.33 s
  Во время скачивания по сети точки `ticker` печатаются (`.....`), но как только начинается
  вычисление `heavy(body)` прямо в корутине — точки полностью замирают (0 точек за время
  расчёта), так как CPU-bound код блокирует единственный поток event loop.
- Вынос в пул процессов (`cpu_main`, `loop.run_in_executor` + `ProcessPoolExecutor`): 1.39 s
  Точки `ticker` равномерно печатаются и во время скачивания (`....`), и во время расчёта
  хешей (`..........`), а общее время выполнения сокращается почти в 2 раза за счёт
  параллельной обработки на нескольких ядрах процессора.

Объяснение:
1. Почему «в лоб» блокирует event loop:
   Асинхронный цикл событий (event loop) работает в одном потоке и использует кооперативную
   многозадачность: переключение между задачами происходит только в точках `await`. Функция
   `heavy(body)` — это чистая синхронная CPU-bound операция без точек переключения. Её прямой
   вызов внутри корутины монополизирует поток на всё время расчёта SHA-256, поэтому корутина
   `ticker()` не получает управления и перестаёт печатать точки.
2. Почему `ProcessPoolExecutor` + `run_in_executor` решает проблему:
   В Python из-за GIL (Global Interpreter Lock) потоки (`ThreadPoolExecutor`) не дают
   параллелизма для CPU-bound кода на чистом Python/байткоде и конкурируют за интерпретатор.
   `ProcessPoolExecutor` запускает отдельные рабочие процессы со своими интерпретаторами Python,
   а `loop.run_in_executor(pool, heavy, body)` возвращает awaitable-объект, позволяя основному
   потоку с event loop не блокироваться и продолжать выполнять `ticker()` каждые 0.1 секунды.
"""

import asyncio
import hashlib
import time
from concurrent.futures import ProcessPoolExecutor

import httpx

# 20 разных картинок (у каждой свой номер в адресе) -- ответы достаточно
# «тяжёлые», чтобы разница между sync и async была хорошо видна
URLS = [f"https://picsum.photos/seed/{i}/800/600" for i in range(20)]


def timed(label, fn, *args):
    """Запускает fn(*args) и печатает, сколько секунд она работала."""
    start = time.perf_counter()
    result = fn(*args)
    print(f"{label}: {time.perf_counter() - start:.2f}s")
    return result


def heavy(body: bytes) -> str:
    # body * 1000 раздувает данные, чтобы хеш считался заметно долго
    return hashlib.sha256(body * 1000).hexdigest()


async def ticker():
    # «пульс» event loop: если точки перестали печататься -- loop заблокирован
    while True:
        print(".", end="", flush=True)
        await asyncio.sleep(0.1)


async def fetch_bytes(client: httpx.AsyncClient, url: str) -> bytes:
    resp = await client.get(url)
    return resp.content


async def cpu_blocking_main() -> list[str]:
    """
    1. Решение «в лоб»: вызываем heavy() прямо в корутине.
    Наглядно видно, что на этапе CPU-расчёта точки ticker перестают печататься.
    """
    ticker_task = asyncio.create_task(ticker())
    try:
        print("[blocking] Скачиваем картинки (точки бегут): ", end="", flush=True)
        async with httpx.AsyncClient(follow_redirects=True) as client:
            bodies = await asyncio.gather(*(fetch_bytes(client, url) for url in URLS))

        print("\n[blocking] Считаем heavy() прямо в корутине (точки ЗАМЕРЛИ): ", end="", flush=True)
        hashes = [heavy(body) for body in bodies]
        print(" готово!")
        return hashes
    finally:
        ticker_task.cancel()
        await asyncio.gather(ticker_task, return_exceptions=True)


async def cpu_main() -> list[str]:
    """
    2. Правильное решение: выносим heavy() в отдельный процесс через
    loop.run_in_executor + ProcessPoolExecutor. Точки ticker продолжают бежать.
    """
    ticker_task = asyncio.create_task(ticker())
    try:
        print("[executor] Скачиваем картинки (точки бегут): ", end="", flush=True)
        async with httpx.AsyncClient(follow_redirects=True) as client:
            bodies = await asyncio.gather(*(fetch_bytes(client, url) for url in URLS))

        print(
            "\n[executor] Считаем heavy() в ProcessPoolExecutor (точки ПРОДОЛЖАЮТ бежать): ",
            end="",
            flush=True,
        )
        loop = asyncio.get_running_loop()
        with ProcessPoolExecutor() as pool:
            hashes = await asyncio.gather(
                *(loop.run_in_executor(pool, heavy, body) for body in bodies)
            )
        print(" готово!")
        return hashes
    finally:
        ticker_task.cancel()
        await asyncio.gather(ticker_task, return_exceptions=True)


if __name__ == "__main__":
    timed("cpu_blocking (в лоб, блокирует event loop)", lambda: asyncio.run(cpu_blocking_main()))
    print()
    timed(
        "cpu_executor (ProcessPoolExecutor, не блокирует event loop)",
        lambda: asyncio.run(cpu_main()),
    )
