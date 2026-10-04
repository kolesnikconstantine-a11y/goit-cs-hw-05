import argparse
import asyncio
import logging
import os
from pathlib import Path

import aiofiles
from aiopath import AsyncPath

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("file_sorter.log", encoding="utf-8"),
    ],
)


async def copy_file(
    file_path: AsyncPath, output_folder: AsyncPath, semaphore: asyncio.Semaphore
) -> None:
    """
    Копіює файл у відповідну підпапку цільової директорії на основі його розширення.
    Використовує семафор для обмеження одночасних операцій.
    """
    async with semaphore:
        try:
            # Отримуємо розширення файлу без крапки (або 'no_extension', якщо воно відсутнє)
            extension = file_path.suffix[1:].lower() if file_path.suffix else "no_extension"
            
            # Створюємо цільову підпапку
            target_dir = output_folder / extension
            await target_dir.mkdir(parents=True, exist_ok=True)
            
            target_file_path = target_dir / file_path.name
            
            # Асинхронне читання та запис блоками по 1 МБ
            async with aiofiles.open(file_path, "rb") as src, aiofiles.open(target_file_path, "wb") as dst:
                while chunk := await src.read(1024 * 1024):
                    await dst.write(chunk)
                    
            logging.info("Успішно скопійовано: %s -> %s", file_path, target_file_path)

        except Exception:
            logging.exception("Помилка при копіюванні файлу %s", file_path)


async def read_folder(
    source_folder: AsyncPath, output_folder: AsyncPath, max_tasks: int = 10
) -> None:
    """
    Рекурсивно читає всі файли у вихідній папці та запускає асинхронне копіювання.
    """
    semaphore = asyncio.Semaphore(max_tasks)
    try:
        tasks = []
        source_path = Path(source_folder)

        # Отримання списку файлів у фоновому потоці (запобігає блокуванню event loop)
        def get_all_files():
            files = []
            for root, _, filenames in os.walk(source_path):
                for filename in filenames:
                    files.append(AsyncPath(root) / filename)
            return files

        files = await asyncio.to_thread(get_all_files)

        for file_entry in files:
            # Створення асинхронного завдання із передачею семафора
            task = asyncio.create_task(
                copy_file(file_entry, output_folder, semaphore)
            )
            tasks.append(task)

        if tasks:
            await asyncio.gather(*tasks)
            logging.info("Опрацьовано файлів: %d", len(tasks))
        else:
            logging.info("У вихідній папці не знайдено файлів для копіювання.")

    except Exception:
        logging.exception("Помилка під час читання директорії %s", source_folder)


def parse_args() -> argparse.Namespace:
    """
    Обробляє аргументи командного рядка.
    """
    parser = argparse.ArgumentParser(
        description="Асинхронний сортувальник файлів за розширенням."
    )
    parser.add_argument(
        "-s",
        "--source",
        type=str,
        required=True,
        help="Шлях до вихідної папки (source folder)",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        required=True,
        help="Шлях до цільової папки (output folder)",
    )
    parser.add_argument(
        "-m",
        "--max-tasks",
        type=int,
        default=10,
        help="Максимальна кількість одночасних завдань копіювання (за замовчуванням: 10)",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()

    source_path = AsyncPath(args.source)
    output_path = AsyncPath(args.output)

    if not await source_path.exists():
        logging.error("Вихідна папка не існує: %s", source_path)
        return

    if not await source_path.is_dir():
        logging.error("Вказаний вихідний шлях не є директорією: %s", source_path)
        return

    logging.info("Початок сортування з '%s' до '%s'...", source_path, output_path)
    await read_folder(source_path, output_path, args.max_tasks)
    logging.info("Завершено сортування файлів.")


if __name__ == "__main__":
    asyncio.run(main())