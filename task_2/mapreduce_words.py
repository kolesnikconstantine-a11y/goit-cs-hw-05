from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
import re
import string

import matplotlib.pyplot as plt
import requests


def get_text(url: str) -> str | None:
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Перевірка на помилки HTTP
        return response.text
    except requests.RequestException as e:
        print(f"Помилка завантаження: {e}")
        return None


def clean_text(text: str) -> list[str]:
    """Видаляє пунктуацію та переводить слова у нижній регістр."""
    text_cleaned = text.translate(str.maketrans("", "", string.punctuation))
    words = re.findall(r"\b\w+\b", text_cleaned.lower())
    return words


def map_function(word: str) -> tuple[str, int]:
    return word, 1


def shuffle_function(mapped_values: list[tuple[str, int]]):
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()


def reduce_function(key_values: tuple[str, list[int]]) -> tuple[str, int]:
    key, values = key_values
    return key, sum(values)


# Виконання MapReduce
def map_reduce(text: str) -> dict[str, int]:
    # Очищаємо текст перед мапінгом
    words = clean_text(text)

    # Паралельний Мапінг
    with ThreadPoolExecutor() as executor:
        mapped_values = list(executor.map(map_function, words))

    # Крок 2: Shuffle
    shuffled_values = shuffle_function(mapped_values)

    # Паралельна Редукція
    with ThreadPoolExecutor() as executor:
        reduced_results = list(executor.map(reduce_function, shuffled_values))

    return dict(reduced_results)


def visualize_top_words(word_counts: dict[str, int], top_n: int = 10) -> None:
    """Побудова діаграми для найчастіше вживаних слів."""
    if not word_counts:
        print("Немає даних для візуалізації.")
        return

    # Отримуємо топ-N слів
    top_words = Counter(word_counts).most_common(top_n)
    words, counts = zip(*reversed(top_words))

    plt.figure(figsize=(10, 6))
    bars = plt.barh(words, counts, color="skyblue", edgecolor="navy")

    # Додавання точних значень біля кожної смуги
    for bar in bars:
        width = bar.get_width()
        plt.text(
            width + (max(counts) * 0.01),
            bar.get_y() + bar.get_height() / 2,
            f"{int(width)}",
            ha="left",
            va="center",
            fontsize=10,
        )

    plt.xlabel("Частота використання", fontsize=12)
    plt.ylabel("Слова", fontsize=12)
    plt.title(f"Топ {top_n} найчастіше вживаних слів у тексті", fontsize=14, fontweight="bold")
    plt.grid(axis="x", linestyle="--", alpha=0.7)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    # Вхідний текст для обробки (Project Gutenberg)

    url = "https://gutenberg.net.au/ebooks/m00019.txt"

    
    print("Завантаження тексту...")
    text = get_text(url)
    
    if text:
        print("Обробка MapReduce...")
        result = map_reduce(text)

        print("Побудова діаграми...")
        visualize_top_words(result, top_n=10)
    else:
        print("Помилка: Не вдалося отримати вхідний текст.")