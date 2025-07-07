import requests
import csv
import time

BASE_SEARCH = "https://openlibrary.org/search.json"
COVERS_URL = "https://covers.openlibrary.org/b/id/{}-L.jpg"
OUTPUT_CSV = "books.csv"

HEADERS = {
    "User-Agent": "RecommendationSystemBot/1.0 (your_email@example.com)"
}

def search_books(query, page=1, limit=20):
    params = {"q": query, "page": page, "limit": limit}
    resp = requests.get(BASE_SEARCH, params=params, headers=HEADERS)
    resp.raise_for_status()
    return resp.json()

def main():
    queries = ["classic literature", "fiction", "science fiction"]
    seen = set()   # для фильтрации дубликатов (title, author)
    all_books = []

    for q in queries:
        for page in range(1, 3):  # парсим по 2 страницы на запрос
            
            data = search_books(q, page=page)
            
            for doc in data.get("docs", []):
                title = doc.get("title", "").strip()
                authors = doc.get("author_name", [])
                author = authors[0].strip() if authors else ""
                year = doc.get("first_publish_year") or 0

                # фильтры
                if year < 1000:
                    continue
                subjects = doc.get("subject", [])
                if not subjects:
                    continue
                key = (title.lower(), author.lower())
                if key in seen:
                    continue
                seen.add(key)

                # формируем поля
                cover = ""
                if doc.get("cover_i"):
                    cover = COVERS_URL.format(doc["cover_i"])
                description = doc.get("subtitle", "").strip()
                genres = subjects[:]   # OpenLibrary не разделяет явно жанры/теги
                tags   = subjects[:]
                book_type = "Fiction" if any("fiction" in s.lower() for s in subjects) else ""

                all_books.append({
                    "title": title,
                    "author": author,
                    "year": year,
                    "image_url": cover,
                    "description": description,
                    "genres": genres,
                    "tags": tags,
                    "type": book_type
                })
            time.sleep(1)

    # пишем CSV
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "title","author","year","image_url","description","genres","tags","type"
        ])
        for b in all_books:
            writer.writerow([
                b["title"],
                b["author"],
                b["year"],
                b["image_url"],
                b["description"],
                ";".join(b["genres"]),
                ";".join(b["tags"]),
                b["type"]
            ])

    print(f"✅ Saved {len(all_books)} unique books with subjects to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
