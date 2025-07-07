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
    all_books = []

    for q in queries:
        for page in range(1, 3):  # will be more later
            data = search_books(q, page=page)
            for doc in data.get("docs", []):
                title = doc.get("title", "")
                author = ", ".join(doc.get("author_name", []))
                year = doc.get("first_publish_year", "")
                cover = COVERS_URL.format(doc["cover_i"]) if doc.get("cover_i") else ""
                description = doc.get("subtitle", "")
                genres = doc.get("subject", [])
                tags = doc.get("subject", [])
                book_type = "Fiction" if "fiction" in [g.lower() for g in genres] else ""

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

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["title","author","year","image_url","description","genres","tags","type"])
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

    print(f"✅ Saved {len(all_books)} books to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
