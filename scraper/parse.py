from selenium import webdriver
from bs4 import BeautifulSoup
import csv
import time

def parse(num):
    with open("books.csv", "a", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        
        options = webdriver.EdgeOptions()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        driver = webdriver.Edge(options=options)
        try:
            url = f"https://thegreatestbooks.org/books/{num}"
            driver.get(url)
            time.sleep(5)  # Wait for page to load

            if "rails-default-error-page" in driver.page_source:
                print(f"Skipping book {num} - error page encountered")
                return

            soup = BeautifulSoup(driver.page_source, "html.parser")

            title = soup.find("h1").find("a", class_="no-underline-link").text.strip()

            author = (
                soup.find("h1").find_all("a", class_="no-underline-link")[1].text.strip()
            )

            year = soup.find("dt", string="Published").find_next_sibling("dd").text.strip()

            image_url = soup.find("div", class_="large-book-image").find("img")["src"]

            description = soup.find("div", class_="book-list-item").find("p").text.strip()

            book_type = soup.find("dt", string="Type").find_next_sibling("dd").text.strip()

            sidebar = soup.find("div", class_="sidebar-content")

            genres = []
            subjects = []

            sections = sidebar.find_all(recursive=False)

            genres_html = sections[5]

            subjects_html = sections[9]

            for genre in genres_html.find_all("a"):
                genres.append(genre.text.strip())

            for subject in subjects_html.find_all("a"):
                subjects.append(subject.text.strip())

            writer.writerow(
                [
                    title,
                    author,
                    year,
                    image_url,
                    description,
                    ", ".join(genres),
                    ", ".join(subjects),
                    book_type,
                ]
            )

        finally:
            driver.quit()

if __name__ == "__main__":
    parse()