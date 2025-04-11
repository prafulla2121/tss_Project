from requests_html import HTMLSession
import re
import csv
from urllib.parse import urljoin, urlparse
import time

BASE_URL = "https://www.business-standard.com/"
ARTICLE_PATTERN = re.compile(r"^https://www\.business-standard\.com/.*-\d+_1\.html$")
BLACKLIST_PATTERNS = [
    "/web-stories", "/video-gallery", "/cricket", "/entertainment",
    "/health", "/companies/result", "/education", "/budget-2025",
    "/sports", "/lifestyle", "/hindi"
]

session = HTMLSession()
visited = set()
article_urls = set()
output_file = "output.csv"
MAX_DEPTH = 3

def is_valid_article(url):
    return ARTICLE_PATTERN.match(url) and not any(b in url for b in BLACKLIST_PATTERNS)

def crawl(url, depth=0):
    if depth > MAX_DEPTH or url in visited:
        return
    visited.add(url)

    try:
        response = session.get(url)
        response.html.render(sleep=2, timeout=20)  # Run JavaScript

        links = response.html.absolute_links
        print(f"[INFO] Depth {depth}: {url} | Found {len(links)} links")

        for link in links:
            parsed = urlparse(link)
            full_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

            if is_valid_article(full_url):
                if full_url not in article_urls:
                    article_urls.add(full_url)
                    print(f"[✅] Found article: {full_url}")
            elif BASE_URL in full_url and full_url not in visited:
                crawl(full_url, depth + 1)

        time.sleep(1)

    except Exception as e:
        print(f"[ERROR] Exception: {e}")

def save_to_csv(file_path, urls):
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["url"])
        writer.writeheader()
        for url in urls:
            writer.writerow({"url": url})

if __name__ == "__main__":
    crawl(BASE_URL)
    save_to_csv(output_file, article_urls)
    print(f"[DONE] Total articles collected: {len(article_urls)}")
