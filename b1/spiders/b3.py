import scrapy
import re
import json
from fake_useragent import UserAgent
from urllib.parse import urljoin
from datetime import datetime

class BusinessStandardSpider(scrapy.Spider):
    name = "b3"
    allowed_domains = ["business-standard.com"]
    start_urls = ["https://www.business-standard.com/"]

    ua = UserAgent(fallback="Mozilla/5.0")
    article_pattern = re.compile(r"^https://www\.business-standard\.com/.*-\d{12}_1\.html$")
    blacklist_patterns = [
        "/web-stories", "/video-gallery", "/cricket", "/entertainment",
        "/health", "/companies/result", "/education", "/budget",
        "/sports", "/lifestyle", "/book", "/photos", "/podcasts",
        "/opinion", "/technology", "/management", "/hindi.business-standard.com",
        "/hindi"
    ]
    visited_urls = set()

    def start_requests(self):
        headers = {"User-Agent": self.ua.random}
        yield scrapy.Request(
            url=self.start_urls[0],
            headers=headers,
            callback=self.parse,
            errback=self.handle_error
        )

    def handle_error(self, failure):
        self.logger.warning(f"❌ Request failed: {failure.request.url}")

    def parse(self, response):
        if response.status != 200:
            self.logger.error(f"Failed to access {response.url} - Status Code: {response.status}")
            return

        links = response.css("a::attr(href)").getall()

        for link in links:
            full_url = urljoin(response.url, link)

            # Ensure the URL is from the English site and not in the blacklist
            if (any(pattern in full_url for pattern in self.blacklist_patterns) or
                "hindi.business-standard.com" in full_url or
                full_url in self.visited_urls or
                not full_url.startswith("https://www.business-standard.com/")):
                continue

            self.visited_urls.add(full_url)

            # If it's a valid article URL, scrape details
            if self.article_pattern.match(full_url):
                yield scrapy.Request(
                    url=full_url,
                    headers={"User-Agent": self.ua.random},
                    callback=self.parse_article,
                    meta={"url": full_url}
                )
            else:
                # Recursively follow internal links
                yield scrapy.Request(
                    url=full_url,
                    headers={"User-Agent": self.ua.random},
                    callback=self.parse,
                    dont_filter=True
                )

    def parse_article(self, response):
        url = response.meta["url"]

        try:
            ld_json_list = response.xpath('//script[@type="application/ld+json"]/text()').getall()
            for ld_json in ld_json_list:
                data = json.loads(ld_json)

                if isinstance(data, dict) and "headline" in data:
                    # Extract article ID from URL (e.g., the 12-digit number before "_1.html")
                    article_id_match = re.search(r"-(\d{12})_1\.html$", url)
                    article_id = article_id_match.group(1) if article_id_match else "N/A"

                    title = data.get("headline", "N/A").strip()
                    content = data.get("articleBody", "N/A").strip()
                    date = data.get("dateModified", data.get("datePublished", "N/A"))
                    keywords = data.get("keywords", [])

                    if isinstance(keywords, str):
                        keywords = [kw.strip() for kw in keywords.split(",")]

                    # Add current scraping timestamp
                    scraped_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    yield {
                        "url": url,
                        "title": title,
                        "content": content,  # Renamed "body" to "content" as per your request
                        "keywords": keywords,
                        "date": date,  # Renamed "update_date" to "date" as per your request
                        "media_house": "Business Standard",  # Static value for this spider
                        "article_id": article_id,
                        "scraped_at": scraped_at
                    }

        except Exception as e:
            self.logger.error(f"Error parsing article {url}: {e}")