import json
import os
import re
from datetime import datetime as dt, timezone

from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from scraping.JobFinder import JobFinder, generate_offer_id
from scraping.utils import measure_time, create_driver, load_id_sets_for_platform


class WelcomeToTheJungle(JobFinder):
    """
    WTTJ via Selenium.
    Convention:
    - fetch_detail(url) -> {"title":..., "description":...} ou None
    """

    def __init__(self):
        self.keywords = []
        self.url = ""
        self.get_config()

    def get_config(self):
        config_file = os.getenv("APP_CONFIG_FILE", "config.json")
        with open(config_file, "r", encoding="utf-8") as f:
            config = json.load(f)
        self.keywords = config.get("keywords", [])
        self.url = re.sub(r"query=[^&]*", "query={}", config.get("url", {}).get("wttj", ""))

    def build_urls(self):
        return [self.url.format(kw) for kw in self.keywords if kw]

    def fetch_detail(self, url: str) -> dict | None:
        driver = create_driver()
        try:
            driver.get(url)

            try:
                voir_plus = WebDriverWait(driver, 3).until(
                    EC.element_to_be_clickable((By.XPATH, "//span[contains(text(), 'Voir plus')]"))
                )
                voir_plus.click()
            except Exception:
                pass

            try:
                title = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.TAG_NAME, "h1"))
                ).text.strip()
            except Exception:
                title = ""

            try:
                description_div = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//div[@id='the-position-section']"))
                )
                description = description_div.text.strip()
            except Exception:
                description = ""

            if not description:
                return None

            return {"title": title or "", "description": description}
        finally:
            try:
                driver.quit()
            except Exception:
                pass

    @measure_time
    def getJob(self, update_callback=None, cache=None, profile_id: str = ""):
        urls = self.build_urls()
        if not urls:
            return self.formatData("wttj", [], [], [], [], [])

        # fallback legacy sets si pas de cache
        blacklist_ids, whitelist_ids, known_offer_ids = set(), set(), set()
        if cache is None:
            try:
                blacklist_ids, whitelist_ids, known_offer_ids = load_id_sets_for_platform("wttj")
            except Exception:
                pass

        driver = create_driver()
        all_jobs = []
        seen_links = set()

        total_pages = len(urls)
        pages_current = 0

        try:
            # 1) URLs (par keyword = 1 "page" logique)
            for idx, url in enumerate(urls, start=1):
                pages_current = idx
                driver.get(url)

                # Les liens d'offres sont généralement sous la forme /fr/companies/<org>/jobs/<slug>
                cards = driver.find_elements(
                    By.XPATH,
                    "//a[contains(@href, '/fr/companies/') and contains(@href, '/jobs/')]"
                )

                for card in cards:
                    try:
                        link = card.get_attribute("href")
                        if not link:
                            continue
                        link = link.split("?")[0]
                        if link in seen_links:
                            continue

                        title = card.text.strip() or ""
                        company = ""

                        try:
                            datetime = dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                        except Exception:
                            datetime = ""

                        offer_id = generate_offer_id("wttj", link)

                        if cache is not None:
                            if cache.exists(offer_id):
                                continue
                            cache.upsert_url(offer_id, "wttj", link, "PENDING_URL")
                        else:
                            if offer_id in blacklist_ids or offer_id in whitelist_ids or offer_id in known_offer_ids:
                                continue

                        seen_links.add(link)
                        all_jobs.append((title, company, link, datetime, offer_id))

                    except Exception:
                        continue

                if update_callback:
                    # offers_total inconnu => on met au moins current (pour éviter 0)
                    update_callback(len(all_jobs), max(len(all_jobs), 1), pages_current, total_pages)

        finally:
            try:
                driver.quit()
            except Exception:
                pass

        if not all_jobs:
            return self.formatData("wttj", [], [], [], [], [])

        # 2) détails (séquentiel, selenium)
        list_title, list_content, list_company, list_link, list_datetime = [], [], [], [], []
        total = len(all_jobs)

        for i, (title, company, link, datetime, offer_id) in enumerate(all_jobs, start=1):
            d = self.fetch_detail(link)
            if not d:
                if update_callback:
                    update_callback(i, total, total_pages, total_pages)
                continue

            final_title = d.get("title") or title or ""
            desc = d.get("description") or ""

            if cache is not None:
                cache.upsert_detail(offer_id, "wttj", link, final_title, desc, status="DETAILED")

            list_title.append(final_title)
            list_content.append(desc)
            list_company.append(company)
            list_link.append(link)
            list_datetime.append(datetime)

            if update_callback:
                update_callback(i, total, total_pages, total_pages)

        df = self.formatData("wttj", list_title, list_content, list_company, list_link, list_datetime)
        df = df.drop_duplicates(subset="hash", keep="first")
        return df
