import os
import json
import math
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from scraping.JobFinder import JobFinder, generate_offer_id
from scraping.utils import measure_time, create_driver, load_id_sets_for_platform


class Apec(JobFinder):
    """
    Scraper Apec (Selenium).
    Convention:
    - fetch_detail(url) -> {"title":..., "description":...} ou None
    """

    def __init__(self):
        self.keywords = []
        self.base_url = ""
        self.get_config()

    def get_config(self):
        config_file = os.getenv("APP_CONFIG_FILE", "config.json")
        with open(config_file, "r", encoding="utf-8") as f:
            config = json.load(f)

        self.keywords = config.get("keywords", [])
        raw_url = config.get("url", {}).get("apec", "").strip()
        if not raw_url:
            self.base_url = ""
            return

        # on force placeholders {keywords} et {page} si besoin
        if "{keywords}" not in raw_url:
            raw_url = raw_url.replace("keywords=", "keywords={keywords}")
        if "page=" not in raw_url:
            raw_url += "&page={page}"
        self.base_url = raw_url

    def _empty_df(self):
        return self.formatData("apec", [], [], [], [], [])

    # ------------------------------
    # Convention: fetch_detail(url)
    # ------------------------------
    def fetch_detail(self, url: str) -> dict | None:
        driver = create_driver()
        try:
            driver.get(url)

            # titre
            try:
                title_el = WebDriverWait(driver, 6).until(
                    EC.presence_of_element_located((By.TAG_NAME, "h1"))
                )
                title = title_el.text.strip()
            except Exception:
                title = ""

            # description
            try:
                job_description_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//div[@class='col-lg-8 border-L']"))
                )
                description = job_description_element.text.strip()
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

    def _close_cookies(self, driver):
        try:
            btn = WebDriverWait(driver, 2).until(
                EC.element_to_be_clickable((By.ID, "onetrust-reject-all-handler"))
            )
            btn.click()
        except Exception:
            pass

    @measure_time
    def getJob(self, update_callback=None, cache=None, profile_id: str = ""):
        if not self.base_url or not self.keywords:
            print("APEC : config incomplète, scraping ignoré.")
            return self._empty_df()

        # fallback legacy sets si pas de cache
        blacklist_ids, whitelist_ids, known_offer_ids = set(), set(), set()
        if cache is None:
            try:
                blacklist_ids, whitelist_ids, known_offer_ids = load_id_sets_for_platform("apec")
            except Exception:
                pass

        driver = create_driver()
        all_jobs = []

        try:
            keyword = self.keywords[0]  # Apec url déjà construite pour le profil
            # page 0 pour détecter total
            first_url = self.base_url.format(keywords=keyword, page=0)
            driver.get(first_url)
            self._close_cookies(driver)

            # total offers (best effort)
            total_offers = 0
            try:
                total_el = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "span[data-cy='count-results']"))
                )
                txt = total_el.text.strip().replace(" ", "")
                total_offers = int("".join([c for c in txt if c.isdigit()]) or 0)
            except Exception:
                total_offers = 0

            # pages estimées (20/page)
            per_page = 20
            total_pages = max(1, math.ceil(total_offers / per_page)) if total_offers else 30

            seen = set()
            for page in range(total_pages):
                url = self.base_url.format(keywords=keyword, page=page)
                driver.get(url)
                self._close_cookies(driver)

                try:
                    offer_elements = WebDriverWait(driver, 5).until(
                        EC.presence_of_all_elements_located(
                            (By.CSS_SELECTOR, "a[queryparams]")
                        )
                    )
                except Exception:
                    offer_elements = []

                if not offer_elements:
                    # stop si pages vides répétées
                    if page > 2:
                        break
                    continue

                for a in offer_elements:
                    link = a.get_attribute("href") or ""
                    link = link.split("?")[0]
                    if not link or link in seen:
                        continue

                    title = a.text.strip() or ""
                    comp = ""
                    datetime_txt = ""

                    offer_id = generate_offer_id("apec", link)

                    if cache is not None:
                        if cache.exists(offer_id):
                            continue
                        cache.upsert_url(offer_id, "apec", link, "PENDING_URL")
                    else:
                        if offer_id in blacklist_ids or offer_id in whitelist_ids or offer_id in known_offer_ids:
                            continue

                    seen.add(link)
                    all_jobs.append((title, comp, link, datetime_txt, offer_id))

                if update_callback:
                    update_callback(len(all_jobs), max(len(all_jobs), 1), page + 1, total_pages)

        finally:
            try:
                driver.quit()
            except Exception:
                pass

        if not all_jobs:
            return self._empty_df()

        # détails (séquentiel, selenium)
        list_title, list_content, list_company, list_link, list_datetime = [], [], [], [], []
        total = len(all_jobs)

        for i, (title, comp, link, datetime_txt, offer_id) in enumerate(all_jobs):
            d = self.fetch_detail(link)
            if not d:
                if update_callback:
                    update_callback(i + 1, total)
                continue

            final_title = d.get("title") or title or ""
            desc = d.get("description") or ""

            if cache is not None:
                cache.upsert_detail(offer_id, "apec", link, final_title, desc, status="DETAILED")

            list_title.append(final_title)
            list_content.append(desc)
            list_company.append(comp)
            list_link.append(link)
            list_datetime.append(datetime_txt)

            if update_callback:
                update_callback(i + 1, total)

        df = self.formatData("apec", list_title, list_content, list_company, list_link, list_datetime)
        df = df.drop_duplicates(subset="hash", keep="first")
        return df
