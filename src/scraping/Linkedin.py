from tqdm import tqdm
import pandas as pd
import json
import time
import random
from bs4 import BeautifulSoup
import backoff
from requests.exceptions import RequestException, HTTPError
import urllib.parse
import requests
import os

from scraping.JobFinder import JobFinder, generate_offer_id
from scraping.utils import measure_time, parallel_map_offers, load_id_sets_for_platform


class Linkedin(JobFinder):
    BASE_API = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"

    def __init__(self):
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
        }
        self.search_query = ""
        self.job_id_api = ""
        self.get_config()

    def get_config(self):
        config_file = os.getenv("APP_CONFIG_FILE", "config.json")
        if not os.path.exists(config_file):
            print(f"LinkedIn : fichier de config {config_file} introuvable.")
            return

        with open(config_file, "r", encoding="utf-8") as f:
            config = json.load(f)

        raw_url = config.get("url", {}).get("linkedin", "").strip()
        if not raw_url:
            print(f"LinkedIn : aucune URL définie dans {config_file}, scraping ignoré.")
            return

        parsed = urllib.parse.urlparse(raw_url)
        if not parsed.query:
            print("LinkedIn : URL invalide (pas de query string), scraping ignoré.")
            return

        self.search_query = parsed.query
        self.job_id_api = f"{self.BASE_API}?{self.search_query}&start={{start}}"

    def _empty_df(self):
        return self.formatData("linkedin", [], [], [], [], [])

    @backoff.on_exception(backoff.expo, (HTTPError, RequestException), max_tries=3)
    def _get_page(self, url: str) -> requests.Response:
        time.sleep(random.uniform(0.2, 0.6))
        resp = requests.get(url, headers=self.headers, timeout=15)
        if resp.status_code >= 400:
            resp.raise_for_status()
        return resp

    def fetch_detail(self, url: str) -> dict | None:
        try:
            resp = requests.get(url, headers=self.headers, timeout=15)
            if not resp.ok:
                return None
            soup = BeautifulSoup(resp.text, "html.parser")

            desc_el = (
                soup.select_one("div.show-more-less-html__markup")
                or soup.select_one("div.description__text")
                or soup.select_one("section.description")
            )
            description = desc_el.get_text(" ", strip=True) if desc_el else ""
            if not description.strip():
                return None

            title_el = soup.select_one("h1")
            title = title_el.get_text(" ", strip=True) if title_el else ""
            if not title:
                og = soup.find("meta", property="og:title")
                if og and og.get("content"):
                    title = og["content"].strip()

            return {"title": title or "", "description": description}
        except Exception:
            return None

    @measure_time
    def getJob(self, update_callback=None, cache=None, profile_id: str = ""):
        if not self.job_id_api:
            print("LinkedIn : configuration invalide ou incomplète, aucun scraping effectué.")
            return self._empty_df()

        blacklist_ids, whitelist_ids, known_offer_ids = set(), set(), set()
        if cache is None:
            try:
                blacklist_ids, whitelist_ids, known_offer_ids = load_id_sets_for_platform("linkedin")
            except Exception:
                pass

        all_jobs = []
        seen_links = set()
        start = 0
        page = 1

        # On ne connait pas le total exact => cap logique
        total_pages = 200

        while True:
            url = self.job_id_api.format(start=start)
            try:
                resp = self._get_page(url)
            except Exception as e:
                print(f"LinkedIn : erreur API jobs-guest : {e}")
                break

            html = resp.text.strip()
            if not html:
                break

            soup = BeautifulSoup(html, "html.parser")
            cards = soup.select("div.base-card") or soup.select("div.job-search-card")
            if not cards:
                break

            for card in cards:
                link_el = (
                    card.select_one("a.base-card__full-link")
                    or card.select_one("a.job-card-container__link")
                )
                company_el = (
                    card.select_one("h4.base-search-card__subtitle")
                    or card.select_one("div.artdeco-entity-lockup__subtitle span")
                )
                date_el = card.select_one("time")

                link = link_el["href"].split("?")[0] if link_el and link_el.get("href") else ""
                if not link or link in seen_links:
                    continue

                title = link_el.get_text(" ", strip=True) if link_el else ""
                company = company_el.get_text(" ", strip=True) if company_el else ""
                date_str = date_el.get("datetime") if date_el else ""

                offer_id = generate_offer_id("linkedin", link)

                if cache is not None:
                    if cache.exists(offer_id):
                        continue
                    cache.upsert_url(offer_id, "linkedin", link, "PENDING_URL")
                else:
                    if offer_id in blacklist_ids or offer_id in whitelist_ids or offer_id in known_offer_ids:
                        continue

                seen_links.add(link)
                all_jobs.append((title, company, link, date_str))

            start += 25
            page += 1

            if update_callback:
                update_callback(len(all_jobs), max(len(all_jobs), 1), min(page, total_pages), total_pages)

            if page > total_pages:
                break

        if not all_jobs:
            return self._empty_df()

        def _map(job):
            title, company, link, date_str = job
            d = self.fetch_detail(link)
            if not d:
                return None
            final_title = d.get("title") or title or ""
            desc = d.get("description") or ""
            if cache is not None:
                oid = generate_offer_id("linkedin", link)
                cache.upsert_detail(oid, "linkedin", link, final_title, desc, status="DETAILED")
            return final_title, company, link, date_str, desc

        detailed_jobs = parallel_map_offers(all_jobs, _map, io_bound=True)
        if not detailed_jobs:
            return self._empty_df()

        list_title = [t for (t, c, l, d, desc) in detailed_jobs]
        list_company = [c for (t, c, l, d, desc) in detailed_jobs]
        list_link = [l for (t, c, l, d, desc) in detailed_jobs]
        list_datetime = [d for (t, c, l, d, desc) in detailed_jobs]
        list_content = [desc for (t, c, l, d, desc) in detailed_jobs]

        df = self.formatData("linkedin", list_title, list_content, list_company, list_link, list_datetime)
        df = df.drop_duplicates(subset="hash", keep="first")
        return df
