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
    """
    Scraper LinkedIn basé sur l'API jobs-guest.
    Lit la config via APP_CONFIG_FILE (sinon config.json).
    """

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
        """
        Charge l'URL LinkedIn depuis la config et construit l'URL de l'API jobs-guest.
        """
        config_file = os.getenv("APP_CONFIG_FILE", "config.json")
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                config = json.load(f)
        except FileNotFoundError:
            print(f"LinkedIn : fichier de config {config_file} introuvable.")
            return

        raw_url = config.get("url", {}).get("linkedin", "").strip()
        if not raw_url:
            print(f"LinkedIn : aucune URL définie dans {config_file}, scraping ignoré.")
            return

        parsed = urllib.parse.urlparse(raw_url)
        if not parsed.query:
            print("LinkedIn : URL invalide (pas de query string), scraping ignoré.")
            return

        # On réutilise toute la query telle quelle (keywords déjà fixés dans l'URL du profil)
        self.search_query = parsed.query

        # URL API jobs-guest : même query + start={start}
        self.job_id_api = f"{self.BASE_API}?{self.search_query}&start={{start}}"
        print(f"LinkedIn : URL API configurée : {self.job_id_api}")

    def _empty_df(self):
        """Retourne un DataFrame vide cohérent."""
        return self.formatData("linkedin", [], [], [], [], [])

    @backoff.on_exception(backoff.expo, (HTTPError, RequestException), max_tries=3)
    def _get_page(self, url: str) -> requests.Response:
        """Appel HTTP avec backoff pour une page LinkedIn."""
        time.sleep(random.uniform(0.5, 1.5))
        resp = requests.get(url, headers=self.headers, timeout=15)
        resp.raise_for_status()
        return resp

    @measure_time
    def getJob(self, update_callback=None):
        """Récupère les offres LinkedIn via l'API jobs-guest + détail via jobPosting (anti-429)."""
        if not self.job_id_api:
            print("LinkedIn : configuration invalide ou incomplète, aucun scraping effectué.")
            return self._empty_df()

        import re
        from concurrent.futures import ThreadPoolExecutor, as_completed

        all_jobs = []
        seen_links = set()
        start = 0
        page = 1

        # --- 1) Récupération des cartes via l'API jobs-guest ---
        while True:
            url = self.job_id_api.format(start=start)
            print(f"LinkedIn : récupération de la page {page} (start={start})")

            try:
                resp = self._get_page(url)
            except Exception as e:
                print(f"LinkedIn : erreur lors de l'appel à l'API jobs-guest : {e}")
                break

            html = resp.text.strip()
            if not html:
                print("LinkedIn : réponse vide, fin de pagination.")
                break

            soup = BeautifulSoup(html, "html.parser")
            cards = soup.select("div.base-card")
            if not cards:
                cards = soup.select("div.job-card-container")
            if not cards:
                print("LinkedIn : aucune offre trouvée sur cette page, fin de pagination.")
                break

            new_jobs = 0
            for card in cards:
                try:
                    link_el = (
                        card.select_one("a.base-card__full-link")
                        or card.select_one("a.job-card-container__link.job-card-list__title--link")
                        or card.select_one("a.job-card-container__link")
                    )

                    title_el = (
                        card.select_one("h3.base-search-card__title")
                        or card.select_one("a.job-card-container__link.job-card-list__title--link span")
                        or card.select_one("a.job-card-container__link")
                    )

                    company_el = (
                        card.select_one("h4.base-search-card__subtitle")
                        or card.select_one("div.artdeco-entity-lockup__subtitle span")
                    )

                    date_el = card.select_one("time")

                    link = (
                        link_el["href"].split("?")[0]
                        if link_el and link_el.has_attr("href")
                        else None
                    )
                    title = title_el.get_text(strip=True) if title_el else None
                    company = company_el.get_text(strip=True) if company_el else None
                    date_str = (
                        date_el.get("datetime") or date_el.get_text(strip=True)
                        if date_el
                        else ""
                    )

                    if link and link not in seen_links and title and company:
                        seen_links.add(link)
                        all_jobs.append((title, company, link, date_str))
                        new_jobs += 1

                except Exception as e:
                    print(f"LinkedIn : erreur parsing carte : {e}")

            print(f"LinkedIn : {new_jobs} nouvelles offres ajoutées sur cette page.")

            if new_jobs == 0:
                print("LinkedIn : plus aucune nouvelle offre, fin de pagination.")
                break

            start += new_jobs
            page += 1

        # --- Filtrage avec white/black lists + offres déjà connues ---
        config_path = os.getenv("APP_CONFIG_FILE", "config.json")
        csv_path = os.getenv("JOB_DATA_FILE", "data/job.csv")

        blacklisted_ids, whitelisted_ids, known_ids = load_id_sets_for_platform(
            config_path=config_path,
            csv_path=csv_path,
            platform_key="linkedin",
        )

        filtered_jobs = []
        for (title, company, link, date_str) in all_jobs:
            offer_id = generate_offer_id("linkedin", link)

            # Blacklist : on skip
            if offer_id in blacklisted_ids:
                continue

            # Offres déjà connues (CSV) et pas whitelistées : on skip
            if offer_id in known_ids and offer_id not in whitelisted_ids:
                continue

            filtered_jobs.append((title, company, link, date_str))

        all_jobs = filtered_jobs
        total = len(all_jobs)
        print(f"LinkedIn : nombre total d'offres à récupérer (après filtres) : {total}")

        if total == 0:
            return self._empty_df()

        # --- 2) Détail via endpoint "jobPosting" (évite /jobs/view -> 429) ---
        JOB_POSTING_API = "https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{}"

        def _extract_job_id(url: str):
            # ex: https://fr.linkedin.com/jobs/view/...-4343017460
            m = re.search(r"-([0-9]{6,})$", url)
            return m.group(1) if m else None

        @backoff.on_exception(
            backoff.expo,
            (HTTPError, RequestException),
            max_tries=5,
            jitter=backoff.full_jitter,
        )
        def _get_detail(url: str) -> str:
            time.sleep(random.uniform(1.2, 2.8))  # anti-rate-limit
            r = requests.get(url, headers=self.headers, timeout=20)
            r.raise_for_status()
            return r.text

        def _fetch_detail(job):
            title, company, link, date_str = job

            job_id = _extract_job_id(link)
            if not job_id:
                print(f"LinkedIn : job_id introuvable dans {link}")
                return None

            detail_url = JOB_POSTING_API.format(job_id)

            try:
                html = _get_detail(detail_url)
            except HTTPError as e:
                status = getattr(e.response, "status_code", None)
                if status == 429:
                    print(f"LinkedIn : 429 sur {detail_url} (rate limit), on skip.")
                    return None
                print(f"LinkedIn : HTTP error {status} sur {detail_url} : {e}")
                return None
            except Exception as e:
                print(f"LinkedIn : erreur récupération détail {detail_url} : {e}")
                return None

            soup = BeautifulSoup(html, "html.parser")
            desc_el = (
                soup.select_one("div.show-more-less-html__markup")
                or soup.select_one("div.jobs-description__content")
                or soup.select_one("section.description")
                or soup.select_one("div.description__text")
            )

            description = desc_el.get_text(" ", strip=True) if desc_el else ""
            if not description.strip():
                print(f"LinkedIn : aucune description trouvée pour {link}, offre ignorée.")
                return None

            return title, company, link, date_str, description

        # ⚠️ IMPORTANT : limiter le parallélisme (sinon 429)
        max_workers = 2  # tu peux mettre 2 si ça passe, mais 1 = le plus safe
        print(f"[SCRAP] LinkedIn détails avec {max_workers} worker(s)")

        detailed_jobs = []
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [ex.submit(_fetch_detail, job) for job in all_jobs]
            done = 0
            for f in as_completed(futures):
                r = f.result()
                if r is not None:
                    detailed_jobs.append(r)

                done += 1
                if update_callback:
                    update_callback(done, total)

        if not detailed_jobs:
            print("LinkedIn : aucun détail d'offre récupéré.")
            return self._empty_df()

        list_title = [t for (t, c, l, d, desc) in detailed_jobs]
        list_company = [c for (t, c, l, d, desc) in detailed_jobs]
        list_link = [l for (t, c, l, d, desc) in detailed_jobs]
        list_datetime = [d for (t, c, l, d, desc) in detailed_jobs]
        list_content = [desc for (t, c, l, d, desc) in detailed_jobs]

        df = self.formatData(
            "linkedin", list_title, list_content, list_company, list_link, list_datetime
        )
        df = df.drop_duplicates(subset="hash", keep="first")
        return df



if __name__ == "__main__":
    job = Linkedin()
    df = job.getJob()
    df = df.sort_values(by="date", ascending=False)
    print(df.head())
