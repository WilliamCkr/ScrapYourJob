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
        """Récupère les offres LinkedIn via l'API jobs-guest."""
        if not self.job_id_api:
            print("LinkedIn : configuration invalide ou incomplète, aucun scraping effectué.")
            return self._empty_df()

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
                    # Lien vers la fiche
                    link_el = (
                        card.select_one("a.base-card__full-link")
                        or card.select_one("a.job-card-container__link.job-card-list__title--link")
                        or card.select_one("a.job-card-container__link")
                    )

                    # Titre de l'offre
                    title_el = (
                        card.select_one("h3.base-search-card__title")
                        or card.select_one("a.job-card-container__link.job-card-list__title--link span")
                        or card.select_one("a.job-card-container__link")
                    )

                    # Nom de l’entreprise
                    company_el = (
                        card.select_one("h4.base-search-card__subtitle")
                        or card.select_one("div.artdeco-entity-lockup__subtitle span")
                    )

                    # Date de publication (si dispo)
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

        # --- 2) Récupération du détail de chaque fiche (en parallèle) ---
        def _fetch_detail(job):
            title, company, link, date_str = job
            description = ""
            try:
                resp = requests.get(link, headers=self.headers, timeout=15)
                if resp.ok:
                    soup = BeautifulSoup(resp.text, "html.parser")
                    desc_el = (
                        soup.select_one("div.show-more-less-html__markup")
                        or soup.select_one("div.description__text")
                        or soup.select_one("div.jobs-description__content")
                    )
                    if desc_el:
                        description = desc_el.get_text(" ", strip=True)
            except Exception as e:
                print(f"LinkedIn : erreur lors de la récupération du détail d'une offre : {e}")

            return title, company, link, date_str, description

        detailed_jobs = parallel_map_offers(all_jobs, _fetch_detail, io_bound=True)

        list_title = [t for (t, c, l, d, desc) in detailed_jobs]
        list_company = [c for (t, c, l, d, desc) in detailed_jobs]
        list_link = [l for (t, c, l, d, desc) in detailed_jobs]
        list_datetime = [d for (t, c, l, d, desc) in detailed_jobs]
        list_content = [desc for (t, c, l, d, desc) in detailed_jobs]

        total = len(detailed_jobs)
        for i in range(total):
            print(f"Linkedin {i+1}/{total}")
            if update_callback:
                update_callback(i + 1, total)

        df = self.formatData("linkedin", list_title, list_content, list_company, list_link, list_datetime)
        df = df.drop_duplicates(subset="hash", keep="first")
        return df


if __name__ == "__main__":
    job = Linkedin()
    df = job.getJob()
    df = df.sort_values(by="date", ascending=False)
    print(df.head())
