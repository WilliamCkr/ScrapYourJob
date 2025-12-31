# scraping/ServicePublic.py
import os
import json
import re
from bs4 import BeautifulSoup
import urllib.parse
import dateparser

from scraping.JobFinder import JobFinder, generate_offer_id
from scraping.utils import measure_time, parallel_map_offers, load_id_sets_for_platform


class ServicePublic(JobFinder):
    """
    Scraper choisirleservicepublic.gouv.fr
    Lit la config via APP_CONFIG_FILE (sinon config.json) et utilise :
      - config['keywords'] : liste de mots-clés
      - config['url']['sp'] : URL modèle contenant 'mot-cles/<quelque chose>'
    """

    def __init__(self):
        self.keywords = []
        self.url_template = None  # URL avec mot-cles/{}
        self.get_config()

    def get_config(self):
        config_file = os.getenv("APP_CONFIG_FILE", "config.json")
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                config = json.load(f)
        except Exception as e:
            print(f"ServicePublic : impossible de lire {config_file} : {e}")
            self.url_template = None
            return

        self.keywords = config.get("keywords", [])
        raw_url = config.get("url", {}).get("sp", "").strip()

        if not raw_url:
            self.url_template = None
            print(
                f"ServicePublic : aucune URL définie dans {config_file}, scraping ignoré."
            )
            return

        # Remplace mot-cles/<quelque-chose> par mot-cles/{}
        self.url_template = re.sub(r"mot-cles/[^/]*", "mot-cles/{}", raw_url)

    def _empty_df(self):
        return self.formatData("sp", [], [], [], [], [])

    def build_keywords(self):
        joined_keywords = " ".join(self.keywords)
        return urllib.parse.quote(joined_keywords)

    def parse_date(self, date_to_parse: str) -> str:
        # Exemple : "En ligne depuis le 18/11/2024"
        date_str = date_to_parse.replace("En ligne depuis le ", "").strip()
        date_obj = dateparser.parse(date_str, languages=["fr"])
        return date_obj.strftime("%Y-%m-%d") if date_obj else ""

    @measure_time
    def getJob(self, update_callback=None):
        if not self.url_template:
            print("ServicePublic : URL non configurée, retour DataFrame vide.")
            return self._empty_df()

        if not self.keywords:
            print("ServicePublic : aucun mot-clé, retour DataFrame vide.")
            return self._empty_df()

        keywords = self.build_keywords()
        base_url = self.url_template.format(keywords)

        # --- 1) Récupérer nb de pages ---
        res = self.get_content(base_url)
        soup = BeautifulSoup(res.text, "html.parser")
        try:
            pages = soup.select("ul.fr-pagination__list a.fr-pagination__link")
            page_numbers = [
                int(a.get_text(strip=True))
                for a in pages
                if a.get_text(strip=True).isdigit()
            ]
            last_page = max(page_numbers) if page_numbers else 1
        except Exception:
            last_page = 1

        # --- 2) Récupérer toutes les offres sur toutes les pages ---
        all_jobs = []
        for i in range(last_page):
            page_url = base_url + f"page/{i + 1}"
            print(f"ServicePublic : page {i + 1}/{last_page} -> {page_url}")

            try:
                res = self.get_content(page_url)
            except Exception as e:
                print(f"ServicePublic : erreur requête page {i+1} : {e}")
                continue

            soup = BeautifulSoup(res.text, "html.parser")
            offers = soup.select("div.fr-col-12.item")

            for offer in offers:
                try:
                    link_el = offer.select_one("a.is-same-domain")
                    if not link_el:
                        continue
                    job_link = link_el["href"]
                    if not job_link.startswith("http"):
                        job_link = urllib.parse.urljoin(base_url, job_link)

                    job_title = link_el.get_text(strip=True)

                    ministere_el = offer.select_one("img.fr-responsive-img")
                    job_ministere = ministere_el.get("alt").strip() if ministere_el else ""

                    date_el = offer.select_one("li.fr-icon-calendar-line")
                    raw_date = date_el.get_text(strip=True) if date_el else ""
                    job_datetime = self.parse_date(raw_date) if raw_date else ""

                    all_jobs.append(
                        (job_title, job_ministere, job_link, job_datetime)
                    )
                except Exception as e:
                    print(f"ServicePublic : erreur lecture offre : {e}")

        if not all_jobs:
            print("ServicePublic : aucune offre trouvée.")
            return self._empty_df()

        print(
            f"Nombre de fiches de poste du Service Public récupérées (brut) : {len(all_jobs)}"
        )

        # --- 3) Filtrage via white/black list + offres déjà connues ---
        config_path = os.getenv("APP_CONFIG_FILE", "config.json")
        csv_path = os.getenv("JOB_DATA_FILE", "data/job.csv")

        blacklisted_ids, whitelisted_ids, known_ids = load_id_sets_for_platform(
            config_path=config_path,
            csv_path=csv_path,
            platform_key="sp",
        )

        filtered_jobs = []
        for title, comp, link, datetime in all_jobs:
            offer_id = generate_offer_id("sp", link)

            # Blacklist : on skip
            if offer_id in blacklisted_ids:
                continue

            # Offres déjà connues et pas whitelistées : on skip
            if offer_id in known_ids and offer_id not in whitelisted_ids:
                continue

            filtered_jobs.append((title, comp, link, datetime, offer_id))

        all_jobs = filtered_jobs
        if not all_jobs:
            print(
                "ServicePublic : aucune nouvelle offre à récupérer après application des listes."
            )
            return self._empty_df()

        print(
            f"ServicePublic : nombre d'offres à récupérer après filtres : {len(all_jobs)}"
        )

        # --- 4) Récupération du détail des offres en parallèle (HTTP) ---
        def _fetch_detail(job):
            title, comp, link, datetime, offer_id = job
            description = ""
            try:
                res = self.get_content(link)
                soup = BeautifulSoup(res.text, "html.parser")
                target_div = soup.find(
                    "div",
                    class_=lambda x: x is not None
                    and "col-left" in x.split()
                    and "rte" in x.split(),
                )
                if target_div:
                    description = target_div.get_text(" ", strip=True)
                else:
                    print(f"ServicePublic : aucune description pour {title}, skip.")
                    return None
            except Exception as e:
                print(f"ServicePublic : erreur récupération détail pour {link} : {e}")
                return None

            # ⬇⬇⬇ on ignore si c'est vide après nettoyage
            if not description.strip():
                print(f"ServicePublic : description vide pour {title}, offre ignorée.")
                return None

            return title, comp, link, datetime, description


        detailed_jobs = parallel_map_offers(all_jobs, _fetch_detail, io_bound=True)

        if not detailed_jobs:
            print("ServicePublic : aucun détail d'offre récupéré.")
            return self._empty_df()

        list_title = [t for (t, c, l, d, desc) in detailed_jobs]
        list_company = [c for (t, c, l, d, desc) in detailed_jobs]
        list_link = [l for (t, c, l, d, desc) in detailed_jobs]
        list_datetime = [d for (t, c, l, d, desc) in detailed_jobs]
        list_content = [desc for (t, c, l, d, desc) in detailed_jobs]

        total = len(detailed_jobs)
        for i in range(total):
            print(f"Service Public {i + 1}/{total}")
            if update_callback:
                update_callback(i + 1, total)

        df = self.formatData(
            "sp", list_title, list_content, list_company, list_link, list_datetime
        )
        df = df.drop_duplicates(subset="hash", keep="first")
        return df


if __name__ == "__main__":
    sp = ServicePublic()
    df = sp.getJob()
    df = df.sort_values(by="date", ascending=False)
    print(df.head())
