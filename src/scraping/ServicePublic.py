# scraping/ServicePublic.py
import os
import json
import re
import urllib.parse

import dateparser
from bs4 import BeautifulSoup

from scraping.JobFinder import JobFinder, generate_offer_id
from scraping.utils import measure_time, parallel_map_offers, load_id_sets_for_platform


class ServicePublic(JobFinder):
    """
    Scraper choisirleservicepublic.gouv.fr

    Supporte maintenant:
    - cache (OfferCache) pour alimenter les compteurs UI (PENDING_URL / DETAILED / SCORED_*)
    - update_callback(offres_cur, offres_total, pages_cur, pages_total) pour afficher les pages
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
            print(f"ServicePublic : aucune URL définie dans {config_file}, scraping ignoré.")
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
    def getJob(self, update_callback=None, cache=None, profile_id: str = ""):
        if not self.url_template:
            print("ServicePublic : URL non configurée, retour DataFrame vide.")
            return self._empty_df()

        if not self.keywords:
            print("ServicePublic : aucun mot-clé, retour DataFrame vide.")
            return self._empty_df()

        keywords = self.build_keywords()
        base_url = self.url_template.format(keywords)

        # fallback legacy sets si pas de cache
        blacklisted_ids, whitelisted_ids, known_ids = set(), set(), set()
        if cache is None:
            config_path = os.getenv("APP_CONFIG_FILE", "config.json")
            csv_path = os.getenv("JOB_DATA_FILE", "data/job.csv")
            try:
                blacklisted_ids, whitelisted_ids, known_ids = load_id_sets_for_platform(
                    config_path=config_path,
                    csv_path=csv_path,
                    platform_key="sp",
                )
            except Exception:
                pass

        # --- 1) Récupérer nb de pages (best effort) ---
        try:
            res = self.get_content(base_url)
            soup = BeautifulSoup(res.text, "html.parser")
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
        all_jobs = []  # (title, comp, link, dt, offer_id)
        seen_links = set()

        for i in range(last_page):
            page_num = i + 1
            page_url = base_url.rstrip("/") + f"/page/{page_num}"

            try:
                res = self.get_content(page_url)
            except Exception as e:
                print(f"ServicePublic : erreur requête page {page_num} : {e}")
                # update pages quand même
                if update_callback:
                    update_callback(len(all_jobs), max(len(all_jobs), 1), page_num, last_page)
                continue

            soup = BeautifulSoup(res.text, "html.parser")
            offers = soup.select("li.fr-col-12.item")

            for offer in offers:
                try:
                    link_el = offer.select_one("a.is-same-domain")
                    if not link_el:
                        continue

                    job_link = link_el.get("href", "")
                    if not job_link:
                        continue

                    if not job_link.startswith("http"):
                        job_link = urllib.parse.urljoin(base_url, job_link)

                    job_link = job_link.split("?")[0].strip()
                    if not job_link or job_link in seen_links:
                        continue

                    job_title = link_el.get_text(strip=True) or ""

                    ministere_el = offer.select_one("img.fr-responsive-img")
                    job_ministere = ministere_el.get("alt").strip() if ministere_el else ""

                    date_el = offer.select_one("li.fr-icon-calendar-line")
                    raw_date = date_el.get_text(strip=True) if date_el else ""
                    job_datetime = self.parse_date(raw_date) if raw_date else ""

                    offer_id = generate_offer_id("sp", job_link)

                    # Cache mode
                    if cache is not None:
                        if cache.exists(offer_id):
                            continue
                        cache.upsert_url(offer_id, "sp", job_link, "PENDING_URL")
                    else:
                        # Legacy filter mode
                        if offer_id in blacklisted_ids:
                            continue
                        if offer_id in known_ids and offer_id not in whitelisted_ids:
                            continue

                    seen_links.add(job_link)
                    all_jobs.append((job_title, job_ministere, job_link, job_datetime, offer_id))

                except Exception as e:
                    print(f"ServicePublic : erreur lecture offre : {e}")

            # callback pages + offers (total inconnu à ce stade)
            if update_callback:
                update_callback(len(all_jobs), max(len(all_jobs), 1), page_num, last_page)

        if not all_jobs:
            print("ServicePublic : aucune offre trouvée.")
            return self._empty_df()

        print(f"ServicePublic : fiches récupérées (après filtres/cache) : {len(all_jobs)}")

        # --- 3) Récupération du détail des offres en parallèle (HTTP) ---
        def _fetch_detail(job):
            title, comp, link, dt, offer_id = job
            description = ""
            try:
                res = self.get_content(link)
                soup = BeautifulSoup(res.text, "html.parser")
                target_div = soup.find(
                    "div",
                    class_=lambda x: x is not None and "col-left" in x.split() and "rte" in x.split(),
                )
                if target_div:
                    description = target_div.get_text(" ", strip=True)
                else:
                    print(f"ServicePublic : aucune description pour {title}, skip.")
                    if cache is not None:
                        cache.mark_error(offer_id, status="ERROR_DETAIL")
                    return None
            except Exception as e:
                print(f"ServicePublic : erreur récupération détail pour {link} : {e}")
                if cache is not None:
                    cache.mark_error(offer_id, status="ERROR_DETAIL")
                return None

            if not description.strip():
                print(f"ServicePublic : description vide pour {title}, offre ignorée.")
                if cache is not None:
                    cache.mark_error(offer_id, status="ERROR_DETAIL")
                return None

            final_title = title or ""

            if cache is not None:
                cache.upsert_detail(
                    offer_id=offer_id,
                    source="sp",
                    url=link,
                    title=final_title,
                    description=description,
                    status="DETAILED",
                )

            return final_title, comp, link, dt, description, offer_id

        detailed_jobs = parallel_map_offers(all_jobs, _fetch_detail, io_bound=True)

        if not detailed_jobs:
            print("ServicePublic : aucun détail d'offre récupéré.")
            return self._empty_df()

        # callback final: offres_tot = offres_cur
        if update_callback:
            update_callback(len(detailed_jobs), len(detailed_jobs), last_page, last_page)

        list_title = [t for (t, c, l, d, desc, oid) in detailed_jobs]
        list_company = [c for (t, c, l, d, desc, oid) in detailed_jobs]
        list_link = [l for (t, c, l, d, desc, oid) in detailed_jobs]
        list_datetime = [d for (t, c, l, d, desc, oid) in detailed_jobs]
        list_content = [desc for (t, c, l, d, desc, oid) in detailed_jobs]

        df = self.formatData("sp", list_title, list_content, list_company, list_link, list_datetime)

        # on garde les offer_id calculés (formatData regénère, donc on override)
        try:
            df["offer_id"] = [oid for (t, c, l, d, desc, oid) in detailed_jobs]
            df["source"] = "sp"
        except Exception:
            pass

        df = df.drop_duplicates(subset="hash", keep="first")
        return df


if __name__ == "__main__":
    sp = ServicePublic()
    df = sp.getJob()
    df = df.sort_values(by="date", ascending=False)
    print(df.head())
