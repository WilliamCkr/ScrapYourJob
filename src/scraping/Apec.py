import json
import re
import os
import math
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import urllib.parse

from scraping.JobFinder import JobFinder, generate_offer_id
from scraping.utils import measure_time, create_driver, load_id_sets_for_platform


class Apec(JobFinder):
    """
    Scraper Apec multi-pages :
    - lit l’URL dans config.json (ou APP_CONFIG_FILE)
    - récupère le nombre total d’offres (ex: 330)
    - boucle sur toutes les pages ?page=0,1,2,... jusqu’au bout
    """

    def __init__(self):
        self.keywords = []
        self.base_url = ""
        self.get_config()

    def get_config(self):
        """
        Exemple d’URL dans le config :
        https://www.apec.fr/candidat/recherche-emploi.html/emploi?typesConvention=...&lieux=75&motsCles=CRM&page=0
        On la transforme en template avec deux placeholders :
        - motsCles={keywords}
        - page={page}
        """
        config_file = os.getenv("APP_CONFIG_FILE", "config.json")
        with open(config_file, 'r', encoding="utf-8") as f:
            config = json.load(f)

        self.keywords = config.get('keywords', [])

        raw_url = config['url']['apec']

        # motsCles -> motsCles={keywords}
        url_kw = re.sub(r'motsCles=[^&]*', 'motsCles={keywords}', raw_url)

        # page=0 -> page={page}
        url_kw_page = re.sub(r'page=\d+', 'page={page}', url_kw)

        self.base_url = url_kw_page

    def build_keywords(self):
        joined_keywords = " OR ".join(self.keywords)
        return urllib.parse.quote(joined_keywords)

    @measure_time
    def getJob(self, update_callback=None):
        driver = create_driver()
        keyword = self.build_keywords()

        # 1) Charger la première page pour récupérer le nombre total d’offres
        first_url = self.base_url.format(keywords=keyword, page=0)
        print(f"APEC : page {first_url}")
        driver.get(first_url)

        # Fermer la bannière cookies si présente
        self._close_cookies(driver)

        # Récupérer le nombre total d’offres (ex: 330)
        try:
            nb_span = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, ".number-candidat span")
                )
            )
            total_offers_text = nb_span.text.replace("\xa0", "").replace(" ", "")
            total_offers = int(total_offers_text)
        except Exception as e:
            print(f"APEC : impossible de lire le nombre total d’offres, fallback à 20. Erreur: {e}")
            total_offers = 20

        # Apec affiche 20 offres par page
        page_size = 20
        total_pages = max(1, math.ceil(total_offers / page_size))
        print(f"APEC : {total_offers} offres détectées, {total_pages} page(s).")

        # 2) Collecter tous les jobs de toutes les pages
        all_jobs = []

        for page in range(total_pages):
            url = self.base_url.format(keywords=keyword, page=page)
            print(f"APEC : chargement de la page {page + 1}/{total_pages} -> {url}")
            driver.get(url)

            # bannière cookies éventuellement sur la 1ère page surtout
            self._close_cookies(driver)

            try:
                offer_elements = WebDriverWait(driver, 5).until(
                    EC.presence_of_all_elements_located(
                        (By.CSS_SELECTOR, "a[queryparamshandling='merge']")
                    )
                )
            except Exception as e:
                print(f"APEC : aucune offre détectée sur la page {page + 1} ({e}), on continue.")
                continue

            for offer in offer_elements:
                try:
                    job_link = offer.get_attribute("href")

                    job_title_el = offer.find_element(By.CSS_SELECTOR, "h2.card-title")
                    job_title = job_title_el.text.strip()

                    company_el = offer.find_element(
                        By.CSS_SELECTOR, "p.card-offer__company"
                    )
                    company_name = company_el.text.strip()

                    datetime_el = offer.find_element(
                        By.XPATH, ".//li[@title='Date de publication']"
                    )
                    datetime_txt = datetime_el.text.strip()

                    if job_title and company_name and job_link:
                        all_jobs.append(
                            (job_title, company_name, job_link, datetime_txt)
                        )
                except Exception as e:
                    print(f"APEC : erreur parsing d’une offre sur la page {page + 1} : {e}")
                    continue

        # --- Filtrage avec white/black lists + offres déjà connues ---
        config_path = os.getenv("APP_CONFIG_FILE", "config.json")
        csv_path = os.getenv("JOB_DATA_FILE", "data/job.csv")

        blacklisted_ids, whitelisted_ids, known_ids = load_id_sets_for_platform(
            config_path=config_path,
            csv_path=csv_path,
            platform_key="apec",
        )

        filtered_jobs = []
        for (title, comp, link, datetime_txt) in all_jobs:
            offer_id = generate_offer_id("apec", link)

            # Blacklist : on skip
            if offer_id in blacklisted_ids:
                continue

            # Offres déjà connues (CSV) et pas whitelistées : on skip
            if offer_id in known_ids and offer_id not in whitelisted_ids:
                continue

            filtered_jobs.append((title, comp, link, datetime_txt))

        all_jobs = filtered_jobs
        print(f"Nombre de fiches APEC collectées (après filtres) : {len(all_jobs)}")

        # 3) Détail de chaque offre
        list_title = []
        list_content = []
        list_company = []
        list_link = []
        list_datetime = []

        total = len(all_jobs)
        for i, (title, comp, link, datetime_txt) in enumerate(all_jobs):
            try:
                driver.get(link)

                job_description_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located(
                        (By.XPATH, "//div[@class='col-lg-8 border-L']")
                    )
                )
                job_description = job_description_element.text

            except Exception as e:
                print(f"APEC : erreur lors de la récupération du détail {link} : {e}")
                job_description = ""

            list_title.append(title)
            list_content.append(job_description)
            list_company.append(comp)
            list_link.append(link)
            list_datetime.append(datetime_txt)

            print(f"APEC {i + 1}/{total}")
            if update_callback:
                update_callback(i + 1, total)

        driver.quit()

        df = self.formatData(
            "apec", list_title, list_content, list_company, list_link, list_datetime
        )
        df = df.drop_duplicates(subset="hash", keep="first")
        return df

    def _close_cookies(self, driver):
        """Ferme la bannière cookies si présente."""
        try:
            cookie_banner = WebDriverWait(driver, 2).until(
                EC.element_to_be_clickable((By.ID, "onetrust-reject-all-handler"))
            )
            driver.execute_script(
                "arguments[0].scrollIntoView({block: 'center'});",
                cookie_banner,
            )
            cookie_banner = WebDriverWait(driver, 2).until(
                EC.element_to_be_clickable((By.ID, "onetrust-reject-all-handler"))
            )
            cookie_banner.click()
            print("APEC : bannière de cookies fermée.")
        except Exception:
            # Pas grave si elle n’est pas là
            pass


if __name__ == "__main__":
    APC = Apec()
    df = APC.getJob()
    df = df.sort_values(by="date", ascending=False)
    print(df.head())
