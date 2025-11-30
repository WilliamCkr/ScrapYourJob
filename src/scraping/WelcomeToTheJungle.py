# import pandas as pd
# from tqdm import tqdm
import time
import os
import json
import re
from datetime import datetime as dt, timezone
# from dotenv import load_dotenv
from selenium.common import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
import urllib.parse

from scraping.JobFinder import JobFinder, generate_offer_id
from scraping.utils import measure_time, create_driver, load_id_sets_for_platform


class WelcomeToTheJungle(JobFinder):

    def __init__(self):
        # self.keywords = ["Data Scientist", "Machine Learning"]
        # self.url = "https://www.welcometothejungle.com/fr/jobs?refinementList%5Bcontract_type%5D%5B%5D=full_time&refinementList%5Bsectors.parent_reference%5D%5B%5D=industry-1&refinementList%5Bsectors.parent_reference%5D%5B%5D=public-administration-1&refinementList%5Bsectors.reference%5D%5B%5D=artificial-intelligence-machine-learning&refinementList%5Bsectors.reference%5D%5B%5D=big-data-1&refinementList%5Bsectors.reference%5D%5B%5D=cyber-security&refinementList%5Blanguage%5D%5B%5D=fr&refinementList%5Boffices.country_code%5D%5B%5D=FR&query={}&page=1&aroundQuery=Nanterre%2C%20France&searchTitle=false&aroundLatLng=48.88822%2C2.19428&aroundRadius=20"
        self.get_config()

    def get_config(self):
        config_file = os.getenv("APP_CONFIG_FILE", "config.json")
        with open(config_file, 'r', encoding="utf-8") as f:
            config = json.load(f)
        self.keywords = config['keywords']
        self.url = re.sub(r'query=[^&]*', 'query={}', config['url']['wttj'])

    def build_urls(self):
        list_url = []
        for k in self.keywords:
            keyword = urllib.parse.quote(k)
            list_url.append(self.url.format(keyword))
        return list_url

    def __close_cookie_banner(self, driver):
        # Fermer la bannière de cookies si elle est présente
        try:
            cookie_banner = WebDriverWait(driver, 2).until(
                EC.element_to_be_clickable((By.ID, "axeptio_btn_dismiss"))
            )
            cookie_banner.click()
            print("Bannière de cookies fermée (refusé tous les cookies).")
        except:
            print("Aucune bannière de cookies détectée.")

        try:
            btn = driver.find_element(
                By.XPATH, "//button[span[text()='Créer une alerte']]"
            )
            driver.execute_script("arguments[0].style.display = 'none';", btn)
            print("Bouton alerte fermé.")
        except:
            print("Aucune bouton alterte détecté.")

    @measure_time
    def getJob(self, update_callback=None):
        driver = create_driver()
        list_urls = self.build_urls()

        # Stocker tous les jobs trouvés
        all_jobs = []
        for url in list_urls:
            driver.get(url)

            count = 1
            while True:
                print(f"Page {count}")

                self.__close_cookie_banner(driver)

                job_cards = WebDriverWait(driver, 5).until(
                    EC.presence_of_all_elements_located(
                        (
                            By.XPATH,
                            "//li[@data-testid='search-results-list-item-wrapper']",
                        )
                    )
                )

                for card in job_cards:
                    # titre du job
                    title_elem = card.find_element(By.XPATH, ".//a[h2]")
                    title = title_elem.text.strip()
                    link = title_elem.get_attribute("href")

                    # nom de l’entreprise
                    company_elem = card.find_element(
                        By.XPATH,
                        ".//span[contains(concat(' ', normalize-space(@class), ' '), ' wui-text ')]",
                    )
                    company = company_elem.text.strip()

                    # gestion de la date ou du bouton "Sponsorisé"
                    try:
                        datetime = card.find_element(
                            By.TAG_NAME, "time"
                        ).get_attribute("datetime")
                    except NoSuchElementException:
                        datetime = dt.now(timezone.utc).strftime(
                            "%Y-%m-%dT%H:%M:%SZ"
                        )

                    if title and link and company and datetime:
                        all_jobs.append((title, company, link, datetime))

                # Vérifier si un bouton "Page suivante" est actif
                try:
                    next_button = WebDriverWait(driver, 2).until(
                        EC.element_to_be_clickable(
                            (
                                By.XPATH,
                                '//nav[@aria-label="Pagination"]//li[last()]//a',
                            )
                        )
                    )
                    is_disabled = next_button.get_attribute("aria-disabled")

                    if is_disabled == "false":
                        print("Passage à la page suivante...")
                        count += 1
                        next_button.click()
                    else:
                        print("Fin de pagination")
                        break
                except TimeoutException:
                    print("Fin de pagination")
                    break

        # Élimination des doublons
        seen_links = set()
        unique_jobs = []
        for job in all_jobs:
            link = job[2]
            if link not in seen_links:
                unique_jobs.append(job)
                seen_links.add(link)

        # --- Filtrage avec white/black lists + offres déjà connues ---
        config_path = os.getenv("APP_CONFIG_FILE", "config.json")
        csv_path = os.getenv("JOB_DATA_FILE", "data/job.csv")

        blacklisted_ids, whitelisted_ids, known_ids = load_id_sets_for_platform(
            config_path=config_path,
            csv_path=csv_path,
            platform_key="wttj",
        )

        filtered_jobs = []
        for title, comp, link, datetime in unique_jobs:
            offer_id = generate_offer_id("wttj", link)

            # Blacklist : on skip
            if offer_id in blacklisted_ids:
                continue

            # Offres déjà connues (CSV) et pas whitelistées : on skip
            if offer_id in known_ids and offer_id not in whitelisted_ids:
                continue

            filtered_jobs.append((title, comp, link, datetime))

        unique_jobs = filtered_jobs

        # Récupérer le contenu de toutes les fiches de poste
        print(
            f"Nombre de fiche de poste WelcomeToTheJungle récupéré {len(unique_jobs)}"
        )
        list_title = []
        list_content = []
        list_company = []
        list_link = []
        list_datetime = []
        total = len(unique_jobs)
        for i, (title, comp, link, datetime) in enumerate(unique_jobs):
            driver.get(link)

            for attempt in range(3):
                try:
                    voir_plus = WebDriverWait(driver, 3).until(
                        EC.element_to_be_clickable(
                            (
                                By.XPATH,
                                "//span[contains(text(), 'Voir plus')]",
                            )
                        )
                    )

                    # Scroll vers l'élément (important en headless)
                    driver.execute_script(
                        "arguments[0].scrollIntoView({block: 'center'});",
                        voir_plus,
                    )

                    WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable(
                            (
                                By.XPATH,
                                "//span[contains(text(), 'Voir plus')]",
                            )
                        )
                    )

                    voir_plus.click()

                    description_div = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located(
                            (By.XPATH, "//div[@id='the-position-section']")
                        )
                    )

                    job_description = description_div.text
                    list_title.append(title)
                    list_content.append(job_description)
                    list_company.append(comp)
                    list_link.append(link)
                    list_datetime.append(datetime)
                    break
                except Exception as e:
                    if attempt < 2:
                        print(f"retrying... {attempt + 1}")
                        time.sleep(1)
                    else:
                        print("Failed to scrap")
                        print(link)

            print(f"WTTF {i}/{total}")
            if update_callback:
                update_callback(i + 1, total)

        driver.quit()

        df = self.formatData(
            "wttj", list_title, list_content, list_company, list_link, list_datetime
        )
        df = df.drop_duplicates(subset="hash", keep="first")
        return df


if __name__ == "__main__":
    WTJ = WelcomeToTheJungle()
    df = WTJ.getJob()
    df = df.sort_values(by="date", ascending=False)
    print("a")
