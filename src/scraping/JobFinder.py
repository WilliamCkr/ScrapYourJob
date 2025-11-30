import requests
import pandas as pd
import hashlib


def generate_offer_id(plateforme: str, link: str) -> str:
    """Génère un ID stable pour une offre (plateforme + lien).

    Cet ID doit rester cohérent entre le scraping et la fusion dans le CSV,
    pour alimenter correctement les white/black lists.
    """
    text = f"{plateforme}{str(link)}"
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class JobFinder:

    def formatData(self, plateforme, list_title, list_content, list_company, list_link, list_datetime):
        def generate_hash(text):
            return hashlib.sha256(text.encode('utf-8')).hexdigest()

        hashes = []
        offer_ids = []
        for title, content, company, link, datetime in zip(
            list_title, list_content, list_company, list_link, list_datetime
        ):
            # hash complet pour l'unicité dans le CSV
            hashes.append(
                generate_hash(plateforme + title + company + content + str(datetime))
            )
            # ID stable pour white/black list (plateforme + lien)
            offer_ids.append(
                generate_offer_id(plateforme, link)
            )

        data = {
            "title": list_title,
            "content": list_content,
            "company": list_company,
            "link": list_link,
            "date": list_datetime,
            "is_read": 0,
            "is_apply": 0,
            "is_refused": 0,
            "is_good_offer": 1,
            "comment": "",
            "score": -1,
            "custom_profile": "",
            "hash": hashes,
            "offer_id": offer_ids,
            "source": plateforme,
        }
        df = pd.DataFrame(data=data)
        if plateforme == "apec":
            df["date"] = pd.to_datetime(df["date"], dayfirst=True).dt.date
        else:
            df["date"] = pd.to_datetime(df["date"]).dt.date
        return df

    def getJob(self, update_callback=None):
        """Méthode à surcharger dans chaque scraper concret."""
        pass

    def get_content(self, url):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }

        response = requests.get(url, headers=headers)
        return response
