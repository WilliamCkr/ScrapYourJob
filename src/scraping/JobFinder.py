import hashlib
import os
from typing import Optional

import pandas as pd
import requests


def generate_offer_id(plateforme: str, link: str) -> str:
    """ID stable (plateforme + lien) pour white/black lists + cache."""
    text = f"{plateforme}{str(link)}"
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class JobFinder:
    """Base scraper helpers."""

    def get_offer_cache(self) -> Optional[object]:
        """Retourne le cache SQLite par profil si dispo, sinon None."""
        try:
            try:
                from scraping.offer_cache import OfferCache  # type: ignore
            except Exception:
                from offer_cache import OfferCache  # type: ignore

            config_path = os.getenv("APP_CONFIG_FILE", "config.json")
            return OfferCache.from_config(config_path)
        except Exception:
            return None

    def formatData(self, plateforme, list_title, list_content, list_company, list_link, list_datetime):
        def generate_hash(text):
            return hashlib.sha256(text.encode("utf-8")).hexdigest()

        hashes = []
        offer_ids = []
        for title, content, company, link, dt in zip(
            list_title, list_content, list_company, list_link, list_datetime
        ):
            hashes.append(generate_hash(plateforme + title + company + content + str(dt)))
            offer_ids.append(generate_offer_id(plateforme, link))

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
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

        return df

    def getJob(self, update_callback=None):
        """Méthode à surcharger dans chaque scraper concret."""
        raise NotImplementedError

    def get_content(self, url: str) -> requests.Response:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
        }
        response = requests.get(url, headers=headers, timeout=20)
        return response
