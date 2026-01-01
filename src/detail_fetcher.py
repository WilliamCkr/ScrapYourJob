from scraping.WelcomeToTheJungle import WelcomeToTheJungle
from scraping.Apec import Apec
from scraping.Linkedin import Linkedin
from scraping.ServicePublic import ServicePublic


SCRAPER_BY_SOURCE = {
    "wttj": WelcomeToTheJungle,
    "apec": Apec,
    "linkedin": Linkedin,
    "sp": ServicePublic,
}


def fetch_detail_by_source(source: str, url: str) -> dict | None:
    """
    Appelle fetch_detail(url) du bon scraper.
    """
    source = (source or "").lower()
    cls = SCRAPER_BY_SOURCE.get(source)
    if not cls:
        return None

    scraper = cls()
    if not hasattr(scraper, "fetch_detail"):
        raise RuntimeError(f"{cls.__name__} n'implémente pas fetch_detail()")

    try:
        return scraper.fetch_detail(url)
    except Exception:
        return None
