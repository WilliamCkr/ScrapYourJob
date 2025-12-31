import time
import json
import functools
import os

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

from ollama import generate
from pydantic import BaseModel


class Format(BaseModel):
    response: int
    justification: str


def measure_time(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time

        if args and hasattr(args[0], "__class__"):
            class_name = args[0].__class__.__name__
            print(f"Temps d'exécution de {class_name}.{func.__name__}: {execution_time:.2f} secondes")
        else:
            print(f"Temps d'exécution de {func.__name__}: {execution_time:.2f} secondes")

        return result

    return wrapper


def create_driver():
    options = Options()
    # options.binary_location = r"C:\Program Files\Google\Chrome\Application\chrome.exe"

    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920x1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver_path = ChromeDriverManager().install()
    try:
        os.chmod(driver_path, 0o755)
    except Exception as e:
        print(f"Impossible de modifier les permissions de chromedriver.exe : {e}")

    return webdriver.Chrome(service=Service(driver_path), options=options)


# Flag global pour éviter de rappeler le LLM après une erreur
LOCAL_LLM_AVAILABLE = True
SCORE_THRESHOLD = 65  # seuil d'acceptation de l'offre


def _local_llm_generate_score(llm_config, row):
    """Appel au LLM local Mistral 7B pour le scoring."""
    title = row["title"]
    company = row["company"]
    description = row["content"]

    response = generate(
        model="qwen2.5:14b-instruct-q4_K_M",
        options={"temperature": 0.1},
        format={
            "type": "object",
            "properties": {
                "reponse": {"type": "number"},
                "justification": {"type": "string"},
            }
        },
        prompt=(
            llm_config["prompt_score"]
            + "\n" + company
            + "\n" + title
            + "\n" + description
        ),
    )

    return json.loads(response.response)


def _local_llm_generate_profile(llm_config, row):
    """Génération du profil personnalisé (facultatif)."""
    response = generate(
        model="qwen2.5:14b-instruct-q4_K_M",
        options={"temperature": 0.3},
        prompt=(
            llm_config["cv"]
            + "\n" + row["content"]
            + "\n" + llm_config["prompt_custom_profile"]
        ),
    )
    return response.response


def add_LLM_comment(client_LLM, llm_config, row):
    global LOCAL_LLM_AVAILABLE

    # -------------------------
    # SCORING
    # -------------------------
    if llm_config.get("generate_score"):
        try:
            if llm_config["provider"] == "Local":
                if not LOCAL_LLM_AVAILABLE:
                    raise RuntimeError("LLM local désactivé après une erreur précédente.")

                json_output = _local_llm_generate_score(llm_config, row)
                score = int(json_output["reponse"])

                row["is_good_offer"] = 1 if score >= SCORE_THRESHOLD else 0
                row["comment"] = json_output["justification"]
                row["score"] = score

            # Providers externes (ChatGPT, Mistral API...)
            elif llm_config["provider"] == "ChatGPT":
                response = client_LLM.responses.parse(
                    model="qwen2.5:14b-instruct-q4_K_M",
                    instructions=llm_config["prompt_score"],
                    temperature=0.1,
                    input=row["company"] + "\n" + row["title"] + "\n" + row["content"],
                    text_format=Format
                )
                json_output = json.loads(response.output_text)
                score = int(json_output["reponse"])

                row["is_good_offer"] = 1 if score >= SCORE_THRESHOLD else 0
                row["comment"] = json_output["justification"]
                row["score"] = score

        except Exception as e:
            print(f"[LLM ERROR] Scoring désactivé : {e}")
            LOCAL_LLM_AVAILABLE = False
            row["is_good_offer"] = 0
            row["comment"] = "Scoring non évalué (erreur LLM)."
            row["score"] = 0

    # -------------------------
    # PROFIL PERSONNALISÉ
    # -------------------------
    if llm_config.get("generate_custom_profile") and (
        not llm_config.get("generate_score") or row.get("is_good_offer") == 1
    ):
        try:
            if llm_config["provider"] == "Local":
                if not LOCAL_LLM_AVAILABLE:
                    raise RuntimeError("LLM local indisponible.")
                row["custom_profile"] = _local_llm_generate_profile(llm_config, row)

        except Exception as e:
            print(f"[LLM ERROR] Profil non généré : {e}")
            row["custom_profile"] = "Profil non généré (erreur LLM)."

    return row

def analyze_categories_for_row(row, llm_config, categories):
    """
    Analyse dynamique des catégories pour une offre donnée.
    Retourne un dict {categorie: valeur_normalisée}.

    Catégories "spéciales" gérées :
    - Télétravail : 0j / 1j / 2j / 3j / 4-5j / Full remote / Occasionnel / Inconnu
    - Salaire    : <30k / 30-40k / 40-50k / 50-60k / 60-70k / >70k / Inconnu
    - Localisation : Ile-de-France / Province / Remote / Etranger / Inconnu
    - Tickets restaurant : Oui / Non / Inconnu
    - Avantages : Mutuelle / Transport / Mutuelle+Transport / Autres / Inconnu

    Les autres catégories (si tu en ajoutes) tombent dans un mode générique :
    texte court ou 'inconnu'.
    """
    if llm_config.get("provider") != "Local":
        return {}

    title = row.get("title", "")
    company = row.get("company", "")
    description = row.get("content", "")

    base_context = f"Entreprise : {company}\nTitre : {title}\nDescription : {description}"

    def _call_llm_single(property_name, prompt, allowed_values):
        """
        Appelle Mistral 7B avec un schéma très simple :
        { "value": "<une valeur autorisée>" }
        """
        try:
            response = generate(
                model="qwen2.5:14b-instruct-q4_K_M",
                options={"temperature": 0.1},
                format={
                    "type": "object",
                    "properties": {
                        "value": {"type": "string"},
                    },
                },
                prompt=prompt,
            )
            raw = json.loads(response.response)
            val = raw.get("value", "").strip()
        except Exception as e:
            print(f"[LLM ERROR] analyse catégories ({property_name}) : {e}")
            val = ""

        if val not in allowed_values:
            val = "Inconnu"
        return val

    result = {}

    for cat in categories:
        cat_clean = cat.strip()
        norm = cat_clean.lower()

        # --- Télétravail ---
        if "télétravail" in norm or "teletravail" in norm:
            allowed = [
                "0j",
                "1j",
                "2j",
                "3j",
                "4-5j",
                "Full remote",
                "Occasionnel",
                "Inconnu",
            ]
            prompt = f"""
Tu analyses une offre d'emploi.

{base_context}

Question : combien de jours de télétravail par semaine sont clairement mentionnés ?

Réponds STRICTEMENT avec ce JSON :
{{ "value": "<une seule de ces valeurs>" }}

Valeurs possibles :
- "0j"           : aucun télétravail
- "1j"
- "2j"
- "3j"
- "4-5j"         : 4 ou 5 jours de télétravail
- "Full remote"  : 100% télétravail possible
- "Occasionnel"  : télétravail ponctuel, non régulier
- "Inconnu"      : si ce n'est pas clair ou pas mentionné
"""
            val = _call_llm_single("Teletravail", prompt, allowed)
            result[cat_clean] = val
            continue

        # --- Salaire ---
        if "salaire" in norm or "rémunération" in norm:
            allowed = [
                "<30k",
                "30-40k",
                "40-50k",
                "50-60k",
                "60-70k",
                ">70k",
                "Inconnu",
            ]
            prompt = f"""
Tu analyses une offre d'emploi.

{base_context}

Si un salaire annuel brut en euros est mentionné (ou une fourchette),
convertis-le en une des fourchettes ci-dessous.

Réponds STRICTEMENT avec ce JSON :
{{ "value": "<une seule de ces valeurs>" }}

Valeurs possibles :
- "<30k"
- "30-40k"
- "40-50k"
- "50-60k"
- "60-70k"
- ">70k"
- "Inconnu"  : si aucun salaire clair n'est mentionné
"""
            val = _call_llm_single("Salaire", prompt, allowed)
            result[cat_clean] = val
            continue

        # --- Localisation ---
        if "localisation" in norm or "lieu" in norm:
            allowed = [
                "Ile-de-France",
                "Province",
                "Remote",
                "Etranger",
                "Inconnu",
            ]
            prompt = f"""
Tu analyses une offre d'emploi.

{base_context}

Tu dois classer la localisation dans une GRANDE ZONE.

Réponds STRICTEMENT avec ce JSON :
{{ "value": "<une seule de ces valeurs>" }}

Valeurs possibles :
- "Ile-de-France" : Paris + région parisienne
- "Province"      : reste de la France (hors IDF)
- "Remote"        : télétravail complet depuis n'importe où
- "Etranger"      : poste situé hors France
- "Inconnu"       : si ce n'est pas clair
"""
            val = _call_llm_single("Localisation", prompt, allowed)
            result[cat_clean] = val
            continue

        # --- Tickets restaurant ---
        if "ticket" in norm and "rest" in norm:
            allowed = ["Oui", "Non", "Inconnu"]
            prompt = f"""
Tu analyses une offre d'emploi.

{base_context}

Question : les tickets restaurant ou titres restaurant sont-ils mentionnés ?

Réponds STRICTEMENT avec ce JSON :
{{ "value": "<une seule de ces valeurs>" }}

Valeurs possibles :
- "Oui"
- "Non"
- "Inconnu" : si ce n'est pas clair ou pas mentionné
"""
            val = _call_llm_single("Tickets restaurant", prompt, allowed)
            result[cat_clean] = val
            continue

        # --- Avantages ---
        if "avantage" in norm:
            allowed = [
                "Mutuelle",
                "Transport",
                "Mutuelle+Transport",
                "Autres",
                "Inconnu",
            ]
            prompt = f"""
Tu analyses une offre d'emploi.

{base_context}

Tu dois classer les AVANTAGES proposés.

Réponds STRICTEMENT avec ce JSON :
{{ "value": "<une seule de ces valeurs>" }}

Valeurs possibles :
- "Mutuelle"           : si une mutuelle / complémentaire santé est mentionnée
- "Transport"          : si il y a remboursement transport / Navigo / frais de transport
- "Mutuelle+Transport" : si les deux sont clairement mentionnés
- "Autres"             : avantages divers mais pas clairement mutuelle ou transport
- "Inconnu"            : si rien n'est mentionné
"""
            val = _call_llm_single("Avantages", prompt, allowed)
            result[cat_clean] = val
            continue

        # --- Générique pour toute autre catégorie ---
        allowed = ["Inconnu", "Oui", "Non"]
        prompt = f"""
Tu analyses une offre d'emploi et une catégorie d'information : "{cat_clean}".

{base_context}

Tu dois répondre par un JSON très court :
{{ "value": "<texte court>" }}

- Si l'information est présente, réponds par un TEXTE TRÈS COURT (un ou deux mots).
- Si l'information n'est pas présente, réponds EXACTEMENT "Inconnu".
"""
        val = _call_llm_single(cat_clean, prompt, allowed + ["Oui", "Non"])
        result[cat_clean] = val

    return result


# ==========================
# OUTILS MULTITHREAD / LISTES
# ==========================
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

def compute_offer_workers(total_offers: int, io_bound: bool = True) -> int:
    """Calcule un nombre de workers adapté pour la récupération des offres.

    - io_bound=True : tâches réseau / I/O (HTTP, Selenium...)
    """
    if total_offers <= 0:
        return 0

    cpu = os.cpu_count() or 4

    if io_bound:
        # Pour I/O, on peut monter plus haut que le nombre de cœurs
        target = cpu * 5
        max_cap = 32
        min_cap = 4
    else:
        # Pour CPU-bound, on reste proche du nombre de cœurs
        target = max(1, cpu - 1)
        max_cap = cpu
        min_cap = 1

    workers = max(min_cap, min(target, max_cap, total_offers))
    return workers


def parallel_map_offers(jobs, func, io_bound: bool = True):
    """Applique func(job) en parallèle sur une liste de jobs.

    - jobs : liste de tuples / dicts décrivant les offres à récupérer
    - func : fonction qui prend un job en entrée et retourne un résultat (ou None)
    - io_bound : True pour HTTP / I/O, False pour CPU intensif

    Retourne une liste de résultats non None.
    """
    if not jobs:
        return []

    max_workers = compute_offer_workers(len(jobs), io_bound=io_bound)
    print(f"[SCRAP] Récupération détails en parallèle ({max_workers} workers, {len(jobs)} offres)")
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_job = {executor.submit(func, job): job for job in jobs}
        for future in as_completed(future_to_job):
            job = future_to_job[future]
            try:
                result = future.result()
                if result is not None:
                    results.append(result)
            except Exception as e:
                print(f"[SCRAP] Erreur pendant la récupération d'une offre : {e} | job = {job}")

    return results


def load_id_sets_for_platform(config_path: str, csv_path: str, platform_key: str):
    """Charge les ensembles d'IDs pour une plateforme donnée.

    - config_path : chemin du fichier de config (APP_CONFIG_FILE)
    - csv_path    : chemin du CSV global des offres (JOB_DATA_FILE)

    Retourne (blacklisted_ids, whitelisted_ids, known_ids).
    """
    # 1) config (white/black lists)
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    id_blacklist = config.get("id_blacklist", {})
    id_whitelist = config.get("id_whitelist", {})

    blacklisted_ids = set(id_blacklist.get(platform_key, []))
    whitelisted_ids = set(id_whitelist.get(platform_key, []))

    # 2) CSV existant : historique des offres déjà vues
    known_ids = set()
    if os.path.exists(csv_path):
        try:
            df = pd.read_csv(csv_path, sep=";", encoding="utf-8")
            if "offer_id" in df.columns:
                known_ids = set(df["offer_id"].dropna().astype(str).unique())
        except Exception as e:
            print(f"[SCRAP] Impossible de lire le CSV {csv_path} pour les known_ids : {e}")

    return blacklisted_ids, whitelisted_ids, known_ids
