import time
import json
import functools
import os
import re
import shutil

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from concurrent.futures import ThreadPoolExecutor, as_completed

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
            print(
                f"Temps d'exécution de {class_name}.{func.__name__}: {execution_time:.2f} secondes"
            )
        else:
            print(f"Temps d'exécution de {func.__name__}: {execution_time:.2f} secondes")

        return result

    return wrapper


def _purge_wdm_cache():
    """
    Purge best-effort du cache webdriver_manager si un zip est corrompu.
    Sur Windows c'est souvent dans: %USERPROFILE%\\.wdm
    """
    try:
        home = os.path.expanduser("~")
        wdm_dir = os.path.join(home, ".wdm")
        if os.path.isdir(wdm_dir):
            shutil.rmtree(wdm_dir, ignore_errors=True)
            print(f"[WDM] Cache purgé: {wdm_dir}")
    except Exception as e:
        print(f"[WDM] Purge cache impossible: {e}")


def create_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920x1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    # Retry 1 fois si chromedriver zip corrompu
    last_err = None
    for attempt in range(2):
        try:
            driver_path = ChromeDriverManager().install()
            try:
                os.chmod(driver_path, 0o755)
            except Exception as e:
                print(f"Impossible de modifier les permissions de chromedriver.exe : {e}")

            return webdriver.Chrome(service=Service(driver_path), options=options)

        except Exception as e:
            last_err = e
            msg = str(e)
            # typiquement: zipfile.BadZipFile: File is not a zip file
            if "BadZipFile" in msg or "not a zip" in msg or "File is not a zip file" in msg:
                print(f"[WDM] Driver corrompu détecté (attempt={attempt+1}/2). Purge + retry…")
                _purge_wdm_cache()
                time.sleep(0.5)
                continue
            break

    raise RuntimeError(f"Impossible de créer le driver Chrome: {last_err}")


# Flag global pour éviter de rappeler le LLM après une erreur
LOCAL_LLM_AVAILABLE = True
SCORE_THRESHOLD = 65  # seuil d'acceptation de l'offre


def _ensure_llm_row(row: dict) -> dict:
    """Normalise les clés attendues par le scoring."""
    row.setdefault("title", "")
    row.setdefault("company", "")
    row.setdefault("content", "")
    return row


def _extract_json_object(text: str) -> dict:
    """
    Essaye de récupérer un JSON object même si le modèle renvoie du texte autour.
    """
    if not isinstance(text, str):
        raise ValueError("Réponse LLM non string")

    text = text.strip()

    # 1) parse direct
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    # 2) extrait le premier {...}
    m = re.search(r"\{[\s\S]*\}", text)
    if not m:
        raise ValueError("JSON introuvable dans la réponse LLM")
    candidate = m.group(0)

    obj = json.loads(candidate)
    if not isinstance(obj, dict):
        raise ValueError("JSON invalide (non dict)")
    return obj


def _local_llm_generate_score(llm_config, row):
    """Appel au LLM local (Ollama) pour le scoring."""
    title = str(row.get("title", "") or "")
    company = str(row.get("company", "") or "")
    description = str(row.get("content", "") or "")

    response = generate(
        model="qwen2.5:14b-instruct-q4_K_M",
        options={"temperature": 0.1},
        format={
            "type": "object",
            "properties": {
                "reponse": {"type": "number"},
                "justification": {"type": "string"},
            },
        },
        prompt=(llm_config["prompt_score"] + "\n" + company + "\n" + title + "\n" + description),
    )

    # ROBUSTE: accepte texte parasite, extrait le JSON
    return _extract_json_object(response.response)


def _local_llm_generate_profile(llm_config, row):
    """Génération du profil personnalisé (facultatif)."""
    response = generate(
        model="qwen2.5:14b-instruct-q4_K_M",
        options={"temperature": 0.3},
        prompt=(llm_config["cv"] + "\n" + row["content"] + "\n" + llm_config["prompt_custom_profile"]),
    )
    return response.response


def add_LLM_comment(client_LLM, llm_config, row):
    """
    IMPORTANT: en cas d'erreur LLM, on NE DOIT PAS blacklister.
    On met score=-1 et on laisse l'offre en attente de scoring.
    """
    global LOCAL_LLM_AVAILABLE

    # -------------------------
    # SCORING
    # -------------------------
    if llm_config.get("generate_score"):
        row = _ensure_llm_row(row)
        try:
            if llm_config["provider"] == "Local":
                if not LOCAL_LLM_AVAILABLE:
                    raise RuntimeError("LLM local désactivé après une erreur précédente.")

                json_output = _local_llm_generate_score(llm_config, row)
                score = int(float(json_output.get("reponse", 0)))

                row["is_good_offer"] = 1 if score >= SCORE_THRESHOLD else 0
                row["comment"] = str(json_output.get("justification", "") or "")
                row["score"] = score

            elif llm_config["provider"] == "ChatGPT":
                # NOTE: ton impl actuelle est suspecte (model=... qwen local)
                # je ne la change pas ici, juste on sécurise le parsing
                response = client_LLM.responses.parse(
                    model="qwen2.5:14b-instruct-q4_K_M",
                    instructions=llm_config["prompt_score"],
                    temperature=0.1,
                    input=str(row.get("company", "")) + "\n" + str(row.get("title", "")) + "\n" + str(row.get("content", "")),
                    text_format=Format,
                )
                json_output = _extract_json_object(response.output_text)
                score = int(float(json_output.get("reponse", 0)))

                row["is_good_offer"] = 1 if score >= SCORE_THRESHOLD else 0
                row["comment"] = str(json_output.get("justification", "") or "")
                row["score"] = score

        except Exception as e:
            print(f"[LLM ERROR] Scoring en erreur (offre laissée en attente) : {e}")
            if llm_config.get("provider") == "Local":
                LOCAL_LLM_AVAILABLE = False

            # NE PAS BLACKLISTER : on marque "non scoré"
            row["is_good_offer"] = 1
            row["comment"] = "Scoring non évalué (erreur LLM) — à re-tenter."
            row["score"] = -1

    # -------------------------
    # PROFIL PERSONNALISÉ
    # -------------------------
    if llm_config.get("generate_custom_profile") and (
        (not llm_config.get("generate_score")) or row.get("is_good_offer") == 1
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
    Analyse IA de catégories (page category_analysis_page.py)
    Retourne un dict {categorie: valeur}
    """
    # Support actuel : Local uniquement
    if llm_config.get("provider") != "Local":
        return {}

    title = row.get("title", "") or ""
    company = row.get("company", "") or ""
    description = row.get("content", "") or ""

    base_context = f"Entreprise : {company}\nTitre : {title}\nDescription : {description}"

    def _call_llm_single(prompt: str, allowed_values):
        try:
            response = generate(
                model="qwen2.5:14b-instruct-q4_K_M",
                options={"temperature": 0.1},
                format={"type": "object", "properties": {"value": {"type": "string"}}},
                prompt=prompt,
            )
            raw = _extract_json_object(response.response)  # <-- ta fonction robuste déjà dans utils.py
            val = (raw.get("value", "") or "").strip() or "Inconnu"
            return val if val in allowed_values else "Inconnu"
        except Exception:
            return "Inconnu"

    result = {}

    for cat in categories:
        cat_clean = (cat or "").strip()
        if not cat_clean:
            continue

        norm = cat_clean.lower()

        if "télétravail" in norm or "teletravail" in norm:
            allowed = ["0j", "1j", "2j", "3j", "4-5j", "Full remote", "Occasionnel", "Inconnu"]
            prompt = f"""
Tu analyses une offre d'emploi.

{base_context}

Question : combien de jours de télétravail par semaine sont clairement mentionnés ?

Réponds STRICTEMENT avec ce JSON :
{{ "value": "<une seule de ces valeurs>" }}

Valeurs possibles :
- "0j"
- "1j"
- "2j"
- "3j"
- "4-5j"
- "Full remote"
- "Occasionnel"
- "Inconnu"
"""
            result[cat_clean] = _call_llm_single(prompt, allowed)
            continue

        if "salaire" in norm or "rémunération" in norm or "remuneration" in norm:
            allowed = ["<30k", "30-40k", "40-50k", "50-60k", "60-70k", ">70k", "Inconnu"]
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
- "Inconnu"
"""
            result[cat_clean] = _call_llm_single(prompt, allowed)
            continue

        if "localisation" in norm or "lieu" in norm:
            allowed = ["Ile-de-France", "Province", "Remote", "Etranger", "Inconnu"]
            prompt = f"""
Tu analyses une offre d'emploi.

{base_context}

Tu dois classer la localisation dans une GRANDE ZONE.

Réponds STRICTEMENT avec ce JSON :
{{ "value": "<une seule de ces valeurs>" }}

Valeurs possibles :
- "Ile-de-France"
- "Province"
- "Remote"
- "Etranger"
- "Inconnu"
"""
            result[cat_clean] = _call_llm_single(prompt, allowed)
            continue

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
- "Inconnu"
"""
            result[cat_clean] = _call_llm_single(prompt, allowed)
            continue

        if "avantage" in norm:
            allowed = ["Mutuelle", "Transport", "Mutuelle+Transport", "Autres", "Inconnu"]
            prompt = f"""
Tu analyses une offre d'emploi.

{base_context}

Tu dois classer les AVANTAGES proposés.

Réponds STRICTEMENT avec ce JSON :
{{ "value": "<une seule de ces valeurs>" }}

Valeurs possibles :
- "Mutuelle"
- "Transport"
- "Mutuelle+Transport"
- "Autres"
- "Inconnu"
"""
            result[cat_clean] = _call_llm_single(prompt, allowed)
            continue

        # fallback générique
        allowed = ["Inconnu", "Oui", "Non"]
        prompt = f"""
Tu analyses une offre d'emploi et une catégorie d'information : "{cat_clean}".

{base_context}

Tu dois répondre par un JSON très court :
{{ "value": "<texte court>" }}

- Si l'information est présente, réponds par un TEXTE TRÈS COURT (un ou deux mots).
- Si l'information n'est pas présente, réponds EXACTEMENT "Inconnu".
"""
        # on accepte aussi du texte court, mais si ça sort des valeurs "autorisées", on remet "Inconnu"
        result[cat_clean] = _call_llm_single(prompt, allowed)

    return result


# ==========================
# OUTILS MULTITHREAD / LISTES
# ==========================
import pandas as pd


def compute_offer_workers(total_offers: int, io_bound: bool = True) -> int:
    if total_offers <= 0:
        return 0

    cpu = os.cpu_count() or 4

    if io_bound:
        target = cpu * 5
        max_cap = 32
        min_cap = 4
    else:
        target = max(1, cpu - 1)
        max_cap = cpu
        min_cap = 1

    workers = max(min_cap, min(target, max_cap, total_offers))
    return workers


def parallel_map_offers(jobs, func, io_bound: bool = True):
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
                print(f"[SCRAP] Erreur sur job {job}: {e}")

    return results


def load_id_sets_for_platform(config_path: str, csv_path: str, platform_key: str):
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    id_blacklist = config.get("id_blacklist", {})
    id_whitelist = config.get("id_whitelist", {})

    blacklisted_ids = set(id_blacklist.get(platform_key, []))
    whitelisted_ids = set(id_whitelist.get(platform_key, []))

    known_ids = set()
    if os.path.exists(csv_path):
        try:
            df = pd.read_csv(csv_path, sep=";", encoding="utf-8")
            if "offer_id" in df.columns:
                known_ids = set(df["offer_id"].dropna().astype(str).unique())
        except Exception as e:
            print(f"[SCRAP] Impossible de lire le CSV {csv_path} pour les known_ids : {e}")

    # (ton ancien cache sqlite "offers_cache.sqlite" est laissé tel quel)
    return blacklisted_ids, whitelisted_ids, known_ids


def parallel_map_pages(items, fn, max_workers=8, desc=None):
    results = []
    if not items:
        return results

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(fn, it): it for it in items}
        for fut in as_completed(futures):
            try:
                out = fut.result()
                if out is None:
                    continue
                if isinstance(out, list):
                    results.extend(out)
                else:
                    results.append(out)
            except Exception as e:
                it = futures.get(fut)
                print(f"[PAGES][ERROR] item={it} err={e}")
    return results
