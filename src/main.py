import pandas as pd
import os
import json
# from rapidfuzz import fuzz
from datetime import datetime
import shutil
# from dotenv import load_dotenv
from openai import OpenAI
from mistralai import Mistral
from tqdm import tqdm
from enum import Enum

from scraping.WelcomeToTheJungle import WelcomeToTheJungle
from scraping.Apec import Apec
from scraping.Linkedin import Linkedin
from scraping.ServicePublic import ServicePublic
from scraping.utils import measure_time, add_LLM_comment, SCORE_THRESHOLD
from concurrent.futures import ThreadPoolExecutor
import traceback

class Platform(Enum):
    wttj = WelcomeToTheJungle
    apec = Apec
    linkedin = Linkedin
    sp = ServicePublic

@measure_time
def get_all_job(progress_dict, all_platforms, is_multiproc):

    def run_source(source_class):
        name = source_class.__name__
        print(f"[SCRAP] Démarrage {name}")
        platform = source_class()

        def update_callback(current, total):
            progress_dict[name] = (current, total)

        df = platform.getJob(update_callback=update_callback)
        print(f"[SCRAP] Fin {name}")
        return df

    if is_multiproc and len(all_platforms) > 1:
        print(f"[SCRAP] Mode multi-thread ({len(all_platforms)} workers)")
        with ThreadPoolExecutor(max_workers=len(all_platforms)) as executor:
            results = list(executor.map(run_source, all_platforms))
    else:
        print("[SCRAP] Mode séquentiel")
        results = [run_source(cls) for cls in all_platforms]

    return pd.concat(results, ignore_index=True)

def merge_dataframes(progress_dict, stored_df, new_df, config):
    """Fusionne les nouvelles offres dans le store et met à jour les listes d'ID.

    - Supprime d'emblée les offres présentes dans la blacklist.
    - Vérifie l'unicité à partir du hash, du lien **et** de l'``offer_id`` afin
      d'éviter les doublons même si le contenu évolue.
    - Applique le scoring LLM si activé et alimente les white/black lists.
    - Retourne ``(merged_df, config_mis_a_jour)``.
    """

    # --- Préparation config / LLM --- #
    use_llm = config.get("use_llm", False)
    llm_config = config.get("llm", {})

    # Initialisation des dictionnaires d'ID si absents
    id_whitelist = config.setdefault("id_whitelist", {})
    id_blacklist = config.setdefault("id_blacklist", {})

    platform_keys = ["wttj", "apec", "linkedin", "sp"]
    for k in platform_keys:
        id_whitelist.setdefault(k, [])
        id_blacklist.setdefault(k, [])

    # Ensemble global des IDs en blacklist (tous sites)
    blacklisted_ids = set()
    for k in platform_keys:
        blacklisted_ids.update(id_blacklist.get(k, []))

    # Client LLM
    client = None
    if use_llm:
        if llm_config.get("provider") == "ChatGPT":
            client = OpenAI(api_key=llm_config.get("gpt_api_key"))
        elif llm_config.get("provider") == "Mistral":
            client = Mistral(api_key=llm_config.get("mistral_api_key"))
        elif llm_config.get("provider") == "Local":
            client = None

    # Prévenir toute mutation surprise de la source
    new_df = new_df.copy()

    # --- Filtrage immédiat des nouvelles offres par blacklist --- #
    if "offer_id" in new_df.columns:
        new_df = new_df[~new_df["offer_id"].isin(blacklisted_ids)].copy()

    def update_id_lists(src, offer_id, score, is_good):
        if not offer_id or src not in platform_keys:
            return
        if not llm_config.get("generate_score"):
            return

        if score >= SCORE_THRESHOLD and is_good == 1:
            if offer_id not in id_whitelist[src]:
                id_whitelist[src].append(offer_id)
            if offer_id in id_blacklist[src]:
                id_blacklist[src].remove(offer_id)
        elif score < SCORE_THRESHOLD and offer_id not in id_blacklist[src]:
            id_blacklist[src].append(offer_id)

    # --- Cas où le store est vide --- #
    if stored_df.empty:
        if use_llm:
            tqdm.pandas()
            new_df = new_df.progress_apply(
                lambda row: add_LLM_comment(client, llm_config, row), axis=1
            )

            for _, row in new_df.iterrows():
                update_id_lists(
                    src=str(row.get("source", "")).lower(),
                    offer_id=row.get("offer_id"),
                    score=row.get("score", 0),
                    is_good=row.get("is_good_offer", 0),
                )

        return new_df, config

    # --- Filtrer les nouvelles lignes déjà présentes dans stored_df --- #
    new_rows = []
    existing_hashes = set(stored_df.get("hash", pd.Series([], dtype=str)).astype(str))
    existing_links = set(stored_df.get("link", pd.Series([], dtype=str)).astype(str))
    existing_offer_ids = set(
        stored_df.get("offer_id", pd.Series([], dtype=str))
        .dropna()
        .astype(str)
    )

    for _, new_row in new_df.iterrows():
        row = new_row.copy()
        hash_val = str(row.get("hash", ""))
        link = str(row.get("link", ""))
        offer_id = row.get("offer_id")
        offer_id_str = str(offer_id) if pd.notna(offer_id) else None

        if hash_val in existing_hashes or link in existing_links:
            continue
        if offer_id_str and offer_id_str in existing_offer_ids:
            continue

        new_rows.append(row)

    # --- Scoring + alimentation white/black list --- #
    for i, new_row in tqdm(
        list(enumerate(new_rows)),
        total=len(new_rows),
        desc="Traitement des offres récupérées",
    ):
        if use_llm:
            new_rows[i] = add_LLM_comment(client, llm_config, new_row)
            progress_dict["Traitement des nouvelles offres (LLM)"] = (
                i + 1,
                len(new_rows),
            )

        row = new_rows[i]
        update_id_lists(
            src=str(row.get("source", "")).lower(),
            offer_id=row.get("offer_id"),
            score=row.get("score", 0),
            is_good=row.get("is_good_offer", 0),
        )

    # --- Fusion finale avec le store --- #
    if new_rows:
        new_data = pd.DataFrame(new_rows)
        merged = pd.concat([stored_df, new_data], ignore_index=True)
    else:
        merged = stored_df

    return merged, config

def save_data(df):
    data_file = os.getenv("JOB_DATA_FILE", "data/job.csv")
    data_dir = os.path.dirname(data_file) or "data"
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    df.to_csv(data_file, index=False, sep=";", encoding="utf-8")

def get_store_data():
    data_file = os.getenv("JOB_DATA_FILE", "data/job.csv")
    backup_dir = "data/backup/"

    if os.path.exists(data_file):
        os.makedirs(backup_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        backup_file = os.path.join(backup_dir, f"job_backup_{timestamp}.csv")
        shutil.copy(data_file, backup_file)
        print(f"Backup créé : {backup_file}")
        return pd.read_csv(data_file, sep=";", encoding="utf-8")
    else:
        return pd.DataFrame(columns=["title", "content", "company", "link", "date"])

@measure_time
def update_store_data(progress_dict):
    """Scraping + fusion + scoring + filtrage des offres < seuil.
    Retourne (success: bool, error_message: str)."""
    try:
        # Charge la config du profil courant
        config_file = os.getenv("APP_CONFIG_FILE", "config.json")
        with open(config_file, "r", encoding="utf-8") as f:
            config = json.load(f)

        # --- Normalisation des flags launch_scrap --- #
        def _to_bool(v):
            if isinstance(v, bool):
                return v
            if isinstance(v, str):
                return v.lower() in ("true", "1", "yes", "on")
            if isinstance(v, (int, float)):
                return v != 0
            return False

        launch_scrap = config.get("launch_scrap", {})

        # On force uniquement les 4 clés connues
        normalized_launch = {}
        for key in ["wttj", "apec", "linkedin", "sp"]:
            normalized_launch[key] = _to_bool(launch_scrap.get(key, False))
        config["launch_scrap"] = normalized_launch

        # Plateformes activées (dans un ordre fixe)
        platform_map = {
            "wttj": Platform.wttj,
            "apec": Platform.apec,
            "linkedin": Platform.linkedin,
            "sp": Platform.sp,
        }
        active_platforms = [
            platform_map[k].value
            for k, active in normalized_launch.items()
            if active
        ]

        print("[SCRAP] launch_scrap normalisé :", normalized_launch)
        print(
            "[SCRAP] Plateformes actives :",
            [cls.__name__ for cls in active_platforms],
        )

        if not active_platforms:
            print("[SCRAP] Aucune plateforme sélectionnée, rien à faire.")
            return True, ""

        # Scraping de toutes les plateformes
        new_df = get_all_job(
            progress_dict,
            active_platforms,
            config.get("use_multithreading", False),
        )

        # Chargement de l'ancien store
        store_df = get_store_data()

        # Fusion + scoring
        merged_df, updated_config = merge_dataframes(
            progress_dict,
            store_df,
            new_df,
            config,
        )


        # Filtrage : on enlève les offres NON retenues (score < seuil)
        if "is_good_offer" in merged_df.columns and "score" in merged_df.columns:
            before = len(merged_df)
            merged_df = merged_df[
                ~(
                    (merged_df["is_good_offer"] == 0)
                    & (merged_df["score"] < SCORE_THRESHOLD)
                )
            ]
            removed = before - len(merged_df)
            if removed > 0:
                print(
                    f"Filtrage : {removed} offres supprimées (score < {SCORE_THRESHOLD})."
                )

        # Sauvegarde
        save_data(merged_df)
        # Sauvegarde config mise à jour (white/black lists)
        with open(config_file, "w", encoding="utf-8") as f:
            json.dump(updated_config, f, ensure_ascii=False, indent=2)


        return True, ""
    except Exception:
        import traceback
        tb = traceback.format_exc()
        print("✘ Erreur pendant update_store_data:")
        print(tb)
        return False, tb

    

if __name__ == "__main__":
    progress_dict = {
        "WelcomeToTheJungle": (0, 1),
        "Linkedin": (0, 1),
        "Apec": (0, 1),
        "ServicePublic": (0, 1),
        "Traitement des nouvelles offres (LLM)": (0, 1)
    }
    update_store_data(progress_dict)







