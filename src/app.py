import streamlit as st
import pandas as pd
import os
import json
import re
from datetime import datetime, timezone

from application.all_pages_app import (
    scrapping_page,
    new_offer_page,
    offer_readed_page,
    offer_applied_page,
    offer_refused_page,
    category_analysis_page,
)

# Fichier de définition des profils (à la racine)
PROFILES_FILE = ".profiles.json"


# ---------- Gestion des profils ---------- #


def _slugify(label: str) -> str:
    base = re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")
    return base or "profil"


def _get_default_profile() -> dict:
    """Profil par défaut, utilisé uniquement quand aucun autre profil n'existe."""
    return {
        "id": "default",
        "label": "Profil par défaut",
        "data_file": os.path.join("data", "default.csv"),
        "config_file": os.path.join("config", "default.json"),
        "created_at": None,
    }


def load_profiles():
    """Lit .profiles.json (ou l'ancien profiles.json) et renvoie la liste des profils utilisateur."""
    profiles_path = PROFILES_FILE

    # Compat : si .profiles.json n'existe pas encore mais profiles.json oui
    if not os.path.exists(profiles_path) and os.path.exists("profiles.json"):
        profiles_path = "profiles.json"

    if not os.path.exists(profiles_path):
        return []

    with open(profiles_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Format actuel : { "profiles": [ ... ] }
    if isinstance(data, dict) and isinstance(data.get("profiles"), list):
        profiles = data["profiles"]
    # Ancien format : liste directe
    elif isinstance(data, list):
        profiles = data
    else:
        profiles = []

    # Normalisation des chemins (tout dans data/ et config/)
    changed = False
    for p in profiles:
        # config_file
        if "config_file" in p and p["config_file"]:
            cf = str(p["config_file"]).replace("\\", "/")
            base = os.path.basename(cf)
            # Ancien nom du type config_william.json -> william.json
            if base.startswith("config_"):
                base = base[len("config_") :]
            if not cf.startswith("config/"):
                cf = os.path.join("config", base)
            if cf != p["config_file"]:
                p["config_file"] = cf
                changed = True

        # data_file
        if "data_file" in p and p["data_file"]:
            df = str(p["data_file"]).replace("\\", "/")
            base = os.path.basename(df)
            if not df.startswith("data/"):
                df = os.path.join("data", base)
            if df != p["data_file"]:
                p["data_file"] = df
                changed = True

    if changed:
        save_profiles(profiles)

    return profiles


def save_profiles(profiles):
    """Sauvegarde la liste de profils dans .profiles.json au format {"profiles": [...]}"""
    with open(PROFILES_FILE, "w", encoding="utf-8") as f:
        json.dump({"profiles": profiles}, f, ensure_ascii=False, indent=2)


def ensure_profile_files(profile):
    """Crée les fichiers data/config du profil s'ils n'existent pas."""
    data_file = profile["data_file"]
    config_file = profile["config_file"]

    os.makedirs(os.path.dirname(data_file), exist_ok=True)
    os.makedirs(os.path.dirname(config_file) or ".", exist_ok=True)

    # CSV des offres
    if not os.path.exists(data_file):
        df = pd.DataFrame(
            columns=[
                "title",
                "content",
                "company",
                "link",
                "date",
                "is_good_offer",
                "comment",
                "score",
                "custom_profile",
                "days_diff",
                "is_read",
                "is_apply",
                "is_refused",
            ]
        )
        df.to_csv(data_file, sep=";", index=False, encoding="utf-8")

    # Fichier de config
    if not os.path.exists(config_file):
        default_config = {
            "keywords": [],
            "url": {
                "wttj": "",
                "apec": "",
                "linkedin": "",
                "sp": "",
            },
            "launch_scrap": {
                "wttj": False,
                "apec": False,
                "linkedin": False,
                "sp": False,
            },
            "use_multithreading": False,
            "use_llm": False,
            "llm": {
                "provider": "Local",
                "gpt_api_key": "",
                "mistral_api_key": "",
                "generate_score": False,
                "prompt_score": "",
                "generate_custom_profile": False,
                "prompt_custom_profile": "",
                "cv": "",
            },
            "categories": [],
            "id_whitelist": {
                "wttj": [],
                "apec": [],
                "linkedin": [],
                "sp": [],
            },
            "id_blacklist": {
                "wttj": [],
                "apec": [],
                "linkedin": [],
                "sp": [],
            },
        }

        with open(config_file, "w", encoding="utf-8") as f:
            json.dump(default_config, f, ensure_ascii=False, indent=2)


def create_profile(label: str) -> dict:
    """Crée un nouveau profil utilisateur (nom 'default' interdit)."""
    if label.strip().lower() == "default":
        raise ValueError("Le nom 'default' est réservé et ne peut pas être utilisé.")

    profiles = load_profiles()
    slug = _slugify(label)

    # Éviter de réutiliser un id déjà existant
    existing_ids = {p["id"] for p in profiles}
    existing_ids.add("default")  # réserve aussi l'id interne du profil par défaut

    base_id = slug
    i = 1
    while slug in existing_ids:
        slug = f"{base_id}_{i}"
        i += 1

    data_file = os.path.join("data", f"{slug}.csv")
    config_file = os.path.join("config", f"{slug}.json")

    profile = {
        "id": slug,
        "label": label,
        "data_file": data_file,
        "config_file": config_file,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    profiles.append(profile)
    save_profiles(profiles)

    ensure_profile_files(profile)
    return profile


# ---------- Chargement / sélection du profil ---------- #


st.set_page_config(page_title="Gestion des candidatures", layout="wide")

st.sidebar.title("Profils")

profiles = load_profiles()
use_default_profile = len(profiles) == 0

if use_default_profile:
    # Aucun profil utilisateur -> profil par défaut (non listé, non supprimable)
    current_profile = _get_default_profile()
    ensure_profile_files(current_profile)
    st.sidebar.markdown("Profil actuel : **Profil par défaut**")
else:
    profile_labels = [p["label"] for p in profiles]
    current_label = st.sidebar.selectbox("Profil courant", profile_labels)
    current_profile = next(p for p in profiles if p["label"] == current_label)

# Boutons de gestion des profils
with st.sidebar.expander("Créer / gérer les profils"):
    # Création
    new_label = st.text_input("Nom du nouveau profil", key="new_profile_label")
    if st.button("Créer le profil"):
        if new_label.strip():
            if new_label.strip().lower() == "default":
                st.sidebar.error("Le nom 'default' est réservé, choisis un autre nom.")
            else:
                try:
                    prof = create_profile(new_label.strip())
                    st.sidebar.success(f"Profil '{prof['label']}' créé.")
                    st.rerun()
                except ValueError as e:
                    st.sidebar.error(str(e))
        else:
            st.sidebar.error("Le nom du profil ne peut pas être vide.")

    # Renommage / suppression uniquement si on n'est pas sur le profil par défaut
    if not use_default_profile:
        rename_label = st.text_input(
            "Renommer le profil courant",
            value=current_profile["label"],
            key="rename_profile_label",
        )
        if st.button("Renommer ce profil"):
            if rename_label.strip():
                if rename_label.strip().lower() == "default":
                    st.sidebar.error("Le nom 'default' est réservé, choisis un autre nom.")
                else:
                    for p in profiles:
                        if p["id"] == current_profile["id"]:
                            p["label"] = rename_label.strip()
                    save_profiles(profiles)
                    st.sidebar.success("Profil renommé.")
                    st.rerun()
            else:
                st.sidebar.error("Le nom du profil ne peut pas être vide.")

        if st.button("Supprimer ce profil"):
            prof = current_profile

            if os.path.exists(prof["data_file"]):
                os.remove(prof["data_file"])
            if os.path.exists(prof["config_file"]):
                os.remove(prof["config_file"])

            profiles = [p for p in profiles if p["id"] != prof["id"]]
            save_profiles(profiles)
            st.sidebar.success(f"Profil '{prof['label']}' supprimé.")
            st.rerun()

# S'assurer que les fichiers du profil courant existent (sécurité)
ensure_profile_files(current_profile)

# Définir les variables d'environnement pour le reste de l'app
os.environ["APP_CONFIG_FILE"] = current_profile["config_file"]
os.environ["JOB_DATA_FILE"] = current_profile["data_file"]


def load_data():
    """Charge le CSV du profil courant et s'assure que les colonnes existent."""
    data_file = os.getenv("JOB_DATA_FILE", "data/job.csv")

    REQUIRED_COLUMNS = [
        "title",
        "content",
        "company",
        "link",
        "date",
        "is_good_offer",
        "comment",
        "score",
        "custom_profile",
        "days_diff",
        "is_read",
        "is_apply",
        "is_refused",
    ]

    if os.path.exists(data_file):
        df = pd.read_csv(data_file, sep=";", encoding="utf-8")

        # Ajout des colonnes manquantes
        for col in REQUIRED_COLUMNS:
            if col not in df.columns:
                if col in ["is_read", "is_apply", "is_refused", "is_good_offer"]:
                    df[col] = 0
                elif col == "score":
                    df[col] = 0
                elif col == "days_diff":
                    df[col] = 0
                else:
                    df[col] = ""
    else:
        df = pd.DataFrame(columns=REQUIRED_COLUMNS)

    return df


# ---------- Navigation ---------- #


st.sidebar.markdown("---")
st.sidebar.title("Navigation")

page = st.sidebar.radio(
    "Choisissez une page :",
    (
        "Scraping d'offres",
        "Nouvelles offres",
        "Offres déjà lues",
        "Candidatures en cours",
        "Candidatures refusées",
        "Analyse IA avancée",
    ),
)

df = load_data()

if page == "Scraping d'offres":
    scrapping_page()

elif page == "Nouvelles offres":
    new_offer_page(df)

elif page == "Offres déjà lues":
    offer_readed_page(df)

elif page == "Candidatures en cours":
    offer_applied_page(df)

elif page == "Candidatures refusées":
    offer_refused_page(df)

elif page == "Analyse IA avancée":
    category_analysis_page(df)
