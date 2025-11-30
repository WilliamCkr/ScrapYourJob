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

PROFILES_FILE = "profiles.json"


# ---------- Gestion des profils ---------- #

def _slugify(label: str) -> str:
    base = re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")
    return base or "profil"


def load_profiles():
    """Lit profiles.json et renvoie une liste de profils."""
    if not os.path.exists(PROFILES_FILE):
        return []

    with open(PROFILES_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Format actuel : {"profiles": [ ... ]}
    if isinstance(data, dict) and "profiles" in data and isinstance(data["profiles"], list):
        return data["profiles"]

    # Format de secours : directement une liste
    if isinstance(data, list):
        return data

    # Sinon on repart de zéro
    return []


def save_profiles(profiles):
    """Sauvegarde la liste de profils dans profiles.json au format {"profiles": [...]}."""
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
    profiles = load_profiles()
    slug = _slugify(label)

    # Éviter de réutiliser un id déjà existant
    existing_ids = {p["id"] for p in profiles}
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

# Si ton ancien fichier avait déjà un profil william, on le garde tel quel
if not profiles:
    # Créer un profil par défaut
    default_profile = create_profile("Profil par défaut")
    profiles = [default_profile]

profile_labels = [p["label"] for p in profiles]
current_label = st.sidebar.selectbox("Profil courant", profile_labels)

current_profile = next(p for p in profiles if p["label"] == current_label)

# Boutons de gestion des profils
with st.sidebar.expander("Créer / gérer les profils"):
    new_label = st.text_input("Nom du nouveau profil")
    if st.button("Créer le profil"):
        if new_label.strip():
            prof = create_profile(new_label.strip())
            st.sidebar.success(f"Profil '{prof['label']}' créé.")
            st.rerun()
        else:
            st.sidebar.error("Le nom du profil ne peut pas être vide.")

    # Renommer le profil courant
    rename_label = st.text_input("Renommer le profil courant", value=current_profile["label"])
    if st.button("Renommer ce profil"):
        if rename_label.strip():
            for p in profiles:
                if p["id"] == current_profile["id"]:
                    p["label"] = rename_label.strip()
            save_profiles(profiles)
            st.sidebar.success("Profil renommé.")
            st.rerun()
        else:
            st.sidebar.error("Le nom du profil ne peut pas être vide.")

    # Supprimer le profil courant
    if st.button("Supprimer ce profil"):
        if len(profiles) == 1:
            st.sidebar.error("Impossible de supprimer le dernier profil.")
        else:
            prof = current_profile
            # Supprimer les fichiers associés
            if os.path.exists(prof["data_file"]):
                os.remove(prof["data_file"])
            if os.path.exists(prof["config_file"]):
                os.remove(prof["config_file"])

            profiles = [p for p in profiles if p["id"] != prof["id"]]
            save_profiles(profiles)
            st.sidebar.success(f"Profil '{prof['label']}' supprimé.")
            st.rerun()

# S'assurer que les fichiers du profil courant existent
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


# Charger les données du profil sélectionné
df = load_data()

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
