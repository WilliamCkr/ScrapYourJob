import streamlit as st
import json
import os

from scraping.utils import analyze_categories_for_row


def save_data(df, data_file=None):
    if data_file is None:
        data_file = os.getenv("JOB_DATA_FILE", "data/job.csv")
    df.to_csv(data_file, sep=";", index=False, encoding="utf-8")


def category_analysis_page(df):
    CONFIG_FILE = os.getenv("APP_CONFIG_FILE", "config.json")

    # Chargement config profil courant
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            config = json.load(f)
    else:
        config = {"llm": {"provider": "Local"}, "categories": []}

    categories = config.get("categories", [])

    st.title("🎯 Analyse IA des catégories")

    # -----------------------------
    # Catégories à extraire (AUTO-SAVE)
    # -----------------------------
    st.subheader("📝 Catégories à extraire")
    categories_text = st.text_area(
        "Entrez une catégorie par ligne (ex : Télétravail, Salaire, Localisation, Tickets restaurant, etc.)",
        value="\n".join(categories),
        height=120,
        key="categories_ia_textarea",
    )

    new_categories = [c.strip() for c in categories_text.splitlines() if c.strip()]

    # Sauvegarde automatique si changement
    if new_categories != categories:
        categories = new_categories
        config["categories"] = categories
        try:
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(config, f, ensure_ascii=False, indent=2)
            st.caption("💾 Catégories sauvegardées automatiquement.")
        except Exception as e:
            st.error(f"Erreur lors de la sauvegarde des catégories : {e}")

    st.markdown("---")
    st.subheader("🤖 Analyse IA des offres retenues")

    if not categories:
        st.info("Ajoutez au moins une catégorie pour lancer l'analyse.")
        return

    good_jobs = df[df.get("is_good_offer", 0) == 1].copy()
    nb_jobs = len(good_jobs)

    if nb_jobs == 0:
        st.write("Aucune offre retenue par le scoring pour le moment.")
        return

    st.write(f"{nb_jobs} offre(s) seront analysées.")

    if st.button("🚀 Lancer / Refaire l'analyse IA sur ces offres"):
        llm_config = config.get("llm", {"provider": "Local"})
        progress = st.progress(0.0)
        status = st.empty()

        for i, (idx, row) in enumerate(good_jobs.iterrows(), start=1):
            values = analyze_categories_for_row(row, llm_config, categories)

            # on écrit directement dans le DF complet
            for cat, val in values.items():
                if cat not in df.columns:
                    df[cat] = ""
                df.at[idx, cat] = val

            progress.progress(i / nb_jobs, text=f"Analyse de l'offre {i}/{nb_jobs}")
            status.write(
                f"Analyse de l'offre {i}/{nb_jobs} : {row.get('title', '')[:80]}"
            )

        save_data(df)
        st.success(
            "Analyse IA terminée ! Les nouvelles colonnes de catégories sont maintenant disponibles pour les filtres."
        )
