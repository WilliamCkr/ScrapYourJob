import streamlit as st
import threading
import time
import pandas as pd
import json
import os

from main import update_store_data
from scraping.utils import analyze_categories_for_row


# ------------------------------------------------------------------
# Utilitaires généraux
# ------------------------------------------------------------------
def save_data(df, data_file=None):
    if data_file is None:
        data_file = os.getenv("JOB_DATA_FILE", "data/job.csv")
    df.to_csv(data_file, sep=";", index=False, encoding="utf-8")


def get_color(score):
    r = int(255 - (score * 2.55))
    g = int(score * 2.55)
    return f"rgb({r},{g},0)"


# ------------------------------------------------------------------
# Page SCRAPING + CONFIG
# ------------------------------------------------------------------
def scrapping_page():
    st.title("🔍 Scraping d'offres d’emploi")

    # Initialisation des états persistants
    if "scraping_running" not in st.session_state:
        st.session_state.scraping_running = False
    if "launch_clicked" not in st.session_state:
        st.session_state.launch_clicked = False
    if "scraping_started" not in st.session_state:
        st.session_state.scraping_started = False
    if "progress_dict" not in st.session_state:
        st.session_state.progress_dict = {
            "WelcomeToTheJungle": (0, 1),
            "Linkedin": (0, 1),
            "Apec": (0, 1),
            "ServicePublic": (0, 1),
            "Traitement des nouvelles offres (LLM)": (0, 1),
        }
    if "progress_bars" not in st.session_state:
        st.session_state.progress_bars = {}

    # Conteneur principal pour garder l’ordre stable
    with st.container():
        # Bouton de lancement (avec protection double-clic)
        if st.button(
            "🚀 Lancer le scraping",
            key="btn_launch_scraping",  # ⬅⬅⬅ clé UNIQUE
            disabled=st.session_state.scraping_running,
        ):
            if not st.session_state.launch_clicked:
                st.session_state.launch_clicked = True
                st.session_state.scraping_running = True
                st.rerun()

        # Affichage (ou re-création) des barres de progression
        for platform, (current, total) in st.session_state.progress_dict.items():
            percent = int((current / total) * 100) if total > 0 else 0
            if (
                platform not in st.session_state.progress_bars
                or st.session_state.progress_bars[platform] is None
            ):
                st.session_state.progress_bars[platform] = st.progress(
                    percent,
                    text=f"{platform} : {current} offres ({percent}%)",
                )
            else:
                st.session_state.progress_bars[platform].progress(
                    percent,
                    text=f"{platform} : {current} offres ({percent}%)",
                )

    # Démarrage réel du scraping
    if st.session_state.scraping_running and not st.session_state.scraping_started:
        st.session_state.scraping_started = True  # Évite plusieurs lancements

        # Réinitialise les barres et compteurs
        for k in st.session_state.progress_dict:
            st.session_state.progress_dict[k] = (0, 1)

        for k in st.session_state.progress_bars:
            st.session_state.progress_bars[k].progress(
                0, text=f"{k} (0 offres - 0%)"
            )

        result_container = {}

        def run(progress_dict):
            success, error_msg = update_store_data(progress_dict)
            result_container["success"] = success
            result_container["error"] = error_msg

        progress_dict = st.session_state.progress_dict
        thread = threading.Thread(target=run, args=(progress_dict,))
        thread.start()

        # Boucle de suivi des barres
        while thread.is_alive():
            for platform in st.session_state.progress_dict:
                current, total = st.session_state.progress_dict[platform]
                percent = int((current / total) * 100) if total > 0 else 0

                # Recrée si besoin (protection post-navigation)
                if (
                    platform not in st.session_state.progress_bars
                    or st.session_state.progress_bars[platform] is None
                ):
                    st.session_state.progress_bars[platform] = st.progress(
                        percent,
                        text=f"{platform} : {current} offres ({percent}%)",
                    )
                else:
                    st.session_state.progress_bars[platform].progress(
                        percent,
                        text=f"{platform} : {current} offres ({percent}%)",
                    )
            time.sleep(0.2)

        # Affiche le message en fonction du résultat
        if result_container.get("success"):
            st.success("🎉 Scraping terminé avec succès !")
        else:
            error_msg = (
                result_container.get("error")
                or "Erreur inconnue. Voir les logs dans le terminal."
            )
            with st.expander("Détails techniques de l'erreur"):
                st.code(error_msg)
            st.error("❌ Une erreur est survenue pendant le scraping.")

        # Réinitialisation des états
        st.session_state.scraping_running = False
        st.session_state.launch_clicked = False
        st.session_state.scraping_started = False

    # Affiche la config en dessous
    configuration_page()


def configuration_page():
    # 🔄 Chemin du fichier de config
    CONFIG_FILE = os.getenv("APP_CONFIG_FILE", "config.json")
    DEFAULT_CONFIG_FILE = "config_default.json"

    # 🧩 Charger la configuration actuelle
    def load_config():
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        else:
            cfg = {
                "keywords": [],
                "url": {
                    "wttj": "",
                    "apec": "",
                    "linkedin": "",
                    "sp": ""
                },
                "launch_scrap": {
                    "wttj": False,
                    "apec": False,
                    "linkedin": False,
                    "sp": False
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
                    "cv": ""
                },
                "categories": [],
            }

        # Initialiser les structures de white/black list si absentes
        platform_keys = ["wttj", "apec", "linkedin", "sp"]
        id_w = cfg.setdefault("id_whitelist", {})
        id_b = cfg.setdefault("id_blacklist", {})
        for k in platform_keys:
            id_w.setdefault(k, [])
            id_b.setdefault(k, [])

        return cfg

    # 💾 Sauvegarder la configuration
    def save_config(config):
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2, ensure_ascii=False)

    # 🔁 Réinitialiser la configuration depuis config_default.json
    def reset_config():
        if os.path.exists(DEFAULT_CONFIG_FILE):
            with open(DEFAULT_CONFIG_FILE, "r", encoding="utf-8") as f:
                default_config = json.load(f)
            save_config(default_config)
            return True
        else:
            return False

    # 🔧 Interface utilisateur Streamlit
    st.title("🔧 Configuration du Scraper")
    st.caption("Toutes les modifications sont sauvegardées automatiquement.")

    config = load_config()

    # 🎯 Section mots-clés
    st.header("🔑 Mots-clés")
    keywords = st.text_area(
        "Entrez les mots-clés (un par ligne)",
        "\n".join(config["keywords"])
    )
    config["keywords"] = [k.strip() for k in keywords.splitlines() if k.strip()]

    # 🌐 Section URLs
    st.header("🔗 URLs des sites")
    config["url"]["wttj"] = st.text_input("WTTJ URL", config["url"]["wttj"])
    config["url"]["apec"] = st.text_input("APEC URL", config["url"]["apec"])
    config["url"]["linkedin"] = st.text_input("LinkedIn URL", config["url"]["linkedin"])
    config["url"]["sp"] = st.text_input("Service Public URL", config["url"]["sp"])

    # 🚀 Sites à scraper
    st.header("📡 Lancer le scraping sur :")
    config["launch_scrap"]["wttj"] = st.checkbox("WTTJ", config["launch_scrap"]["wttj"])
    config["launch_scrap"]["apec"] = st.checkbox("APEC", config["launch_scrap"]["apec"])
    config["launch_scrap"]["linkedin"] = st.checkbox("LinkedIn", config["launch_scrap"]["linkedin"])
    config["launch_scrap"]["sp"] = st.checkbox("Service Public", config["launch_scrap"]["sp"])

    # ⚙️ Options générales
    st.header("⚙️ Options générales")
    config["use_multithreading"] = st.checkbox(
        "Utiliser le multithreading (permet de scrapper plusieurs sites en même temps mais demande plus de ressource)",
        config["use_multithreading"]
    )
    config["use_llm"] = st.checkbox("Utiliser un LLM", config["use_llm"])

    # 🤖 Configuration LLM
    if config["use_llm"]:
        st.subheader("🧠 Paramètres du LLM")

        config["llm"]["provider"] = st.radio(
            "Choisissez le fournisseur LLM :",
            ["Local", "ChatGPT", "Mistral"],
            index=["Local", "ChatGPT", "Mistral"].index(
                config["llm"].get("provider", "Local")
            ),
        )

        if config["llm"]["provider"] == "ChatGPT":
            config["llm"]["gpt_api_key"] = st.text_input(
                "Clé API GPT",
                config["llm"].get("gpt_api_key", "")
            )

        elif config["llm"]["provider"] == "Mistral":
            config["llm"]["mistral_api_key"] = st.text_input(
                "Clé API Mistral",
                config["llm"].get("mistral_api_key", "")
            )

        # 🔒 Scoring auto ON quand LLM est activé
        config["llm"]["generate_score"] = True
        st.markdown("✅ Le scoring est automatiquement activé quand le LLM est utilisé.")

        # Prompt de scoring
        config["llm"]["prompt_score"] = st.text_area(
            "Prompt de scoring (personnalisable) :",
            config["llm"].get("prompt_score", ""),
            height=250,
        )

        # Profil personnalisé optionnel
        config["llm"]["generate_custom_profile"] = st.checkbox(
            "Générer un profil en fonction de l'offre",
            config["llm"].get("generate_custom_profile", False),
        )
        if config["llm"]["generate_custom_profile"]:
            config["llm"]["prompt_custom_profile"] = st.text_area(
                "Entrez votre prompt pour générer votre profile",
                config["llm"].get("prompt_custom_profile", ""),
                height=180,
            )
            config["llm"]["cv"] = st.text_area(
                "Le texte de votre CV afin de mieux adapter le résumé du profile",
                config["llm"].get("cv", ""),
                height=250,
            )
    else:
        # Si LLM désactivé, on coupe aussi generate_score proprement
        config["llm"]["generate_score"] = False

    # 🧹 Nettoyage des blacklists par site
    st.header("🧹 Nettoyer les blacklists d'offres")
    platform_labels = {
        "wttj": "Welcome to the Jungle",
        "apec": "APEC",
        "linkedin": "LinkedIn",
        "sp": "Service Public",
    }

    col_b1, col_b2 = st.columns(2)
    with col_b1:
        for key in ["wttj", "apec"]:
            label = platform_labels[key]
            if st.button(f"Vider la blacklist {label}", key=f"clear_blacklist_{key}"):
                config["id_blacklist"][key] = []
                save_config(config)
                st.success(f"Blacklist {label} vidée.")

    with col_b2:
        for key in ["linkedin", "sp"]:
            label = platform_labels[key]
            if st.button(f"Vider la blacklist {label}", key=f"clear_blacklist_{key}"):
                config["id_blacklist"][key] = []
                save_config(config)
                st.success(f"Blacklist {label} vidée.")

    # Bas de page : seulement reset + SAUVEGARDE AUTO
    col1, col2 = st.columns(2)

    with col1:
        st.write("")  # espace vide ou petit texte si tu veux

    with col2:
        if st.button("♻️ Réinitialiser la configuration", key="btn_reset_config"):
            if reset_config():
                st.success("Configuration réinitialisée depuis config_default.json.")
                st.rerun()
            else:
                st.error("Fichier config_default.json introuvable.")

    # ✅ Sauvegarde automatique de TOUTE la configuration
    save_config(config)


# ------------------------------------------------------------------
# Page NOUVELLES OFFRES (avec Précédent / Suivant / Postuler)
# ------------------------------------------------------------------
def new_offer_page(df):
    CONFIG_FILE = os.getenv("APP_CONFIG_FILE", "config.json")
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            config = json.load(f)
        categories = config.get("categories", [])
    except Exception:
        categories = []

    st.title("📌 Offres pertinentes")

    unread_jobs = df[
        (df.get("is_read", 0) == 0) & (df.get("is_good_offer", 0) == 1)
    ]
    if unread_jobs.empty:
        st.write("✅ Aucune nouvelle offre pertinente à afficher.")
        return

    unread_jobs = unread_jobs.copy()

    def find_col(name_part: str):
        for c in categories:
            if name_part in c.lower() and c in df.columns:
                return c
        return None

    tele_col = find_col("télétravail") or find_col("teletravail")
    sal_col = find_col("salaire")
    loc_col = find_col("localisation")
    tick_col = None
    for c in categories:
        if "ticket" in c.lower() and "rest" in c.lower() and c in df.columns:
            tick_col = c
            break
    av_col = find_col("avantage")

    # ------------------- Filtres avancés ------------------- #
    st.subheader("🎯 Filtres avancés")
    col_f1, col_f2 = st.columns(2)

    with col_f1:
        if tele_col:
            tele_order = [
                "Full remote",
                "4-5j",
                "3j",
                "2j",
                "1j",
                "0j",
                "Occasionnel",
                "Inconnu",
            ]
            present = sorted(df[tele_col].dropna().unique())
            tele_options = [o for o in tele_order if o in present]
            tele_selected = st.multiselect("Télétravail", tele_options)
            if tele_selected:
                unread_jobs = unread_jobs[unread_jobs[tele_col].isin(tele_selected)]

    with col_f2:
        if sal_col:
            salary_order = [
                "<30k",
                "30-40k",
                "40-50k",
                "50-60k",
                "60-70k",
                ">70k",
                "Inconnu",
            ]
            present = sorted(df[sal_col].dropna().unique())
            salary_options = [o for o in salary_order if o in present]
            sal_selected = st.multiselect("Salaire (fourchettes)", salary_options)
            if sal_selected:
                unread_jobs = unread_jobs[unread_jobs[sal_col].isin(sal_selected)]

    col_f3, col_f4 = st.columns(2)

    with col_f3:
        if loc_col:
            loc_order = ["Ile-de-France", "Province", "Remote", "Etranger", "Inconnu"]
            present = sorted(df[loc_col].dropna().unique())
            loc_options = [o for o in loc_order if o in present]
            loc_selected = st.multiselect("Localisation", loc_options)
            if loc_selected:
                unread_jobs = unread_jobs[unread_jobs[loc_col].isin(loc_selected)]

    with col_f4:
        if tick_col:
            tick_order = ["Oui", "Non", "Inconnu"]
            present = sorted(df[tick_col].dropna().unique())
            tick_options = [o for o in tick_order if o in present]
            tick_selected = st.multiselect("Tickets restaurant", tick_options)
            if tick_selected:
                unread_jobs = unread_jobs[unread_jobs[tick_col].isin(tick_selected)]

    if av_col:
        adv_order = ["Mutuelle+Transport", "Mutuelle", "Transport", "Autres", "Inconnu"]
        present = sorted(df[av_col].dropna().unique())
        adv_options = [o for o in adv_order if o in present]
        adv_selected = st.multiselect("Avantages principaux", adv_options)
        if adv_selected:
            unread_jobs = unread_jobs[unread_jobs[av_col].isin(adv_selected)]

    # ------------------- Tri ------------------- #
    st.subheader("📊 Tri des offres")
    sort_choice = st.selectbox(
        "Trier par",
        [
            "Score décroissant",
            "Score croissant",
            "Date (plus récent d'abord)",
            "Date (plus ancien d'abord)",
            "Salaire décroissant",
            "Salaire croissant",
        ],
    )

    salary_order_map = {
        "<30k": 1,
        "30-40k": 2,
        "40-50k": 3,
        "50-60k": 4,
        "60-70k": 5,
        ">70k": 6,
        "Inconnu": 0,
    }
    if sal_col and sal_col in unread_jobs.columns:
        unread_jobs["_salary_order"] = (
            unread_jobs[sal_col].map(salary_order_map).fillna(0).astype(int)
        )
    else:
        unread_jobs["_salary_order"] = 0

    if sort_choice == "Score décroissant":
        unread_jobs = unread_jobs.sort_values("score", ascending=False)
    elif sort_choice == "Score croissant":
        unread_jobs = unread_jobs.sort_values("score", ascending=True)
    elif sort_choice == "Date (plus récent d'abord)":
        unread_jobs = unread_jobs.sort_values("days_diff", ascending=True)
    elif sort_choice == "Date (plus ancien d'abord)":
        unread_jobs = unread_jobs.sort_values("days_diff", ascending=False)
    elif sort_choice == "Salaire décroissant":
        unread_jobs = unread_jobs.sort_values("_salary_order", ascending=False)
    elif sort_choice == "Salaire croissant":
        unread_jobs = unread_jobs.sort_values("_salary_order", ascending=True)

    unread_jobs = unread_jobs.reset_index(drop=True)
    total_jobs = len(unread_jobs)
    if total_jobs == 0:
        st.write("❌ Aucune offre ne correspond aux filtres.")
        return

    # ------------------- Navigation ------------------- #
    if "index" not in st.session_state:
        st.session_state.index = 0

    st.markdown(f"**Offre {st.session_state.index + 1} / {total_jobs}**")
    current_index = st.session_state.index % total_jobs
    job = unread_jobs.iloc[current_index]

    st.subheader(job.get("title", ""))
    st.subheader(job.get("company", ""))

    if isinstance(job.get("link"), str):
        st.markdown(f"[🔗 Lien vers l'offre]({job['link']})", unsafe_allow_html=True)

    days = job.get("days_diff")
    if pd.notna(days):
        st.write(f"Publié il y a **{int(days)}** jours")
    else:
        st.write("Date de publication non renseignée")

    score = int(job.get("score", 0))
    if score != -1:
        color = get_color(score)
        st.markdown(
            f"""
            <div style="margin: 20px 0; width: 60%; background-color: #eee; border-radius: 5px;">
                <div style="width: {score}%; background-color: {color}; padding: 5px; border-radius: 5px; text-align: center; color: white;">
                    Score IA : {score}/100
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    if categories:
        with st.expander("🧩 Synthèse IA (télétravail, salaire, etc.)"):
            if tele_col and tele_col in job:
                st.write(f"- **Télétravail** : {job.get(tele_col, 'Inconnu')}")
            if sal_col and sal_col in job:
                st.write(f"- **Salaire** : {job.get(sal_col, 'Inconnu')}")
            if loc_col and loc_col in job:
                st.write(f"- **Localisation** : {job.get(loc_col, 'Inconnu')}")
            if tick_col and tick_col in job:
                st.write(f"- **Tickets resto** : {job.get(tick_col, 'Inconnu')}")
            if av_col and av_col in job:
                st.write(f"- **Avantages** : {job.get(av_col, 'Inconnu')}")

    with st.expander("📄 Description de l'offre"):
        st.write(job.get("content", ""))
        if isinstance(job.get("comment"), str) and job["comment"].strip():
            st.markdown("---")
            st.markdown("**Commentaire IA :**")
            st.write(job["comment"])

    # Boutons navigation + actions
    col_prev, col_mark, col_refuse, col_next = st.columns(4)

    with col_prev:
        if st.button("⬅️ Précédent"):
            st.session_state.index = (st.session_state.index - 1) % total_jobs
            st.rerun()

    with col_mark:
        if st.button("✅ Marquer comme lue"):
            df.loc[df["link"] == job["link"], "is_read"] = 1
            save_data(df)
            st.session_state.index = min(
                st.session_state.index, max(total_jobs - 2, 0)
            )
            st.rerun()

    with col_refuse:
        if st.button("🚫 Refuser"):
            df.loc[df["link"] == job["link"], ["is_refused", "is_read"]] = 1
            save_data(df)
            st.session_state.index = min(
                st.session_state.index, max(total_jobs - 2, 0)
            )
            st.rerun()

    with col_next:
        if st.button("➡️ Suivant"):
            st.session_state.index = (st.session_state.index + 1) % total_jobs
            st.rerun()

    # Bouton Postuler centré
    col_left, col_center, col_right = st.columns([1, 1, 1])
    with col_center:
        if st.button("📎 Postuler"):
            df.loc[df["link"] == job["link"], ["is_apply", "is_read"]] = 1
            save_data(df)
            st.session_state.index = min(
                st.session_state.index, max(total_jobs - 2, 0)
            )
            st.rerun()


# ------------------------------------------------------------------
# Autres pages (inchangées)
# ------------------------------------------------------------------
def offer_gpt_filter_page(df):
    st.title("📄 Offres non pertinentes")

    applied_jobs = (
        df[(df["is_read"] == 0) & (df["is_good_offer"] == 0)]
        .sort_values(by=["days_diff", "score"], ascending=[True, False])
        .reset_index(drop=True)
    )

    if applied_jobs.empty:
        st.write("❌ Aucune offre filtrée")
    else:
        for index, job in applied_jobs.iterrows():
            col1, col2 = st.columns([0.85, 0.15])

            with col1:
                score = int(job["score"])
                color = get_color(score)
                st.markdown(
                    f"""
                    <div style="margin: 20px 0; width: 100%; background-color: #eee; border-radius: 5px;">
                      <div style="width: {score}%; background-color: {color}; padding: 10px 0; border-radius: 5px; text-align: center; color: white; font-weight: bold;">
                        {score}%
                      </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
                if pd.notna(job["days_diff"]):
                    st.write(f"Publié il y a **{int(job['days_diff'])}** jours")
                else:
                    st.write("Date de publication non renseignée")

                with st.expander(
                    job["title"] + " | " + job["company"] + "\n" + job["comment"]
                ):
                    st.write(job["content"])
                    st.markdown(
                        f"[🔗 Lien vers l'offre]({job['link']})",
                        unsafe_allow_html=True,
                    )

            with col2:
                st.markdown(
                    "<div style='height: 85px;'></div>", unsafe_allow_html=True
                )
                if st.button("🔄 Restaurer", key=f"restore_bad_{index}"):
                    df.loc[df["link"] == job["link"], "is_good_offer"] = 1
                    save_data(df)
                    st.rerun()


def category_analysis_page(df):
    CONFIG_FILE = os.getenv("APP_CONFIG_FILE", "config.json")

    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            config = json.load(f)
    else:
        config = {"llm": {"provider": "Local"}, "categories": []}

    categories = config.get("categories", [])

    st.title("🎯 Analyse IA des catégories")

    st.subheader("📝 Catégories à extraire")
    categories_text = st.text_area(
        "Entrez une catégorie par ligne (ex : Télétravail, Salaire, Localisation, Tickets restaurant, etc.)",
        value="\n".join(categories),
        height=120,
    )
    categories = [c.strip() for c in categories_text.splitlines() if c.strip()]

    if st.button("💾 Sauvegarder les catégories"):
        config["categories"] = categories
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
        st.success("Catégories sauvegardées. Vous pouvez lancer l'analyse juste en dessous.")

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


def offer_readed_page(df):
    st.title("📄 Offres déjà lues et non postulées")

    applied_jobs = df[(df["is_read"] == 1) & (df["is_apply"] == 0)].reset_index(
        drop=True
    )

    if applied_jobs.empty:
        st.write("❌ Aucun poste n'a été lu.")
    else:
        for index, job in applied_jobs.iterrows():
            col1, col2 = st.columns([0.9, 0.1])

            with col1:
                with st.expander(job["title"] + " | " + job["company"]):
                    st.write(job["content"])
                    st.markdown(
                        f"[🔗 Lien vers l'offre]({job['link']})",
                        unsafe_allow_html=True,
                    )

            with col2:
                if st.button("🔄 Restaurer", key=f"restore_read_{index}"):
                    df.loc[df["link"] == job["link"], ["is_apply", "is_read"]] = 0
                    save_data(df)
                    st.rerun()


def offer_refused_page(df):
    st.title("🚫 Candidatures refusées")

    refused_jobs = df[df["is_refused"] == 1].reset_index(drop=True)

    if refused_jobs.empty:
        st.write("✅ Aucun poste n'a été marqué comme refusé.")
    else:
        for index, job in refused_jobs.iterrows():
            col1, col2 = st.columns([0.9, 0.2])

            with col1:
                with st.expander(job["title"] + " | " + job["company"]):
                    st.write(job["content"])
                    st.markdown(
                        f"[🔗 Lien vers l'offre]({job['link']})",
                        unsafe_allow_html=True,
                    )

            with col2:
                if st.button("🔄 Restaurer", key=f"restore_refused_{index}"):
                    df.loc[df["link"] == job["link"], "is_refused"] = 0
                    save_data(df)
                    st.rerun()


def offer_applied_page(df):
    st.title("📄 Candidatures en cours")

    applied_jobs = df[
        (df["is_apply"] == 1) & (df["is_refused"] == 0)
    ].reset_index(drop=True)

    if applied_jobs.empty:
        st.write("❌ Aucune candidature en cours.")
    else:
        for index, job in applied_jobs.iterrows():
            col1, col2, col3 = st.columns([0.8, 0.1, 0.2])

            with col1:
                with st.expander(job["title"] + " | " + job["company"]):
                    st.write(job["content"])
                    st.markdown(
                        f"[🔗 Lien vers l'offre]({job['link']})",
                        unsafe_allow_html=True,
                    )

            with col2:
                if st.button("🗑️", key=f"delete_apply_{index}"):
                    df.loc[df["link"] == job["link"], ["is_apply", "is_read"]] = 0
                    save_data(df)
                    st.rerun()

            with col3:
                if st.button("❌ Refusé", key=f"refused_apply_{index}"):
                    df.loc[df["link"] == job["link"], "is_refused"] = 1
                    save_data(df)
                    st.rerun()
