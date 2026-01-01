import streamlit as st
import pandas as pd
import json
import os


def save_data(df, data_file=None):
    if data_file is None:
        data_file = os.getenv("JOB_DATA_FILE", "data/job.csv")
    df.to_csv(data_file, sep=";", index=False, encoding="utf-8")


def get_color(score):
    r = int(255 - (score * 2.55))
    g = int(score * 2.55)
    return f"rgb({r},{g},0)"


def new_offer_page(df):
    CONFIG_FILE = os.getenv("APP_CONFIG_FILE", "config.json")
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            config = json.load(f)
        categories = config.get("categories", [])
    except Exception:
        categories = []

    st.title("📌 Offres pertinentes")

    # base : offres pertinentes et non lues
    unread_jobs = df[
        (df.get("is_read", 0) == 0) & (df.get("is_good_offer", 0) == 1)
    ].copy()

    # ⚠️ ne garder que celles qui ont une vraie description
    if "content" in unread_jobs.columns:
        unread_jobs["content"] = unread_jobs["content"].fillna("").astype(str)
        unread_jobs = unread_jobs[unread_jobs["content"].str.strip() != ""].copy()

    if unread_jobs.empty:
        st.write("✅ Aucune nouvelle offre pertinente à afficher.")
        return

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
