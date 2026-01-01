import streamlit as st
import os


def save_data(df, data_file=None):
    if data_file is None:
        data_file = os.getenv("JOB_DATA_FILE", "data/job.csv")
    df.to_csv(data_file, sep=";", index=False, encoding="utf-8")


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
