import streamlit as st
import os


def save_data(df, data_file=None):
    if data_file is None:
        data_file = os.getenv("JOB_DATA_FILE", "data/job.csv")
    df.to_csv(data_file, sep=";", index=False, encoding="utf-8")


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
