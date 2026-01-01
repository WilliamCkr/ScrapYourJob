import streamlit as st
import os


def save_data(df, data_file=None):
    if data_file is None:
        data_file = os.getenv("JOB_DATA_FILE", "data/job.csv")
    df.to_csv(data_file, sep=";", index=False, encoding="utf-8")


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
