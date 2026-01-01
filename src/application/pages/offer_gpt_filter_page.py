import streamlit as st
import pandas as pd
import os


def save_data(df, data_file=None):
    if data_file is None:
        data_file = os.getenv("JOB_DATA_FILE", "data/job.csv")
    df.to_csv(data_file, sep=";", index=False, encoding="utf-8")


def get_color(score):
    r = int(255 - (score * 2.55))
    g = int(score * 2.55)
    return f"rgb({r},{g},0)"


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
