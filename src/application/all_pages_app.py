from application.pages.scrapping_page import scrapping_page
from application.pages.new_offer_page import new_offer_page
from application.pages.offer_gpt_filter_page import offer_gpt_filter_page
from application.pages.category_analysis_page import category_analysis_page
from application.pages.offer_readed_page import offer_readed_page
from application.pages.offer_refused_page import offer_refused_page
from application.pages.offer_applied_page import offer_applied_page
import streamlit as st
import pandas as pd
import os


def load_data():
    data_file = os.getenv("JOB_DATA_FILE", "data/job.csv")
    if os.path.exists(data_file):
        return pd.read_csv(data_file, sep=";")
    return pd.DataFrame()


def all_pages_app():
    df = load_data()

    page = st.sidebar.selectbox(
        "Navigation",
        [
            "Scraping & configuration",
            "Nouvelles offres",
            "Offres filtrées (IA)",
            "Analyse catégories IA",
            "Offres lues",
            "Offres refusées",
            "Candidatures en cours",
        ],
    )

    if page == "Scraping & configuration":
        scrapping_page()

    elif page == "Nouvelles offres":
        new_offer_page(df)

    elif page == "Offres filtrées (IA)":
        offer_gpt_filter_page(df)

    elif page == "Analyse catégories IA":
        category_analysis_page(df)

    elif page == "Offres lues":
        offer_readed_page(df)

    elif page == "Offres refusées":
        offer_refused_page(df)

    elif page == "Candidatures en cours":
        offer_applied_page(df)
