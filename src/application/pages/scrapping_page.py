import os
import json
import threading
import time
import streamlit as st

from main import update_store_data
from scraping.offer_cache import OfferCache


def scrapping_page():
    st.title("🔍 Scraping d'offres d’emploi")

    # -------------------------------
    # Helpers
    # -------------------------------
    def _to_int(x, default=0) -> int:
        try:
            if isinstance(x, bool):
                return int(x)
            if isinstance(x, (int, float)):
                return int(x)
            if isinstance(x, str):
                s = x.strip()
                if not s:
                    return default
                return int(float(s))
            return default
        except Exception:
            return default

    def _safe_ratio(current, total) -> float:
        c = _to_int(current, 0)
        t = _to_int(total, 0)
        if t <= 0:
            return 0.0
        r = c / float(t)
        if r < 0.0:
            return 0.0
        if r > 1.0:
            return 1.0
        return float(r)

    def _get_profile_id_from_config() -> str:
        cfg = os.getenv("APP_CONFIG_FILE", "config.json")
        return os.path.splitext(os.path.basename(cfg))[0] or "default"

    def _cache_path_for_profile(profile_id: str) -> str:
        return os.path.join("data", f"cache_{profile_id}.sqlite")

    def _read_ui_counts(profile_id: str) -> dict:
        cache_path = _cache_path_for_profile(profile_id)
        if not os.path.exists(cache_path):
            return {
                "accepted_scoring": 0,
                "pending_scoring": 0,
                "pending_url": 0,
                "failed_scoring": 0,
            }

        cache = OfferCache(cache_path)

        pending_scoring = cache.count_by_status("DETAILED")
        pending_url = cache.count_by_status("PENDING_URL")

        # ✅ ACCEPTÉES : scorées "white" + whitelist (bootstrap)
        accepted_scoring = cache.count_by_statuses(["SCORED_WHITE", "WHITE"])

        # ❌ REFUSÉES : scorées "black" + blacklist (bootstrap)
        failed_scoring = cache.count_by_statuses(["SCORED_BLACK", "BLACK"])

        return {
            "accepted_scoring": int(accepted_scoring),
            "pending_scoring": int(pending_scoring),
            "pending_url": int(pending_url),
            "failed_scoring": int(failed_scoring),
        }

    def _ensure_progress_dict_for_profile(profile_id: str):
        """Reset propre quand on change de profil."""
        if st.session_state.get("_active_profile_id") != profile_id:
            st.session_state._active_profile_id = profile_id
            st.session_state.progress_dict = {
                "WelcomeToTheJungle": (0, 1, 0, 1),
                "Linkedin": (0, 1, 0, 1),
                "Apec": (0, 1, 0, 1),
                "ServicePublic": (0, 1, 0, 1),
                "Reprise détail (URL)": (0, 1),
                "Reprise scoring (cache)": (0, 1),
                "Traitement des nouvelles offres (LLM)": (0, 1),
                "_ui_counts": _read_ui_counts(profile_id),
            }
            st.session_state._bar_keys = []
            st.session_state._bar_elems = {}

    # -------------------------------
    # Session state
    # -------------------------------
    if "scraping_running" not in st.session_state:
        st.session_state.scraping_running = False
    if "launch_clicked" not in st.session_state:
        st.session_state.launch_clicked = False
    if "scraping_started" not in st.session_state:
        st.session_state.scraping_started = False

    profile_id = _get_profile_id_from_config()

    if "progress_dict" not in st.session_state:
        st.session_state.progress_dict = {}

    _ensure_progress_dict_for_profile(profile_id)

    # refresh counts dès qu'on arrive sur la page
    try:
        st.session_state.progress_dict["_ui_counts"] = _read_ui_counts(profile_id)
    except Exception:
        pass

    # -------------------------------
    # Placeholders UI (anti duplication)
    # -------------------------------
    header_counts = st.empty()
    btn_row = st.empty()
    status_area = st.empty()
    bars_area = st.container()
    footer_msg = st.empty()

    # -------------------------------
    # Renderers
    # -------------------------------
    def _render_counts():
        c = st.session_state.progress_dict.get("_ui_counts", {}) or {}
        accepted = _to_int(c.get("accepted_scoring", 0), 0)
        pending_scoring = _to_int(c.get("pending_scoring", 0), 0)
        pending_url = _to_int(c.get("pending_url", 0), 0)
        failed_scoring = _to_int(c.get("failed_scoring", 0), 0)

        with header_counts:
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("✅ Acceptées (scoring)", accepted)
            col2.metric("🧠 En attente de scoring", pending_scoring)
            col3.metric("🔗 En attente de détail (URL)", pending_url)
            col4.metric("❌ Refusées (scoring)", failed_scoring)

    def _render_status():
        active_lines = []
        for name, val in st.session_state.progress_dict.items():
            if name in ("_ui_counts",):
                continue
            if not isinstance(val, (tuple, list)) or len(val) < 2:
                continue
            if isinstance(val[0], dict) or isinstance(val[1], dict):
                continue

            cur = _to_int(val[0], 0)
            total = _to_int(val[1], 0)
            if total <= 0:
                continue
            if cur < total:
                active_lines.append(f"- **{name}** : {cur}/{total}")

        with status_area:
            if active_lines:
                st.markdown("\n".join(active_lines))
            else:
                st.caption("Aucune étape en cours.")

    def _render_bars():
        keys = [k for k in st.session_state.progress_dict.keys() if k not in ("_ui_counts",)]

        if st.session_state.get("_bar_keys") != keys:
            st.session_state._bar_keys = keys
            st.session_state._bar_elems = {}
            with bars_area:
                for k in keys:
                    st.session_state._bar_elems[k] = st.progress(0.0, text=f"{k}: 0/1")

        for name in keys:
            val = st.session_state.progress_dict.get(name)
            bar = st.session_state._bar_elems.get(name)
            if bar is None:
                continue

            if not isinstance(val, (tuple, list)) or len(val) < 2:
                bar.progress(0.0, text=f"{name}: -")
                continue

            if len(val) >= 4:
                oc = _to_int(val[0], 0)
                ot = max(_to_int(val[1], 0), 1)
                pc = _to_int(val[2], 0)
                pt = max(_to_int(val[3], 0), 1)
                ratio = _safe_ratio(oc, ot)
                bar.progress(ratio, text=f"{name}: {oc}/{ot} offres — Pages: {pc}/{pt}")
            else:
                c = _to_int(val[0], 0)
                t = max(_to_int(val[1], 0), 1)
                ratio = _safe_ratio(c, t)
                bar.progress(ratio, text=f"{name}: {c}/{t}")

    def _sync_counts():
        try:
            st.session_state.progress_dict["_ui_counts"] = _read_ui_counts(profile_id)
        except Exception:
            pass

    # -------------------------------
    # Button
    # -------------------------------
    with btn_row:
        if st.button(
            "🚀 Lancer le scraping",
            key="btn_launch_scraping_unique",
            disabled=st.session_state.scraping_running,
        ):
            if not st.session_state.launch_clicked:
                st.session_state.launch_clicked = True
                st.session_state.scraping_running = True
                st.rerun()

    # -------------------------------
    # Initial draw
    # -------------------------------
    _sync_counts()
    _render_counts()
    _render_status()
    _render_bars()

    # -------------------------------
    # Run scraping
    # -------------------------------
    if st.session_state.scraping_running and not st.session_state.scraping_started:
        st.session_state.scraping_started = True
        result_container = {}

        def run(progress_dict):
            success, error_msg = update_store_data(progress_dict)
            result_container["success"] = success
            result_container["error"] = error_msg

        thread = threading.Thread(target=run, args=(st.session_state.progress_dict,))
        thread.start()

        while thread.is_alive():
            _sync_counts()
            _render_counts()
            _render_status()
            _render_bars()
            time.sleep(0.2)

        _sync_counts()
        _render_counts()
        _render_status()
        _render_bars()

        if result_container.get("success"):
            footer_msg.success("🎉 Scraping terminé avec succès !")
        else:
            error_msg = result_container.get("error") or "Erreur inconnue. Voir les logs."
            with st.expander("Détails techniques de l'erreur"):
                st.code(error_msg)
            footer_msg.error("❌ Une erreur est survenue pendant le scraping.")

        st.session_state.scraping_running = False
        st.session_state.launch_clicked = False
        st.session_state.scraping_started = False

    configuration_page()


def configuration_page():
    CONFIG_FILE = os.getenv("APP_CONFIG_FILE", os.path.join("config", "default.json"))

    def load_config():
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        else:
            cfg = {
                "keywords": [],
                "url": {"wttj": "", "apec": "", "linkedin": "", "sp": ""},
                "launch_scrap": {"wttj": False, "apec": False, "linkedin": False, "sp": False},
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
            }

        platform_keys = ["wttj", "apec", "linkedin", "sp"]
        id_w = cfg.setdefault("id_whitelist", {})
        id_b = cfg.setdefault("id_blacklist", {})
        for k in platform_keys:
            id_w.setdefault(k, [])
            id_b.setdefault(k, [])
        return cfg

    def save_config(config):
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2, ensure_ascii=False)

    st.title("🔧 Configuration du Scraper")
    st.caption("Toutes les modifications sont sauvegardées automatiquement.")

    config = load_config()

    st.header("🔑 Mots-clés")
    keywords = st.text_area("Entrez les mots-clés (un par ligne)", "\n".join(config["keywords"]))
    config["keywords"] = [k.strip() for k in keywords.splitlines() if k.strip()]

    st.header("🔗 URLs des sites")
    config["url"]["wttj"] = st.text_input("WTTJ URL", config["url"]["wttj"])
    config["url"]["apec"] = st.text_input("APEC URL", config["url"]["apec"])
    config["url"]["linkedin"] = st.text_input("LinkedIn URL", config["url"]["linkedin"])
    config["url"]["sp"] = st.text_input("Service Public URL", config["url"]["sp"])

    st.header("📡 Lancer le scraping sur :")
    config["launch_scrap"]["wttj"] = st.checkbox("WTTJ", config["launch_scrap"]["wttj"])
    config["launch_scrap"]["apec"] = st.checkbox("APEC", config["launch_scrap"]["apec"])
    config["launch_scrap"]["linkedin"] = st.checkbox("LinkedIn", config["launch_scrap"]["linkedin"])
    config["launch_scrap"]["sp"] = st.checkbox("Service Public", config["launch_scrap"]["sp"])

    st.header("⚙️ Options générales")
    config["use_multithreading"] = st.checkbox(
        "Utiliser le multithreading (scraper plusieurs sites en même temps)",
        config["use_multithreading"],
    )
    config["use_llm"] = st.checkbox("Utiliser un LLM", config["use_llm"])

    if config["use_llm"]:
        st.subheader("🧠 Paramètres du LLM")

        providers = ["Local", "ChatGPT", "Mistral"]
        current_provider = config["llm"].get("provider", "Local")
        if current_provider not in providers:
            current_provider = "Local"

        config["llm"]["provider"] = st.radio(
            "Choisissez le fournisseur LLM :",
            providers,
            index=providers.index(current_provider),
        )

        if config["llm"]["provider"] == "ChatGPT":
            config["llm"]["gpt_api_key"] = st.text_input("Clé API GPT", config["llm"].get("gpt_api_key", ""))

        elif config["llm"]["provider"] == "Mistral":
            config["llm"]["mistral_api_key"] = st.text_input("Clé API Mistral", config["llm"].get("mistral_api_key", ""))

        config["llm"]["generate_score"] = True
        st.markdown("✅ Le scoring est automatiquement activé quand le LLM est utilisé.")

        config["llm"]["prompt_score"] = st.text_area(
            "Prompt de scoring :",
            config["llm"].get("prompt_score", ""),
            height=250,
        )

        config["llm"]["generate_custom_profile"] = st.checkbox(
            "Générer un profil en fonction de l'offre",
            config["llm"].get("generate_custom_profile", False),
        )
        if config["llm"]["generate_custom_profile"]:
            config["llm"]["prompt_custom_profile"] = st.text_area(
                "Prompt profil :",
                config["llm"].get("prompt_custom_profile", ""),
                height=180,
            )
            config["llm"]["cv"] = st.text_area(
                "CV :",
                config["llm"].get("cv", ""),
                height=250,
            )
    else:
        config["llm"]["generate_score"] = False

    st.header("🧹 Nettoyer les blacklists d'offres")
    platform_labels = {"wttj": "Welcome to the Jungle", "apec": "APEC", "linkedin": "LinkedIn", "sp": "Service Public"}

    col_b1, col_b2 = st.columns(2)
    with col_b1:
        for key in ["wttj", "apec"]:
            if st.button(f"Vider la blacklist {platform_labels[key]}", key=f"clear_blacklist_{key}"):
                config["id_blacklist"][key] = []
                save_config(config)
                st.success(f"Blacklist {platform_labels[key]} vidée.")

    with col_b2:
        for key in ["linkedin", "sp"]:
            if st.button(f"Vider la blacklist {platform_labels[key]}", key=f"clear_blacklist_{key}"):
                config["id_blacklist"][key] = []
                save_config(config)
                st.success(f"Blacklist {platform_labels[key]} vidée.")

    save_config(config)
