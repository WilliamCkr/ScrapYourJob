import os
import json
import csv
import traceback
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from openai import OpenAI
from mistralai import Mistral

from scraping.WelcomeToTheJungle import WelcomeToTheJungle
from scraping.Apec import Apec
from scraping.Linkedin import Linkedin
from scraping.ServicePublic import ServicePublic
from scraping.utils import measure_time, add_LLM_comment, SCORE_THRESHOLD
from scraping.offer_cache import OfferCache

from detail_fetcher import fetch_detail_by_source


def _to_bool(v):
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.lower() in ("true", "1", "yes", "on")
    if isinstance(v, (int, float)):
        return v != 0
    return False


def _append_rows_csv(rows, data_file: str, sep: str = ";"):
    if not rows:
        return

    data_dir = os.path.dirname(data_file) or "data"
    os.makedirs(data_dir, exist_ok=True)
    file_exists = os.path.exists(data_file) and os.path.getsize(data_file) > 0

    fieldnames = []
    seen = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                fieldnames.append(k)
                seen.add(k)

    if file_exists:
        try:
            with open(data_file, "r", encoding="utf-8", newline="") as f:
                reader = csv.reader(f, delimiter=sep)
                existing_header = next(reader, None)
            if existing_header:
                fieldnames = existing_header
        except Exception:
            pass

    with open(data_file, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=sep, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in fieldnames})


def _init_llm_client(config):
    use_llm = config.get("use_llm", False)
    llm_config = config.get("llm", {})
    client = None

    if use_llm:
        provider = llm_config.get("provider")
        if provider == "ChatGPT":
            client = OpenAI(api_key=llm_config.get("gpt_api_key"))
        elif provider == "Mistral":
            client = Mistral(api_key=llm_config.get("mistral_api_key"))
        elif provider == "Local":
            client = None

    return use_llm, llm_config, client


def _status_from_score(score: int, is_good: int) -> str:
    return "SCORED_WHITE" if (score >= SCORE_THRESHOLD and is_good == 1) else "SCORED_BLACK"


def _update_id_lists(config, platform_keys, src: str, offer_id: str, score: int, is_good: int):
    if not offer_id or src not in platform_keys:
        return

    llm_config = config.get("llm", {})
    if not llm_config.get("generate_score"):
        return

    id_whitelist = config.setdefault("id_whitelist", {})
    id_blacklist = config.setdefault("id_blacklist", {})
    for k in platform_keys:
        id_whitelist.setdefault(k, [])
        id_blacklist.setdefault(k, [])

    if score >= SCORE_THRESHOLD and is_good == 1:
        if offer_id not in id_whitelist[src]:
            id_whitelist[src].append(offer_id)
        if offer_id in id_blacklist[src]:
            id_blacklist[src].remove(offer_id)
    else:
        if offer_id not in id_blacklist[src]:
            id_blacklist[src].append(offer_id)


def _row_from_cache_offer(o: dict):
    return {
        "offer_id": o.get("offer_id", "") or "",
        "source": o.get("source", "") or "",
        "link": o.get("url", "") or "",
        "title": o.get("title", "") or "",
        "content": o.get("description", "") or "",
    }


def _resume_pending_details(cache: OfferCache, limit: int = 200, on_tick=None):
    pendings = cache.list_by_status("PENDING_URL", limit=limit)
    detailed_rows = []

    for idx, o in enumerate(pendings, start=1):
        offer_id = o.get("offer_id", "")
        source = (o.get("source", "") or "").lower()
        url = o.get("url", "") or ""

        if not offer_id or not url:
            if offer_id:
                cache.mark_error(offer_id, status="ERROR_DETAIL")
            if on_tick:
                on_tick(idx)
            continue

        data = fetch_detail_by_source(source, url)
        if not data:
            cache.mark_error(offer_id, status="ERROR_DETAIL")
            if on_tick:
                on_tick(idx)
            continue

        title = (data.get("title") or "").strip()
        desc = (data.get("description") or "").strip()

        if not title or not desc:
            cache.mark_error(offer_id, status="ERROR_DETAIL")
            if on_tick:
                on_tick(idx)
            continue

        cache.upsert_detail(
            offer_id=offer_id,
            source=source,
            url=url,
            title=title,
            description=desc,
            status="DETAILED",
        )

        detailed_rows.append({
            "offer_id": offer_id,
            "source": source,
            "link": url,
            "title": title,
            "content": desc,
        })

        if on_tick:
            on_tick(idx)

    return detailed_rows


@measure_time
def get_all_job(progress_dict, all_platforms, is_multiproc, cache=None, profile_id: str = ""):
    def run_source(source_class):
        name = source_class.__name__
        print(f"[SCRAP] Démarrage {name}")
        platform = source_class()

        def update_callback(*args):
            if not args:
                return
            if len(args) >= 4:
                offers_current, offers_total, pages_current, pages_total = args[:4]
                progress_dict[name] = (
                    int(offers_current),
                    int(offers_total) if int(offers_total) > 0 else max(int(offers_current), 1),
                    int(pages_current),
                    int(pages_total) if int(pages_total) > 0 else max(int(pages_current), 1),
                )
            else:
                current, total = args[:2]
                progress_dict[name] = (
                    int(current),
                    int(total) if int(total) > 0 else max(int(current), 1),
                )

        try:
            df = platform.getJob(update_callback=update_callback, cache=cache, profile_id=profile_id)
        except TypeError:
            df = platform.getJob(update_callback=update_callback)

        print(f"[SCRAP] Fin {name}")
        return df

    if is_multiproc and len(all_platforms) > 1:
        print(f"[SCRAP] Mode multi-thread ({len(all_platforms)} workers)")
        with ThreadPoolExecutor(max_workers=len(all_platforms)) as executor:
            results = list(executor.map(run_source, all_platforms))
    else:
        print("[SCRAP] Mode séquentiel")
        results = [run_source(cls) for cls in all_platforms]

    if not results:
        return pd.DataFrame(columns=["title", "content", "company", "link", "date", "hash", "source", "offer_id"])
    return pd.concat(results, ignore_index=True)


@measure_time
def update_store_data(progress_dict):
    try:
        def ui_log(role: str, msg: str):
            if not role or not msg:
                return
            logs = progress_dict.setdefault("_logs", [])
            last = logs[-1] if logs else None
            if last and last.get("role") == role and last.get("msg") == msg:
                return
            logs.append({"role": role, "msg": msg})
            if len(logs) > 200:
                del logs[:-200]

        ui_log("INFO", "Initialisation…")

        config_file = os.getenv("APP_CONFIG_FILE", "config.json")
        with open(config_file, "r", encoding="utf-8") as f:
            config = json.load(f)

        profile_id = os.path.splitext(os.path.basename(config_file))[0] or "default"

        cache_path = os.path.join("data", f"cache_{profile_id}.sqlite")
        cache = OfferCache(cache_path)

        # >>> ROLLBACK : annule les faux refus (SCORED_BLACK score=0/is_good=0)
        rolled = cache.rollback_scoring_black_to_detailed()
        if rolled:
            ui_log("WARN", f"Rollback effectué: {rolled} offres repassées en DETAILED (scoring KO).")
            print(f"[ROLLBACK] {rolled} offres repassées en DETAILED (scoring KO).")

        def _push_ui_counts():
            pending_scoring = cache.count_by_status("DETAILED")
            pending_url = cache.count_by_status("PENDING_URL")
            existing_treated = cache.count_by_statuses(
                ["SCORED_WHITE", "SCORED_BLACK", "WHITE", "BLACK", "KNOWN"]
            )
            failed_scoring = cache.count_by_statuses(["SCORED_BLACK", "BLACK"])
            progress_dict["_ui_counts"] = {
                "existing_treated": int(existing_treated),
                "pending_scoring": int(pending_scoring),
                "pending_url": int(pending_url),
                "failed_scoring": int(failed_scoring),
            }

        _push_ui_counts()

        launch_scrap = config.get("launch_scrap", {})
        active_platforms = []
        if _to_bool(launch_scrap.get("wttj", False)):
            active_platforms.append(WelcomeToTheJungle)
        if _to_bool(launch_scrap.get("linkedin", False)):
            active_platforms.append(Linkedin)
        if _to_bool(launch_scrap.get("apec", False)):
            active_platforms.append(Apec)
        if _to_bool(launch_scrap.get("sp", False)):
            active_platforms.append(ServicePublic)

        if not active_platforms:
            ui_log("WARN", "Aucune plateforme sélectionnée.")
            print("[SCRAP] Aucune plateforme sélectionnée, rien à faire.")
            return True, ""

        platform_keys = ["wttj", "apec", "linkedin", "sp"]
        data_file = os.getenv("JOB_DATA_FILE", "data/job.csv")

        # Bootstrap CSV -> KNOWN
        if os.path.exists(data_file):
            try:
                df_known = pd.read_csv(data_file, sep=";", encoding="utf-8")
                if "offer_id" in df_known.columns and "link" in df_known.columns:
                    for _, r in df_known[["offer_id", "link"]].dropna().iterrows():
                        cache.upsert_url(str(r["offer_id"]), "known", str(r["link"]), "KNOWN")
                ui_log("INFO", "Bootstrap CSV -> cache (KNOWN) OK.")
            except Exception as e:
                ui_log("WARN", f"Bootstrap CSV ignoré: {e}")
                print(f"[CACHE] bootstrap CSV ignoré : {e}")

        # Bootstrap listes
        try:
            id_whitelist = config.setdefault("id_whitelist", {})
            id_blacklist = config.setdefault("id_blacklist", {})
            for k in platform_keys:
                id_whitelist.setdefault(k, [])
                id_blacklist.setdefault(k, [])
                cache.bootstrap_ids(id_blacklist.get(k, []), k, "BLACK")
                cache.bootstrap_ids(id_whitelist.get(k, []), k, "WHITE")
            ui_log("INFO", "Bootstrap listes (black/white) OK.")
        except Exception as e:
            ui_log("WARN", f"Bootstrap listes ignoré: {e}")
            print(f"[CACHE] bootstrap listes ignoré : {e}")

        _push_ui_counts()

        use_llm, llm_config, client = _init_llm_client(config)
        ui_log("INFO", f"LLM: {'ON' if use_llm else 'OFF'}.")

        # 0) Reprise PENDING_URL
        resume_pending_limit = int(config.get("resume_pending_limit", 200))
        progress_dict["Reprise détail (URL)"] = (0, max(resume_pending_limit, 1))
        ui_log("STEP", "Reprise détail (PENDING_URL) -> DETAILED…")

        def _on_pending_tick(i):
            progress_dict["Reprise détail (URL)"] = (
                i,
                max(len(cache.list_by_status("PENDING_URL", limit=resume_pending_limit)), 1),
            )
            _push_ui_counts()

        pending_rows = _resume_pending_details(cache, limit=resume_pending_limit, on_tick=_on_pending_tick)
        if pending_rows:
            ui_log("INFO", f"{len(pending_rows)} offres détaillées depuis PENDING_URL.")
            print(f"[RESUME] {len(pending_rows)} offres PENDING_URL détaillées.")
        else:
            ui_log("INFO", "Aucune offre PENDING_URL à détailler.")
        _push_ui_counts()

        # 1) Reprise scoring DETAILED
        resumed = cache.list_not_scored(limit=int(config.get("resume_limit", 1000)))
        kept_rows_resume = []

        if resumed:
            ui_log("STEP", f"Reprise scoring cache ({len(resumed)} offres)…")
            print(f"[RESUME] {len(resumed)} offres DETAILED à scorer.")
            progress_dict["Reprise scoring (cache)"] = (0, len(resumed))

            for i, o in enumerate(resumed, start=1):
                row = _row_from_cache_offer(o)
                offer_id = row.get("offer_id", "")
                src = (row.get("source", "") or "").lower()

                if not row.get("content", "").strip():
                    if offer_id:
                        cache.mark_error(offer_id, status="ERROR_DETAIL")
                    progress_dict["Reprise scoring (cache)"] = (i, len(resumed))
                    _push_ui_counts()
                    continue

                if use_llm:
                    row = add_LLM_comment(client, llm_config, row)

                score = int(row.get("score", 0) or 0)
                is_good = int(row.get("is_good_offer", 0) or 0)

                # >>> SI score=-1 (non scoré), on NE CHANGE PAS le status (reste DETAILED)
                if score < 0:
                    progress_dict["Reprise scoring (cache)"] = (i, len(resumed))
                    if i % 3 == 0:
                        _push_ui_counts()
                    continue

                _update_id_lists(config, platform_keys, src, offer_id, score, is_good)

                status = _status_from_score(score, is_good)
                if offer_id:
                    cache.set_scoring(offer_id, score=score, is_good=is_good, status=status)

                if not (is_good == 0 and score < SCORE_THRESHOLD):
                    kept_rows_resume.append(row)

                progress_dict["Reprise scoring (cache)"] = (i, len(resumed))
                if i % 3 == 0:
                    _push_ui_counts()

            _append_rows_csv(kept_rows_resume, data_file)
            ui_log("INFO", f"Reprise scoring OK (kept={len(kept_rows_resume)}).")
            print(f"[RESUME] kept={len(kept_rows_resume)}")
            _push_ui_counts()
        else:
            ui_log("INFO", "Aucune offre en reprise scoring (cache).")

        # 2) Scraping
        ui_log("STEP", "Scraping des plateformes…")
        new_df = get_all_job(
            progress_dict,
            active_platforms,
            config.get("use_multithreading", False),
            cache=cache,
            profile_id=profile_id,
        )

        _push_ui_counts()

        if new_df is None or new_df.empty:
            ui_log("INFO", "Aucune nouvelle offre.")
            print("[SCRAP] Aucune nouvelle offre.")
            with open(config_file, "w", encoding="utf-8") as f:
                json.dump(config, f, ensure_ascii=False, indent=2)
            return True, ""

        if "content" in new_df.columns:
            new_df["content"] = new_df["content"].fillna("").astype(str)
        else:
            new_df["content"] = ""

        # 3) Streaming scoring nouvelles offres
        kept_rows = []
        total = len(new_df)
        progress_dict["Traitement des nouvelles offres (LLM)"] = (0, total)
        ui_log("STEP", f"Traitement des nouvelles offres ({total})…")

        for i, row in enumerate(new_df.to_dict(orient="records"), start=1):
            offer_id = str(row.get("offer_id") or "").strip()
            src = (row.get("source", "") or "").lower()

            if offer_id:
                stt = cache.get_status(offer_id)
                if stt in {"BLACK", "WHITE", "SCORED_WHITE", "SCORED_BLACK", "KNOWN"}:
                    progress_dict["Traitement des nouvelles offres (LLM)"] = (i, total)
                    continue

            if not str(row.get("content", "")).strip():
                if offer_id:
                    cache.mark_error(offer_id, status="ERROR_DETAIL")
                progress_dict["Traitement des nouvelles offres (LLM)"] = (i, total)
                _push_ui_counts()
                continue

            if use_llm:
                row = add_LLM_comment(client, llm_config, row)

            score = int(row.get("score", 0) or 0)
            is_good = int(row.get("is_good_offer", 0) or 0)

            # >>> SI score=-1 (non scoré), on laisse DETAILED et on n'append pas
            if score < 0:
                progress_dict["Traitement des nouvelles offres (LLM)"] = (i, total)
                if i % 3 == 0:
                    _push_ui_counts()
                continue

            _update_id_lists(config, platform_keys, src, offer_id, score, is_good)

            if offer_id:
                status = _status_from_score(score, is_good)
                cache.set_scoring(offer_id, score=score, is_good=is_good, status=status)

            if not (is_good == 0 and score < SCORE_THRESHOLD):
                kept_rows.append(row)

            progress_dict["Traitement des nouvelles offres (LLM)"] = (i, total)
            if i % 3 == 0:
                _push_ui_counts()

        _append_rows_csv(kept_rows, data_file)
        _push_ui_counts()

        # 4) Save config
        with open(config_file, "w", encoding="utf-8") as f:
            json.dump(config, f, ensure_ascii=False, indent=2)

        ui_log("INFO", f"Terminé. New={len(new_df)} kept={len(kept_rows)}.")
        print(
            f"[DONE] pending_detailed={len(pending_rows)} "
            f"resumed={len(resumed)} kept_resume={len(kept_rows_resume)} "
            f"new={len(new_df)} kept_new={len(kept_rows)}"
        )
        return True, ""

    except Exception as e:
        traceback.print_exc()
        try:
            progress_dict.setdefault("_logs", []).append({"role": "ERROR", "msg": str(e)})
        except Exception:
            pass
        return False, str(e)


if __name__ == "__main__":
    progress_dict = {
        "WelcomeToTheJungle": (0, 1),
        "Linkedin": (0, 1),
        "Apec": (0, 1),
        "ServicePublic": (0, 1),
        "Reprise détail (URL)": (0, 1),
        "Reprise scoring (cache)": (0, 1),
        "Traitement des nouvelles offres (LLM)": (0, 1),
    }
    update_store_data(progress_dict)
