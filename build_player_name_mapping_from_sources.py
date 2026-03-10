import json
import pandas as pd
from rapidfuzz import process, fuzz

from pipeline_utils import load_players_master_dataframe_from_excel, detect_player_name_column, get_delivery_info_from_wrapper

# ---------- cricsheet player extraction ----------

def load_player_name_list_from_cricsheet_json(match_json_path_str):
    with open(match_json_path_str, "r") as match_file:
        match_data_dict = json.load(match_file)

    match_info_dict = match_data_dict["info"]
    match_date_str = match_info_dict["dates"][0]
    venue_str = match_info_dict["venue"]
    team_a_str, team_b_str = match_info_dict["teams"][0], match_info_dict["teams"][1]

    player_name_set = set()

    for innings_dict in match_data_dict["innings"]:
        for over_dict in innings_dict["overs"]:
            for delivery_wrapper_dict in over_dict["deliveries"]:
                delivery_info_dict = (
                    delivery_wrapper_dict
                    if "batter" in delivery_wrapper_dict
                    else get_delivery_info_from_wrapper(delivery_wrapper_dict)
                )

                if delivery_info_dict.get("batter"):
                    player_name_set.add(delivery_info_dict["batter"])
                if delivery_info_dict.get("bowler"):
                    player_name_set.add(delivery_info_dict["bowler"])

                for wicket_dict in delivery_info_dict.get("wickets", []):
                    if wicket_dict.get("player_out"):
                        player_name_set.add(wicket_dict["player_out"])

    return sorted(player_name_set)

# ---------- name normalization helpers ----------

def normalize_player_name(name_str):
    if not isinstance(name_str, str):
        return ""
    return name_str.lower().replace(".", "").replace("-", " ").strip()

def extract_last_name_token(name_str):
    tokens = normalize_player_name(name_str).split()
    return tokens[-1] if tokens else ""

def extract_first_initial(name_str):
    normalized = normalize_player_name(name_str)
    return normalized[0] if normalized else ""

# ---------- candidate narrowing ----------

def build_candidate_name_list(source_name_str, master_name_list):
    src_last = extract_last_name_token(source_name_str)
    src_init = extract_first_initial(source_name_str)

    candidates = []
    for master_name in master_name_list:
        if not isinstance(master_name, str):
            continue
        if extract_last_name_token(master_name) == src_last and extract_first_initial(master_name) == src_init:
            candidates.append(master_name)

    return candidates if candidates else master_name_list

# ---------- fuzzy match ----------

def build_single_name_match(source_name_str, master_name_list, score_cutoff_int):
    candidates = build_candidate_name_list(source_name_str, master_name_list)

    match = process.extractOne(source_name_str, candidates, scorer=fuzz.token_sort_ratio)

    if not match:
        return {
            "source_player_name_str": source_name_str,
            "matched_player_name_str": None,
            "match_score_int": 0,
            "needs_manual_review_bool": True,
        }

    matched_name, score, _ = match

    return {
        "source_player_name_str": source_name_str,
        "matched_player_name_str": matched_name,
        "match_score_int": int(score),
        "needs_manual_review_bool": score < score_cutoff_int,
    }

# ---------- dataframe builder ----------

def build_player_name_mapping_dataframe(match_json_path_str, master_excel_path_str, score_cutoff_int=85):

    source_names = load_player_name_list_from_cricsheet_json(match_json_path_str)
    master_df = load_players_master_dataframe_from_excel(master_excel_path_str, debug_bool=False)

    name_col = detect_player_name_column(master_df)
    master_names = master_df[name_col].dropna().unique().tolist()

    rows = [build_single_name_match(n, master_names, score_cutoff_int) for n in source_names]
    df = pd.DataFrame(rows)

    df["resolved_player_name_str"] = df["matched_player_name_str"].fillna(df["source_player_name_str"])
    return df

if __name__ == "__main__":
    print(r"build_player_name_mapping_from_sources.py seems to be working..")