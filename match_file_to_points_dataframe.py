import logging
import pandas as pd

from pipeline_utils import load_players_master_dataframe_from_excel, detect_player_name_column, detect_role_column, build_points_breakdown_str_from_row

logger = logging.getLogger(__name__)
from data_source_to_player_stats_dict import extract_player_match_stats
from scoring_engine_functions import compute_player_match_fantasy_points_with_breakdown
from build_player_name_mapping_from_sources import build_player_name_mapping_dataframe

# dataframe build from match source block
def build_match_points_dataframe_from_data_source(match_file_path_str: str, master_excel_path_str: str, debug_bool: bool = True) -> pd.DataFrame:

    player_name_to_match_stats_dict = extract_player_match_stats(match_file_path_str)

    # ---------- ROLE MAPPING BLOCK (before scoring so duck penalty uses correct role) ----------

    mapping_df = build_player_name_mapping_dataframe(match_file_path_str, master_excel_path_str)

    # --- direct JSON name → canonical name lookup from Players List ---
    # Uses "JSON Player Name" column as authoritative override before fuzzy match kicks in
    try:
        players_list_df = pd.read_excel(master_excel_path_str, sheet_name="Players List")
        json_to_canonical = dict(zip(players_list_df["JSON Player Name"], players_list_df["Player Name"]))
        mapping_df["resolved_player_name_str"] = mapping_df["source_player_name_str"].map(json_to_canonical).fillna(mapping_df["resolved_player_name_str"])
    except Exception:
        pass

    players_master_df = load_players_master_dataframe_from_excel(master_excel_path_str, debug_bool=False)

    name_col = detect_player_name_column(players_master_df)
    role_col = detect_role_column(players_master_df)
    nation_col = "nation" if "nation" in players_master_df.columns else "team"

    master_name_to_role_dict = dict(zip(players_master_df[name_col], players_master_df[role_col]))
    master_name_to_nation_dict = dict(zip(players_master_df[name_col], players_master_df[nation_col]))

    resolved_name_to_role_dict = dict(zip(mapping_df["resolved_player_name_str"], mapping_df["resolved_player_name_str"].map(master_name_to_role_dict)))
    resolved_name_to_nation_dict = dict(zip(mapping_df["resolved_player_name_str"], mapping_df["resolved_player_name_str"].map(master_name_to_nation_dict)))

    source_to_resolved_name_dict = dict(zip(mapping_df["source_player_name_str"], mapping_df["resolved_player_name_str"]))

    # assign correct role to each stats dict NOW — before scoring runs
    for player_name_str, player_match_stats_dict in player_name_to_match_stats_dict.items():
        resolved_name = source_to_resolved_name_dict.get(player_name_str)
        if resolved_name:
            role = resolved_name_to_role_dict.get(resolved_name)
            if role:
                player_match_stats_dict["role"] = role

    # ---------- SCORING LOOP ----------

    player_rows_list = []

    for player_name_str, player_match_stats_dict in player_name_to_match_stats_dict.items():

        fantasy_points_int, scoring_breakdown_dict = compute_player_match_fantasy_points_with_breakdown(player_match_stats_dict)

        player_total_points_int = fantasy_points_int

        eligible_fantasy_points_int = player_total_points_int * player_match_stats_dict["played_match_flag"]

        player_row_dict = {
            "player_name_str": player_name_str,

            # extracted stats
            **player_match_stats_dict,
            **scoring_breakdown_dict,

            # scoring totals
            "fantasy_points_int": fantasy_points_int,
            "eligible_fantasy_points_int": eligible_fantasy_points_int,

            # --- FORCE MATCH METADATA FIELDS ---
            "match_id": player_match_stats_dict.get("match_id"),
            "nation_code": player_match_stats_dict.get("team_code"),
            "match_date": player_match_stats_dict.get("match_date"),
            "match_label": player_match_stats_dict.get("match_label"),
        }

        player_rows_list.append(player_row_dict)

    match_points_dataframe = pd.DataFrame(player_rows_list)

    # ---------- CANONICAL NAME / ROLE / TEAM ON DATAFRAME ----------

    match_points_dataframe["canonical_player_name"] = (
        match_points_dataframe["player_name_str"]
        .map(source_to_resolved_name_dict)
        .fillna(match_points_dataframe["player_name_str"])
    )

    match_points_dataframe["role"] = (
        match_points_dataframe["canonical_player_name"]
            .map(resolved_name_to_role_dict)
            .fillna("AR")
    )

    match_points_dataframe["team_code"] = (
        match_points_dataframe["canonical_player_name"]
            .map(resolved_name_to_nation_dict)
    )

    match_points_dataframe["nation_code"] = match_points_dataframe["team_code"]

    # ---------- VALIDATION REPORT ----------
    if debug_bool:
        logger.info("=== MATCH VALIDATION REPORT ===")
        logger.info("Players scored: %d", len(match_points_dataframe))
        logger.info("Played match flag = 1: %d", int((match_points_dataframe["played_match_flag"] == 1).sum()))
        logger.info("Played match flag = 0: %d", int((match_points_dataframe["played_match_flag"] == 0).sum()))
        if "role" in match_points_dataframe.columns:
            logger.info("Role distribution: %s", match_points_dataframe["role"].value_counts().to_dict())
        if "fantasy_points_int" in match_points_dataframe.columns:
            logger.info("Total fantasy points sum: %d", int(match_points_dataframe["fantasy_points_int"].sum()))
        if "economy_rate_points_int" in match_points_dataframe.columns:
            logger.info("Negative economy penalties applied: %d", int((match_points_dataframe["economy_rate_points_int"] < 0).sum()))
        if "strike_rate_points_int" in match_points_dataframe.columns:
            logger.info("Strike rate bonuses applied: %d", int((match_points_dataframe["strike_rate_points_int"] > 0).sum()))
   
    # build fpl style atomic component breakdown
    match_points_dataframe["points_breakdown_str"] = match_points_dataframe.apply(build_points_breakdown_str_from_row, axis=1)

    return match_points_dataframe

# standalone runner
if __name__ == "__main__":
    from pipeline_utils import EXCEL_WORKBOOK_PATH, JSON_FOLDER_PATH

    match_file_path_str = str(JSON_FOLDER_PATH) + "/1512733.json"
    master_excel_path_str = EXCEL_WORKBOOK_PATH

    df = build_match_points_dataframe_from_data_source(match_file_path_str, master_excel_path_str, debug_bool=True)

    output_file_name_str = "icc_mens_t20wc_2026_9teams_match_points_latest_from_json.csv"
    df.to_csv(output_file_name_str, index=False)
    logger.info("Wrote: %s, rows: %d", output_file_name_str, len(df))
