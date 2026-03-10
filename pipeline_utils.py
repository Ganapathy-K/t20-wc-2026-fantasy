import json
import os
import pandas as pd
from pathlib import Path

# --- Configurable paths — update these if your files live elsewhere ---
EXCEL_WORKBOOK_PATH = r"C:/Users/ganap/Downloads/ICC T20 WC 2026 Auction Game.xlsx"
JSON_FOLDER_PATH    = str(Path(__file__).resolve().parent / "icc_mens_t20_world_cup_male_json")

# load players master dataframe from excel workbook
def load_players_master_dataframe_from_excel(master_excel_path_str, sheet_name_str=None, debug_bool=False):
    excel_file_obj = pd.ExcelFile(master_excel_path_str)
    if sheet_name_str is None:
        sheet_name_str = next(
            name for name in excel_file_obj.sheet_names
            if "player" in name.lower()
        )
    players_master_dataframe = excel_file_obj.parse(sheet_name_str)
    players_master_dataframe.columns = players_master_dataframe.columns.str.strip().str.lower().str.replace(" ", "_")

    if "player_name" in players_master_dataframe.columns:
        players_master_dataframe["canonical_player_name"] = players_master_dataframe["player_name"]

    if debug_bool:
        print("Detected sheet:", sheet_name_str)
        print("Detected columns:", players_master_dataframe.columns.tolist())
    return players_master_dataframe

# unwrap cricsheet delivery wrapper (supports both formats)
def get_delivery_info_from_wrapper(delivery_wrapper_dict):
    if "batter" in delivery_wrapper_dict:
        return delivery_wrapper_dict
    return next(iter(delivery_wrapper_dict.values()))

# legal delivery check helper (wide + no-ball excluded)
def is_legal_delivery_bool_from_delivery(delivery_info_dict):
    extras_dict = delivery_info_dict.get("extras", {})
    return not ("wides" in extras_dict or "noballs" in extras_dict)

def detect_player_name_column(dataframe_obj):
    for column_name_str in dataframe_obj.columns:
        normalized_column_name_str = column_name_str.lower().replace(" ", "_")
        if normalized_column_name_str in ("player", "player_name", "name") or ("player" in normalized_column_name_str and "name" in normalized_column_name_str):
            return column_name_str
    return None

def detect_role_column(dataframe_obj):
    for column_name_str in dataframe_obj.columns:
        normalized_column_name_str = column_name_str.lower().replace(" ", "_")
        if normalized_column_name_str in ("role", "position", "skill"):
            return column_name_str
    return None

def build_resolved_player_to_role_mapping(name_mapping_dataframe, players_master_dataframe, player_name_column_str, player_role_column_str):
    # index master by canonical player name for fast lookup
    master_indexed_df = players_master_dataframe.set_index(player_name_column_str)

    source_to_meta_mapping = {}

    for _, row in name_mapping_dataframe.iterrows():
        source_name = row["source_player_name_str"]
        resolved_name = row["resolved_player_name_str"]

        if resolved_name in master_indexed_df.index:
            master_row = master_indexed_df.loc[resolved_name]

            source_to_meta_mapping[source_name] = {
                "role": master_row.get(player_role_column_str),
                "team": master_row.get("team"),
                "nation": master_row.get("nation"),
                "group": master_row.get("group")
            }
        else:
            source_to_meta_mapping[source_name] = {}

    return source_to_meta_mapping

def build_fpl_style_pairs_str_from_row(player_row_series):

    pairs = [
        ("Runs", "runs", "runs_points_int"),
        ("Fours", "fours", "boundary_points_int"),
        ("Sixes", "sixes", "six_points_int"),
        ("Runs milestone bonus", "runs", "milestone_bonus_points_int"),
        ("Duck", "runs", "duck_points_int"),

        ("Wickets", "wickets", "wicket_points_int"),
        ("LBW/Bowled bonus", "bowled_lbw_wickets", "lbw_bowled_bonus_points_int"),
        ("Maiden overs", "maidens", "maiden_points_int"),
        ("3-Wicket bonus", "wickets", "wicket_haul_bonus_points_int"),
        ("Dot balls", "dot_balls", "dot_ball_points_int"),

        ("Catches", "catches", "catch_points_int"),
        ("3-catches bonus", "catches", "catch_3_bonus_points_int"),
        ("Stumpings", "stumpings", "stumping_points_int"),
        ("Runout - direct", "runouts_direct", "runout_direct_points_int"),
        ("Runout - indirect", "runouts_indirect", "runout_indirect_points_int"),

        ("Strike Rate", "strike_rate", "strike_rate_points_int"),
        ("Economy Rate", "bowling_economy", "economy_rate_points_int"),
        ("Announced", "played_match_flag", "appearance_points_int"),
    ]

    parts = []

    for label, stat_col, pts_col in pairs:
        stat_val = player_row_series.get(stat_col, 0)
        pts_val = player_row_series.get(pts_col, 0)

        if pd.notna(pts_val) and pts_val != 0:
            parts.append(f"{label}: {stat_val} → {int(pts_val)}")

    return " | ".join(parts)

def parse_readme_to_match_metadata_df(readme_path_str):
    rows = []
    with open(readme_path_str, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line.startswith("2026-"):
                continue
            parts = line.split(" - ")
            if len(parts) < 6:
                continue
            match_date = parts[0]
            match_id = parts[4]
            match_label = parts[5]
            if " vs " not in match_label:
                continue
            team1, team2 = match_label.split(" vs ", 1)
            rows.append({
                "match_id": match_id,
                "match_date": match_date,
                "team1": team1,
                "team2": team2,
                "match_label": match_label
            })
    return pd.DataFrame(rows)

def build_participant_squad_from_main_auction(excel_path_str):

    raw = pd.read_excel(
        excel_path_str,
        sheet_name="Main Auction",
        header=None
    )

    GROUP_COUNT = 8
    COLS_PER_GROUP = 4
    START_ROW = 8

    records = []

    for group_block in range(GROUP_COUNT):

        col0 = group_block * COLS_PER_GROUP

        block = raw.iloc[START_ROW:, col0:col0+4].copy()
        block.columns = ["player_name_str", "team_code", "role", "price"]
        block["participant_group"] = f"Group {group_block+1}"

        records.append(block)

    squad_df = pd.concat(records, ignore_index=True)
    squad_df = squad_df.dropna(subset=["player_name_str"])

    # stable participant_id
    groups = sorted(squad_df["participant_group"].unique())
    group_to_pid = {g: f"P{str(i+1).zfill(2)}" for i, g in enumerate(groups)}
    squad_df["participant_id"] = squad_df["participant_group"].map(group_to_pid)

    return squad_df[
        ["participant_id","participant_group","player_name_str","team_code","role","price"]
    ]

def build_player_team_map_from_json_folder(json_folder_str, teams_metadata_df):

    records = []

    # build team_name → nation_code map from metadata
    team_name_to_code = dict(
        zip(
            teams_metadata_df["team_name"].str.strip(),
            teams_metadata_df["team_code"].str.strip()
        )
    )

    for fname in os.listdir(json_folder_str):

        if not fname.endswith(".json"):
            continue

        fpath = os.path.join(json_folder_str, fname)

        with open(fpath, "r", encoding="utf-8") as f:
            data = json.load(f)

        info = data.get("info", {})
        teams = info.get("teams", [])

        # many scorecard JSONs store players by team in info.players
        players_by_team = info.get("players", {})

        for team_name, player_list in players_by_team.items():

            nation_code = team_name_to_code.get(team_name)

            for p in player_list:
                records.append({
                    "player_name_str": p.strip(),
                    "team_name_json": team_name,
                    "nation_code": nation_code
                })

    df = pd.DataFrame(records)

    # deduplicate
    df = df.drop_duplicates(subset=["player_name_str"])

    return df
