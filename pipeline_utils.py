import json
import logging
import os
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)

# --- Configurable paths — update these if your files live elsewhere ---
EXCEL_WORKBOOK_PATH = r"C:/Users/ganap/Downloads/ICC T20 WC 2026 Auction Game.xlsx"
JSON_FOLDER_PATH    = str(Path(__file__).resolve().parent / "icc_mens_t20_world_cup_male_json")

# load players master dataframe from excel workbook
def load_players_master_dataframe_from_excel(master_excel_path_str: str, sheet_name_str: str | None = None, debug_bool: bool = False) -> pd.DataFrame:
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
        logger.info("Detected sheet: %s", sheet_name_str)
        logger.info("Detected columns: %s", players_master_dataframe.columns.tolist())
    return players_master_dataframe

# unwrap cricsheet delivery wrapper (supports both formats)
def get_delivery_info_from_wrapper(delivery_wrapper_dict: dict) -> dict:
    if "batter" in delivery_wrapper_dict:
        return delivery_wrapper_dict
    return next(iter(delivery_wrapper_dict.values()))

# legal delivery check helper (wide + no-ball excluded)
def is_legal_delivery_bool_from_delivery(delivery_info_dict: dict) -> bool:
    extras_dict = delivery_info_dict.get("extras", {})
    return not ("wides" in extras_dict or "noballs" in extras_dict)

def detect_player_name_column(dataframe_obj: pd.DataFrame) -> str | None:
    for column_name_str in dataframe_obj.columns:
        normalized_column_name_str = column_name_str.lower().replace(" ", "_")
        if normalized_column_name_str in ("player", "player_name", "name") or ("player" in normalized_column_name_str and "name" in normalized_column_name_str):
            return column_name_str
    return None

def parse_readme_to_match_metadata_df(readme_path_str: str) -> pd.DataFrame:
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

def build_player_team_map_from_json_folder(json_folder_str: str, teams_metadata_df: pd.DataFrame) -> pd.DataFrame:

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
