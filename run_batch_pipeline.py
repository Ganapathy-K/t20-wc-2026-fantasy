import os
import pandas as pd
from datetime import datetime

from match_file_to_points_dataframe import build_match_points_dataframe_from_data_source
from pipeline_utils import load_players_master_dataframe_from_excel, parse_readme_to_match_metadata_df, build_player_team_map_from_json_folder, EXCEL_WORKBOOK_PATH, JSON_FOLDER_PATH

timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")

JSON_FOLDER_STR = JSON_FOLDER_PATH
MASTER_EXCEL_PATH_STR = EXCEL_WORKBOOK_PATH
README_PATH_STR = os.path.join(JSON_FOLDER_STR, "README.txt")

if not os.path.isdir(JSON_FOLDER_STR):
    raise FileNotFoundError(f"JSON folder not found: {JSON_FOLDER_STR}")
if not os.path.isfile(README_PATH_STR):
    raise FileNotFoundError(f"README not found: {README_PATH_STR}")

PLAYER_OVERRIDE_MAP = {
    "pwh de silva": "wanindu hasaranga",
    "phkd mendis": "kamindu mendis",
    "bkg mendis": "kusal mendis",
    "ms chapman": "Mark Chapman"
}

# test
teams_metadata_df = pd.read_excel(MASTER_EXCEL_PATH_STR, sheet_name="Teams Metadata")
teams_metadata_df.columns = teams_metadata_df.columns.str.lower().str.strip()
auto_map_df = build_player_team_map_from_json_folder(JSON_FOLDER_STR, teams_metadata_df)
print(auto_map_df.head(3))
print("Auto-mapped players:", len(auto_map_df))

# load player master once
# players_master_df = load_players_master_dataframe_from_excel(MASTER_EXCEL_PATH_STR, sheet_name_str="Players List", debug_bool=False)
players_master_df = pd.read_excel(MASTER_EXCEL_PATH_STR, sheet_name="Players List")

players_master_df.columns = players_master_df.columns.str.strip().str.lower().str.replace(" ", "_")
players_master_df["json_name_key"] = players_master_df["json_player_name"].astype(str).str.lower().str.replace(".", "", regex=False).str.strip()
# players_master_df["role"] = players_master_df["postiion"]

meta_cols = [c for c in ["player_name", "nation", "group"] if c in players_master_df.columns]

# parse match metadata once
match_meta_df = parse_readme_to_match_metadata_df(README_PATH_STR)
match_meta_df["match_id"] = match_meta_df["match_id"].astype(str)

all_match_dfs = []
df_fix = pd.read_excel(MASTER_EXCEL_PATH_STR, sheet_name="Players List")
df_fix.columns = df_fix.columns.str.strip()
if "Postiion" in df_fix.columns:
    df_fix = df_fix.rename(columns={"Postiion": "Role"})
with pd.ExcelWriter(MASTER_EXCEL_PATH_STR, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
    df_fix.to_excel(writer, sheet_name="Players List", index=False)

for file_name_str in sorted(os.listdir(JSON_FOLDER_STR)):

    if not file_name_str.endswith(".json"):
        continue

    match_file_path_str = os.path.join(JSON_FOLDER_STR, file_name_str)
    print("Processing:", file_name_str)

    match_df = build_match_points_dataframe_from_data_source(match_file_path_str, MASTER_EXCEL_PATH_STR, debug_bool=False)

    # NEW name mapping map b/w JSON-EXCEL
    audit_df = match_df.copy()

    # JSON name + last name
    audit_df["json_name"] = audit_df["player_name_str"]
    audit_df["json_last"] = audit_df["json_name"].str.split().str[-1]

    # Excel match attempt (based on your existing key logic)
    audit_df["json_name_key"] = audit_df["player_name_str"].str.lower().str.replace(".", "", regex=False).str.strip()

    # bring excel names
    audit_df = audit_df.merge(players_master_df[["json_name_key", "player_name"]], on="json_name_key", how="left")

    # Excel name + last name
    audit_df["excel_name"] = audit_df["player_name"]
    audit_df["excel_last"] = audit_df["excel_name"].str.split().str[-1]

    # add match info
    audit_df["match_id"] = file_name_str.replace(".json", "")
    audit_df["match_label"] = audit_df.get("match_label", None)

    # keep only required columns
    audit_df = audit_df[["json_name", "json_last", "excel_name", "excel_last", "json_name_key", "match_id", "match_label"]]

    # ensure canonical column always exists
    if "canonical_player_name" not in match_df.columns:
        match_df["canonical_player_name"] = match_df["player_name_str"]

    match_df["match_id"] = file_name_str.replace(".json", "")
    match_df["json_name_key"] = match_df["player_name_str"].astype(str).str.lower().str.replace(".", "", regex=False).str.strip()
    match_df["json_name_key"] = match_df["json_name_key"].replace(PLAYER_OVERRIDE_MAP)

    # merge player metadata
    if meta_cols:
        match_df = match_df.merge(
            players_master_df[["json_name_key"] + meta_cols],
            left_on="json_name_key",
            right_on="json_name_key",
            how="left"
        ).drop(columns=["player_name"], errors="ignore")
    all_match_dfs.append(match_df)

# final_audit_df.to_csv("player_name_audit.csv", index=False)

tournament_df = pd.concat(all_match_dfs, ignore_index=True)


# --- build JSON-based nation map (PRIMARY source) ---
player_to_auto_nation = dict(zip(auto_map_df["player_name_str"], auto_map_df["nation_code"]))

json_nation_series = tournament_df["player_name_str"].map(player_to_auto_nation)

# ensure column exists before fill
if "nation_code" not in tournament_df.columns:
    tournament_df["nation_code"] = None

# JSON first, resolver/master fallback second
tournament_df["nation_code"] = json_nation_series.fillna(tournament_df["nation_code"])

# move nation_code to front (now safe)
cols = ["nation_code"] + [c for c in tournament_df.columns if c != "nation_code"]
tournament_df = tournament_df[cols]

# merge README match metadata
tournament_df["match_id"] = tournament_df["match_id"].astype(str)
tournament_df = tournament_df.merge(match_meta_df, on="match_id", how="left")

# --- build team-round table from Match Schedule sheet ---
schedule_df = pd.read_excel(MASTER_EXCEL_PATH_STR, sheet_name="Match Schedule")
schedule_df["match_id"] = schedule_df["match_id"].astype(str)

teams_long_df = (
    schedule_df.assign(team1=schedule_df["match"].str.split(" vs ").str[0],
                       team2=schedule_df["match"].str.split(" vs ").str[1])
    .melt(
        id_vars=["match_id", "match_date"],
        value_vars=["team1", "team2"],
        value_name="team_name_full"
    )[["match_id", "match_date", "team_name_full"]]
)

# map full team name → nation_code using Teams Metadata
# detect full team name column automatically
team_name_col = [c for c in teams_metadata_df.columns if "team" in c][0]
nation_code_col = [c for c in teams_metadata_df.columns if "code" in c][0]

team_name_to_code = dict(zip(
    teams_metadata_df[team_name_col],
    teams_metadata_df[nation_code_col]
))

teams_long_df["nation_code"] = teams_long_df["team_name_full"].map(team_name_to_code)

tournament_df = tournament_df.merge(teams_long_df[["match_id","nation_code"]], on=["match_id","nation_code"], how="left")

# canonical fantasy score column
tournament_df["fantasy_points_final"] = tournament_df["final_total_points_int"]

# reorder important columns first
priority_cols = [
    "player_name_str",
    'canonical_player_name',
    "fantasy_points_final",
    "points_pairs_str",
    "nation_code",
    "role",
    "match_id",
    "match_date",
    "team1",
    "team2",
    "match_label"
]

priority_cols = [c for c in priority_cols if c in tournament_df.columns]
other_cols = [c for c in tournament_df.columns if c not in priority_cols]
tournament_df = tournament_df[priority_cols + other_cols]

# output_path_str = f"icc_mens_t20wc_2026_all_match_points-{timestamp_str}.csv"
draft_teams = ["IND","PAK","AUS","ENG","NZ","SA","SL","WI","AFG"]
tournament_9teams_df = tournament_df[tournament_df["nation_code"].isin(draft_teams)]
output_path_str = f"icc_mens_t20wc_2026_9teams_match_points_batch.csv"
tournament_9teams_df.to_csv(output_path_str, index=False)

print("Batch complete — rows:", len(tournament_df))
print("Wrote:", output_path_str)
