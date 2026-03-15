import pandas as pd
import logging
import sys
from pathlib import Path
from pipeline_utils import EXCEL_WORKBOOK_PATH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("contest_scoring.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)
project_root = Path(__file__).resolve().parent

def log_uncaught_exceptions(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.exception("Uncaught exception during report run", exc_info=(exc_type, exc_value, exc_traceback))

sys.excepthook = log_uncaught_exceptions


def ensure_required_columns(df: pd.DataFrame, defaults: dict) -> pd.DataFrame:
    out = df.copy()
    for col, default in defaults.items():
        if col not in out.columns:
            out[col] = default
    return out

def norm_name(name_series) -> pd.Series:
    return (
        pd.Series(name_series, copy=False)
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"[^a-z0-9]+", " ", regex=True)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )

def norm_match_id(match_id_series) -> pd.Series:
    return (
        pd.Series(match_id_series, copy=False)
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )


def norm_round(round_series) -> pd.Series:
    # Handles 1, 1.0, "1", "R1", "Round 2".
    series = pd.Series(round_series, copy=False).astype(str).str.strip()
    extracted = series.str.extract(r"(\d+)", expand=False)
    return pd.to_numeric(extracted, errors="coerce")


def norm_team_name(team_name_str: str) -> str:
    s = str(team_name_str).strip().lower()
    s = "".join(ch if ch.isalnum() else " " for ch in s)
    s = " ".join(s.split())
    replacements = {
        "men": "",
        "mens": "",
        "women": "",
        "womens": "",
    }
    for k, v in replacements.items():
        s = s.replace(k, v)
    s = " ".join(s.split())
    alias_map = {
        "new zealand": "new zealand",
        "nz": "new zealand",
        "england": "england",
        "india": "india",
        "pakistan": "pakistan",
        "australia": "australia",
        "south africa": "south africa",
        "sri lanka": "sri lanka",
        "west indies": "west indies",
        "afghanistan": "afghanistan",
        "afg": "afghanistan",
    }
    return alias_map.get(s, s)


def detect_team_metadata_columns(teams_metadata_df: pd.DataFrame) -> tuple[str, str]:
    cols = [str(c).strip() for c in teams_metadata_df.columns]
    lower = {c: c.lower() for c in cols}

    code_candidates = [c for c in cols if "code" in lower[c]]
    if not code_candidates:
        raise ValueError("Teams Metadata: unable to detect nation/team code column.")

    # Prefer nation/team code style names over generic code fields.
    code_candidates = sorted(
        code_candidates,
        key=lambda c: (
            0 if ("nation" in lower[c] or "team" in lower[c]) else 1,
            len(c),
        ),
    )
    nation_code_col = code_candidates[0]

    name_candidates = [c for c in cols if ("team" in lower[c] and "code" not in lower[c])]
    if not name_candidates:
        raise ValueError("Teams Metadata: unable to detect full team name column.")

    # Prefer explicit full/name columns over short/abbr columns.
    name_candidates = sorted(
        name_candidates,
        key=lambda c: (
            0 if ("full" in lower[c] or "name" in lower[c]) else 1,
            1 if ("short" in lower[c] or "abbr" in lower[c]) else 0,
            len(c),
        ),
    )
    team_name_col = name_candidates[0]
    return team_name_col, nation_code_col

BATCH_CSV_PATH = project_root / "icc_mens_t20wc_2026_9teams_match_points_batch.csv"


def is_abandoned_or_no_result_text(match_result_str) -> bool:
    txt = str(match_result_str).strip().lower()
    if txt in ("", "nan", "none"):
        return False
    blocked_tokens = [
        "match abandoned",
        "abandoned",
        "no result",
        "n/r",
        "nr",
        "tbd",
    ]
    return any(tok in txt for tok in blocked_tokens)


# AUTOSUB & CHIPS LOGIC
# Minimum formation that must be preserved after every autosub
_ROLE_MIN = {"WK": 1, "BAT": 2, "AR": 1, "BOWL": 2}
DEFAULT_SUB_ORDER = 999  # placeholder for "no sub order assigned"
MATCHWINNER_POTM_MULTIPLIER = 3  # multiplier when matchwinner chip player is POTM
MATCHWINNER_NO_POTM_MULTIPLIER = 1  # multiplier when matchwinner chip player is NOT POTM

def apply_autosub(df: pd.DataFrame) -> pd.DataFrame: # Autosub
    points_series = df.get("points_breakdown_str", pd.Series("DID NOT PLAY", index=df.index))
    df["played_match_flag"] = pd.to_numeric(df.get("played_match_flag", 0), errors="coerce").fillna((points_series != "DID NOT PLAY").astype(int)).astype(int)
    df["is_playing_xi"] = pd.to_numeric(df["is_playing_xi"], errors="coerce").fillna(0).astype(int)
    df["sub_order"] = pd.to_numeric(df["sub_order"], errors="coerce").fillna(DEFAULT_SUB_ORDER).astype(int)
    pos_col = "effective_position" if "effective_position" in df.columns else "position"
    # STEP 1: initialize columns
    df["subbed_out"] = 0
    df["subbed_in"] = 0
    df["final_playing_xi"] = df["is_playing_xi"]
    # STEP 2: process per participant_group + round
    for (group, rnd), sub_df in df.groupby(["participant_group", "round"]): # G1-R1-remaining df
        # STEP 3: split XI and bench
        xi_players = sub_df[sub_df["is_playing_xi"] == 1] # Players in remaining df who were picked in x1 by participants
        bench_players = sub_df[sub_df["is_playing_xi"] == 0].sort_values("sub_order") # Subs in remaining df by participants
        # STEP 4: identify XI players who did not play
        failed_xi = xi_players[xi_players["played_match_flag"] == 0] # Players who started for participants but not IRL
        # STEP 5: iterate over failed XI players
        for xi_idx in failed_xi.index:
            # STEP 6: find eligible bench replacement
            replacement = bench_players[
                (bench_players["played_match_flag"] == 1) & # Pick earliest sub who did play IRL
                (df.loc[bench_players.index, "subbed_in"] == 0) # Using indices of benched players on main df, check if subbed_in column is 0/not 0
            ] # Fill 'replacement' df with only those records from bench_players who have 1. played IRL 2. 0 as value in subbed_in column
            if replacement.empty:
                break  # no more replacements available
            # STEP 6b: formation constraint — if removing the outgoing role drops below
            # the minimum (e.g. only 1 WK left and no WK bench played), skip this sub.
            out_role = str(df.loc[xi_idx, pos_col]).upper()
            live_xi_mask = df.index.isin(sub_df.index) & (df["final_playing_xi"] == 1)
            role_counts = df.loc[live_xi_mask, pos_col].str.upper().value_counts().to_dict()
            if role_counts.get(out_role, 0) - 1 < _ROLE_MIN.get(out_role, 0):
                # Outgoing role would fall below minimum — only allow same-role replacement
                replacement = replacement[df.loc[replacement.index, pos_col].str.upper() == out_role]
                if replacement.empty:
                    continue  # No valid same-role bench player; skip, player stays at 0 pts
            # STEP 7: pick highest priority bench player
            rep_idx = replacement.iloc[0].name
            # STEP 8: mark XI player as subbed out
            df.loc[xi_idx, "subbed_out"] = 1
            df.loc[xi_idx, "final_playing_xi"] = 0
            # STEP 9: mark bench player as subbed in
            df.loc[rep_idx, "subbed_in"] = 1
            df.loc[rep_idx, "final_playing_xi"] = 1
    return df

def apply_captain_vice(df: pd.DataFrame) -> pd.DataFrame: # Captain/vice-captain
    out = df.copy()
    out["fantasy_points_final"] = pd.to_numeric(out["fantasy_points_final"], errors="coerce").fillna(0)
    out["final_playing_xi"] = pd.to_numeric(out.get("final_playing_xi", out["is_playing_xi"]), errors="coerce").fillna(0).astype(int)
    out["played_match_flag"] = pd.to_numeric(out.get("played_match_flag", 0), errors="coerce").fillna(0).astype(int)
    out["captain_flag"] = pd.to_numeric(out.get("captain_flag", 0), errors="coerce").fillna(0).astype(int)
    out["vice_flag"] = pd.to_numeric(out.get("vice_flag", 0), errors="coerce").fillna(0).astype(int)
    out["points_multiplier"] = pd.to_numeric(out.get("points_multiplier", 1), errors="coerce").fillna(1)

    for (_, _), sub_df in out.groupby(["participant_group", "round"], sort=False):
        has_best11 = pd.to_numeric(sub_df.get("chip_best11", 0), errors="coerce").fillna(0).astype(int).any()
        has_flexicap = pd.to_numeric(sub_df.get("chip_flexicap", 0), errors="coerce").fillna(0).astype(int).any()
        # Best11/Flexicap own the 2x logic for the round.
        if has_best11 or has_flexicap:
            continue
        xi = sub_df[sub_df["final_playing_xi"] == 1]
        if xi.empty:
            continue

        target_idx = None
        captain = xi[(xi["captain_flag"] == 1) & (xi["played_match_flag"] == 1)]
        if not captain.empty:
            target_idx = captain.index[0]
        else:
            vice = xi[xi["vice_flag"] == 1]
            if not vice.empty:
                target_idx = vice.index[0]

        if target_idx is not None:
            out.loc[target_idx, "points_multiplier"] = 2

    out["points_multiplier"] = pd.to_numeric(out["points_multiplier"], errors="coerce").fillna(1)
    out["final_points"] = out["fantasy_points_final"] * out["points_multiplier"]
    return out

def apply_converter(df: pd.DataFrame) -> pd.DataFrame:  # Converter chip
    # Permanent role conversion from the trigger round onward per participant + player.
    if "chip_converter" not in df.columns or "role_override" not in df.columns:
        return df

    out = df.copy()
    out["round"] = norm_round(out["round"])
    out["player_name"] = out["player_name"].astype(str).str.strip()
    out["effective_position"] = out["effective_position"].astype(str).str.strip().str.upper()
    out["role_override"] = out["role_override"].astype(str).str.strip().str.upper()

    converter_rows = out[
        (pd.to_numeric(out["chip_converter"], errors="coerce").fillna(0) == 1) &
        (out["role_override"].ne("")) &
        (out["role_override"].ne("NAN"))
    ].copy()

    if converter_rows.empty:
        return out

    converter_rows = converter_rows.sort_values("round")

    # Keep earliest converter trigger per participant + player.
    first_triggers = converter_rows.drop_duplicates(subset=["participant_group", "player_name"], keep="first")

    for _, trig in first_triggers.iterrows():
        mask = (
            (out["participant_group"] == trig["participant_group"]) &
            (out["player_name"] == trig["player_name"]) &
            (out["round"] >= trig["round"])
        )
        out.loc[mask, "effective_position"] = trig["role_override"]

    return out

def apply_best11(df: pd.DataFrame) -> pd.DataFrame: # Best11 chip
    role_min = {"WK": 1, "BAT": 2, "AR": 1, "BOWL": 2}
    out_parts = []

    for (_, _), sub_df in df.groupby(["participant_group", "round"], sort=False, dropna=False):
        part = sub_df.copy()
        part["effective_position"] = part["effective_position"].astype(str).str.strip().str.upper()
        part["fantasy_points_final"] = pd.to_numeric(part["fantasy_points_final"], errors="coerce").fillna(0)

        selected_idx = []

        # First pass: satisfy minimum role requirements from highest scorers.
        for role, min_count in role_min.items():
            role_candidates = part[part["effective_position"] == role].sort_values("fantasy_points_final", ascending=False)
            selected_idx.extend(role_candidates.head(min_count).index.tolist())

        # Second pass: fill remaining slots by highest points across all roles.
        selected_idx = list(dict.fromkeys(selected_idx))
        remaining_slots = max(0, min(11, len(part)) - len(selected_idx))
        if remaining_slots > 0:
            remaining = part[~part.index.isin(selected_idx)].sort_values("fantasy_points_final", ascending=False)
            selected_idx.extend(remaining.head(remaining_slots).index.tolist())

        part["subbed_out"] = 0
        part["subbed_in"] = 0
        part["final_playing_xi"] = 0
        part.loc[selected_idx, "final_playing_xi"] = 1
        out_parts.append(part)

    if out_parts:
        return pd.concat(out_parts, axis=0).sort_index()
    return df

def apply_best11_multiplier(df: pd.DataFrame) -> pd.DataFrame: # Best11 highest-scorer 2x
    out = df.copy()
    out["fantasy_points_final"] = pd.to_numeric(out["fantasy_points_final"], errors="coerce").fillna(0)
    out["final_playing_xi"] = pd.to_numeric(out.get("final_playing_xi", out["is_playing_xi"]), errors="coerce").fillna(0).astype(int)
    out["points_multiplier"] = pd.to_numeric(out.get("points_multiplier", 1), errors="coerce").fillna(1)

    for (_, _), sub_df in out.groupby(["participant_group", "round"], sort=False):
        has_best11 = pd.to_numeric(sub_df.get("chip_best11", 0), errors="coerce").fillna(0).astype(int).any()
        if not has_best11:
            continue
        xi = sub_df[sub_df["final_playing_xi"] == 1]
        if xi.empty:
            continue
        target_idx = xi.sort_values("fantasy_points_final", ascending=False).index[0]
        out.loc[target_idx, "points_multiplier"] = 2

    out["final_points"] = out["fantasy_points_final"] * out["points_multiplier"]
    return out

def apply_flexicap(df: pd.DataFrame) -> pd.DataFrame: # Flexicap chip
    out = df.copy()
    out["fantasy_points_final"] = pd.to_numeric(out["fantasy_points_final"], errors="coerce").fillna(0)
    out["final_playing_xi"] = pd.to_numeric(out.get("final_playing_xi", out["is_playing_xi"]), errors="coerce").fillna(0).astype(int)
    out["points_multiplier"] = pd.to_numeric(out.get("points_multiplier", 1), errors="coerce").fillna(1)

    for (_, _), sub_df in out.groupby(["participant_group", "round"], sort=False):
        has_flexicap = pd.to_numeric(sub_df.get("chip_flexicap", 0), errors="coerce").fillna(0).astype(int).any()
        if not has_flexicap:
            continue
        xi = sub_df[sub_df["final_playing_xi"] == 1]
        if xi.empty:
            continue
        target_idx = xi.sort_values("fantasy_points_final", ascending=False).index[0]
        out.loc[target_idx, "points_multiplier"] = 2

    out["final_points"] = out["fantasy_points_final"] * out["points_multiplier"]
    return out

def apply_matchwinner(df: pd.DataFrame) -> pd.DataFrame: # Matchwinner chip
    out = df.copy()
    out["points_multiplier"] = pd.to_numeric(out.get("points_multiplier", 1), errors="coerce").fillna(1)
    out["final_playing_xi"] = pd.to_numeric(out.get("final_playing_xi", out["is_playing_xi"]), errors="coerce").fillna(0).astype(int)
    out["potm_bonus"] = pd.to_numeric(out.get("potm_bonus", 0), errors="coerce").fillna(0)
    out["chip_matchwinner"] = pd.to_numeric(out.get("chip_matchwinner", 0), errors="coerce").fillna(0).astype(int)
    out["matchwinner_multiplier"] = 1

    for (_, _), sub_df in out.groupby(["participant_group", "round"], sort=False):
        mw_rows = sub_df[(sub_df["chip_matchwinner"] == 1) & (sub_df["final_playing_xi"] == 1)]
        if mw_rows.empty:
            continue
        mw_idx = mw_rows.index[0]
        out.loc[mw_idx, "matchwinner_multiplier"] = MATCHWINNER_POTM_MULTIPLIER if out.loc[mw_idx, "potm_bonus"] > 0 else MATCHWINNER_NO_POTM_MULTIPLIER
        out.loc[mw_idx, "points_multiplier"] = out.loc[mw_idx, "points_multiplier"] * out.loc[mw_idx, "matchwinner_multiplier"]

    out["final_points"] = pd.to_numeric(out["fantasy_points_final"], errors="coerce").fillna(0) * out["points_multiplier"]
    return out

def apply_chips(participants_match_df: pd.DataFrame) -> pd.DataFrame:
    df = participants_match_df.copy()
    df = ensure_required_columns(
        df,
        {
            "chip_converter": 0,
            "chip_best11": 0,
            "chip_flexicap": 0,
            "chip_matchwinner": 0,
            "captain_flag": 0,
            "vice_flag": 0,
            "role_override": "",
            "sub_order": DEFAULT_SUB_ORDER,
            "is_playing_xi": 0,
            "played_match_flag": 0,
            "points_multiplier": 1,
            "potm_bonus": 0,
        },
    )
    # Converter must run globally first because it is permanent across rounds.
    if "chip_converter" in df.columns and df["chip_converter"].any():
        df = apply_converter(df)

    outputs = []
    # Apply remaining chips independently per participant group + round.
    for (_, _), sub_df in df.groupby(["participant_group", "round"], sort=False):
        group_df = sub_df.copy()

        # XI logic (Best11 overrides autosub only for this slice)
        if group_df["chip_best11"].any():
            group_df = apply_best11(group_df)
            group_df = apply_best11_multiplier(group_df)
        else:
            group_df = apply_autosub(group_df)

        # Flexicap
        if group_df["chip_flexicap"].any():
            group_df = apply_flexicap(group_df)

        # Captain/Vice
        group_df = apply_captain_vice(group_df)

        # Matchwinner
        if group_df["chip_matchwinner"].any():
            group_df = apply_matchwinner(group_df)

        outputs.append(group_df)

    if outputs:
        return pd.concat(outputs, axis=0).sort_index()
    return df


def main() -> None:
    # STEP 1 - Load match data (player performance per match)
    match_all_teams_df = pd.read_csv(BATCH_CSV_PATH)  # read match CSV into DataFrame

    # DROP "match_date_x", "match_label_x", rename "match_date_y", "match_label_y" as "match_date", "match_label"
    match_all_teams_df = match_all_teams_df.drop(columns=["match_date_x", "match_label_x"], errors="ignore")
    match_all_teams_df = match_all_teams_df.rename(columns={"team_code": "team", "nation_code": "team_code", "match_date_y": "match_date", "match_label_y": "match_label"})
    match_all_teams_df['match_id'] = match_all_teams_df['match_id'].astype('str')
    match_all_teams_df["team_round_source"] = pd.to_numeric(match_all_teams_df.get("team_round"), errors="coerce")
    match_all_teams_df = match_all_teams_df.drop(columns=["team_round"], errors="ignore")

    # Insert AFG player points
    afg_match_df_raw = pd.read_excel(EXCEL_WORKBOOK_PATH, sheet_name="AFG Player Points")  # read squad sheet from Excel

    # FIX 1: Non-AFG opponent players in AFG match rows (e.g. NZ in match 1512722,
    # SA in match 1512731) have team_code=NaN and role=NaN because Cricsheet withholds
    # Afghanistan matches from JSON files.  Without role, the Step-3 lineup merge
    # (left_on=["player","effective_position","round"], right_on=["player","role","team_round"])
    # silently drops those players, giving them 0 points instead of their real scores.
    # Solution: look up role and team_code from the Players List sheet.
    _afg_pl_df = pd.read_excel(EXCEL_WORKBOOK_PATH, sheet_name="Players List")
    _afg_pl_df.columns = _afg_pl_df.columns.str.strip().str.lower().str.replace(" ", "_")
    _afg_name_to_role = dict(zip(_afg_pl_df["player_name"].astype(str).str.strip(), _afg_pl_df["role"]))
    _afg_name_to_code = dict(zip(_afg_pl_df["player_name"].astype(str).str.strip(), _afg_pl_df["team_code"]))
    _missing_code_mask = afg_match_df_raw["team_code"].isna()
    if _missing_code_mask.any():
        _lookup = afg_match_df_raw.loc[_missing_code_mask, "canonical_player_name"].astype(str).str.strip()
        afg_match_df_raw.loc[_missing_code_mask, "role"] = _lookup.map(_afg_name_to_role)
        afg_match_df_raw.loc[_missing_code_mask, "team_code"] = _lookup.map(_afg_name_to_code)
        logger.info(
            "Enriched %d non-AFG opponent rows in AFG Player Points with role/team_code.",
            int(_missing_code_mask.sum()),
        )

    # FIX 2: Abandoned match (1512759 – NZ vs PAK, Super 8) has no JSON file and is
    # correctly excluded from POTM via is_abandoned_or_no_result_text().  The schedule
    # assigns team_round=5 for both NZ and PAK; participants' round=5 rows will receive
    # 0 points and points_breakdown_str="DID NOT PLAY" which is correct contest behaviour.
    # No data change needed here – the schedule-based team_round_df already handles it.

    # FIX 3: Ensure match_id in the AFG sheet is a string so it aligns with the batch
    # CSV and the schedule mapping (both use string match_ids).
    afg_match_df_raw["match_id"] = afg_match_df_raw["match_id"].astype(str)

    afg_round_source = pd.to_numeric(afg_match_df_raw.get("team_round"), errors="coerce") if "team_round" in afg_match_df_raw.columns else pd.Series(pd.NA, index=afg_match_df_raw.index)
    afg_match_df = afg_match_df_raw.drop(columns=["team_round"], errors="ignore")
    afg_match_df = afg_match_df.reindex(columns=match_all_teams_df.columns)
    afg_match_df["team_round_source"] = afg_round_source
    match_df = pd.concat([match_all_teams_df, afg_match_df], ignore_index=True)
    match_df["match_date"] = pd.to_datetime(match_df["match_date"]).dt.date
    logger.info("AFG player points loaded")
    logger.debug("AFG sample:\n%s", afg_match_df.head(3))

    match_df = match_df.sort_values(["match_id", "team_code", "fantasy_points_final"]) # sort data based on match_id - unique column

    # Build team_round from official schedule first to prevent round-shift when some match data is missing.
    schedule_rounds_df = pd.read_excel(EXCEL_WORKBOOK_PATH, sheet_name="Match Schedule")
    teams_metadata_df = pd.read_excel(EXCEL_WORKBOOK_PATH, sheet_name="Teams Metadata")
    schedule_rounds_df.columns = [str(c).strip() for c in schedule_rounds_df.columns]
    teams_metadata_df.columns = [str(c).strip() for c in teams_metadata_df.columns]

    team_name_col, nation_code_col = detect_team_metadata_columns(teams_metadata_df)
    team_name_to_code = dict(
        zip(
            teams_metadata_df[team_name_col].astype(str).map(norm_team_name),
            teams_metadata_df[nation_code_col].astype(str).str.strip().str.upper(),
        )
    )

    schedule_rounds_df["match_id"] = schedule_rounds_df["match_id"].astype(str).str.strip()
    schedule_rounds_df["match_date"] = pd.to_datetime(schedule_rounds_df["match_date"], errors="coerce").dt.date

    teams_split_df = schedule_rounds_df["match"].astype(str).str.extract(
        r"^\s*(.*?)\s+v(?:s)?\.?\s+(.*?)\s*$",
        expand=True,
    )
    schedule_teams_long_df = (
        schedule_rounds_df.assign(
            team1=teams_split_df[0].fillna("").str.strip(),
            team2=teams_split_df[1].fillna("").str.strip(),
        )
        .melt(
            id_vars=["match_id", "match_date"],
            value_vars=["team1", "team2"],
            value_name="team_name_full",
        )[["match_id", "match_date", "team_name_full"]]
    )
    schedule_teams_long_df["team_code"] = schedule_teams_long_df["team_name_full"].map(lambda x: team_name_to_code.get(norm_team_name(x)))

    coverage = schedule_teams_long_df["team_code"].notna().mean()
    if coverage < 0.9:
        missing_team_examples = (
            schedule_teams_long_df[schedule_teams_long_df["team_code"].isna()]["team_name_full"]
            .dropna()
            .astype(str)
            .str.strip()
            .replace("", pd.NA)
            .dropna()
            .drop_duplicates()
            .head(10)
            .tolist()
        )
        raise ValueError(
            f"Schedule team mapping coverage too low ({coverage:.1%}). "
            f"Check Teams Metadata columns/match text. Missing examples: {missing_team_examples}"
        )

    schedule_teams_long_df = schedule_teams_long_df.dropna(subset=["team_code"]).copy()
    schedule_teams_long_df["team_code"] = schedule_teams_long_df["team_code"].astype(str).str.upper().str.strip()
    schedule_teams_long_df = schedule_teams_long_df.sort_values(["team_code", "match_date", "match_id"])
    schedule_teams_long_df["team_round"] = schedule_teams_long_df.groupby("team_code").cumcount() + 1

    # Fallback inferred team_round from available scored matches (used only when schedule mapping is missing).
    inferred_team_round_df = (
        match_df[["team_code", "match_id", "match_date"]]
        .drop_duplicates()
        .sort_values(["team_code", "match_date", "match_id"])
    )
    inferred_team_round_df["team_round_inferred"] = inferred_team_round_df.groupby("team_code").cumcount() + 1

    team_round_df = schedule_teams_long_df[["team_code", "match_id", "match_date", "team_round"]].copy()
    missing_sched_map_df = inferred_team_round_df.merge(
        team_round_df[["team_code", "match_id"]],
        on=["team_code", "match_id"],
        how="left",
        indicator=True,
    )
    missing_sched_map_df = missing_sched_map_df[missing_sched_map_df["_merge"] == "left_only"].drop(columns=["_merge"])
    if not missing_sched_map_df.empty:
        logger.warning(
            "Schedule mapping missing for %s team/match pairs; using inferred team_round fallback for those rows.",
            len(missing_sched_map_df),
        )
        audit_cols = ["team_code", "match_id", "match_date", "team_round_inferred"]
        missing_sched_audit_df = missing_sched_map_df[audit_cols].sort_values(["team_code", "match_date", "match_id"])
        missing_sched_audit_df.to_csv(project_root / "schedule_round_mapping_missing_pairs.csv", index=False)
        logger.warning(
            "Wrote schedule mapping audit file: %s",
            project_root / "schedule_round_mapping_missing_pairs.csv",
        )
        fallback_df = missing_sched_map_df.rename(columns={"team_round_inferred": "team_round"})
        team_round_df = pd.concat(
            [team_round_df, fallback_df[["team_code", "match_id", "match_date", "team_round"]]],
            ignore_index=True,
        )

    team_round_df["team_round"] = pd.to_numeric(team_round_df["team_round"], errors="coerce")
    team_round_df = team_round_df.dropna(subset=["team_round"]).copy()
    team_round_df["team_round"] = team_round_df["team_round"].astype("Int64")
    team_round_df["is_fallback"] = 0
    if not missing_sched_map_df.empty:
        fallback_pairs = fallback_df[["team_code", "match_id"]].drop_duplicates()
        fallback_pairs["is_fallback"] = 1
        team_round_df = team_round_df.merge(fallback_pairs, on=["team_code", "match_id"], how="left", suffixes=("", "_tmp"))
        team_round_df["is_fallback"] = team_round_df["is_fallback_tmp"].fillna(team_round_df["is_fallback"]).astype(int)
        team_round_df = team_round_df.drop(columns=["is_fallback_tmp"], errors="ignore")

    # Ensure one mapping per team_code + match_id (prefer schedule rows over fallback rows).
    team_round_df = team_round_df.sort_values(["team_code", "match_id", "is_fallback"]).drop_duplicates(
        subset=["team_code", "match_id"], keep="first"
    )
    team_round_df = team_round_df.drop(columns=["is_fallback"], errors="ignore")

    match_df = pd.merge(
        match_df,
        team_round_df[["team_code", "match_id", "team_round"]],
        on=["team_code", "match_id"],
        how="left",
    )
    match_df["team_round"] = pd.to_numeric(match_df["team_round"], errors="coerce").fillna(
        pd.to_numeric(match_df["team_round_source"], errors="coerce")
    )
    match_df["team_round"] = match_df["team_round"].astype("Int64")
    match_df = match_df.drop(columns=["team_round_source"], errors="ignore")

    match_df.columns = match_df.columns.str.strip()
    match_df["player"] = match_df["canonical_player_name"].astype(str).str.strip().str.lower()
    match_df["team_code"] = match_df["team_code"].astype(str).str.strip().str.upper()

    logger.info("Player points dataframe prepared")
    logger.debug("Player points sample:\n%s", match_df.sample(2))

    # STEP 2 - Load squad data (players owned by each participant)
    participants_df = pd.read_excel(EXCEL_WORKBOOK_PATH, sheet_name="Round Lineups")  # read squad sheet from Excel
    participants_df["player"] = participants_df["player_name"].astype(str).str.strip().str.lower()  # standardize squad player names
    participants_df["effective_position"] = participants_df["position"].astype(str).str.strip().str.upper()
    participants_df["round"] = norm_round(participants_df["round"])
    logger.info("Participant lineups loaded")
    logger.debug("Participant lineup sample:\n%s", participants_df.sample(2))

    # STEP 3 - Merge (attach match points to each squad player)
    match_unique_columns = match_df.columns.difference(participants_df.columns).tolist()
    join_columns = ["player"]
    match_unique_columns.extend(join_columns)
    left_join_columns = ["player", "effective_position", "round"]
    right_join_columns = ["player", "role", "team_round"]

    participants_match_df = pd.merge(participants_df, match_df[match_unique_columns], left_on=left_join_columns, right_on=right_join_columns, how="left")

    # STEP 4 - Fill missing values
    participants_match_df["fantasy_points_final"] = participants_match_df["fantasy_points_final"].fillna(0)
    participants_match_df["team_round"] = participants_match_df["team_round"].fillna(participants_match_df["round"])
    participants_match_df["points_breakdown_str"] = participants_match_df["points_breakdown_str"].fillna("DID NOT PLAY")

    participants_match_df = participants_match_df.merge(team_round_df, on=["team_code", "team_round"], how="left", suffixes=("", "_filled"))
    participants_match_df["match_id"] = participants_match_df["match_id"].fillna(participants_match_df["match_id_filled"])
    participants_match_df = participants_match_df.drop(columns=["match_id_filled"])
    participants_match_df["match_id"] = norm_match_id(participants_match_df["match_id"])

    # Use a unique match_id -> match_date map to avoid many-to-many duplication on match_id.
    match_date_map = match_df[["match_id", "match_date"]].drop_duplicates(subset=["match_id"]).rename(columns={"match_date": "match_date_from_id"})
    participants_match_df = participants_match_df.merge(match_date_map, on=["match_id"], how="left")
    participants_match_df["match_date"] = participants_match_df["match_date"].fillna(participants_match_df["match_date_from_id"])
    participants_match_df = participants_match_df.drop(columns=["match_date_from_id"])

    # Guard against many-to-many merge artifacts:
    # keep one row per participant + round + player (a player cannot appear twice in one lineup round).
    dedup_keys = ["participant_group", "round", "player_name"]
    if all(k in participants_match_df.columns for k in dedup_keys):
        before_dedup = len(participants_match_df)
        participants_match_df = participants_match_df.sort_values(
            by=["participant_group", "round", "player_name", "fantasy_points_final"],
            ascending=[True, True, True, False],
        ).drop_duplicates(subset=dedup_keys, keep="first")
        dropped = before_dedup - len(participants_match_df)
        if dropped > 0:
            logger.warning("Dropped %s duplicate lineup rows after merge (participant_group+round+player_name).", dropped)

    # POTM bonus (+50) from Match Schedule sheet.
    try:
        schedule_df = pd.read_excel(EXCEL_WORKBOOK_PATH, sheet_name="Match Schedule")
        schedule_df.columns = [str(c).strip() for c in schedule_df.columns]
        col_map = {c.lower(): c for c in schedule_df.columns}
        match_id_col = col_map.get("match_id")
        potm_col = col_map.get("potm")
        if match_id_col and potm_col:
            potm_map = schedule_df[[match_id_col, potm_col]].dropna(subset=[match_id_col, potm_col]).copy()
            potm_map = potm_map.rename(columns={match_id_col: "match_id", potm_col: "potm"})
            potm_map = potm_map[~potm_map["potm"].map(is_abandoned_or_no_result_text)].copy()
            potm_map["match_id"] = norm_match_id(potm_map["match_id"])
            potm_map["potm_key"] = norm_name(potm_map["potm"])
            potm_map["potm_bonus"] = 50
            participants_match_df["match_id"] = norm_match_id(participants_match_df["match_id"])
            participants_match_df["potm_key"] = norm_name(
                participants_match_df.get("canonical_player_name", participants_match_df["player"]).fillna(participants_match_df["player"])
            )
            participants_match_df = participants_match_df.merge(
                potm_map[["match_id", "potm_key", "potm_bonus"]],
                on=["match_id", "potm_key"],
                how="left",
            )
            participants_match_df["potm_bonus"] = pd.to_numeric(participants_match_df["potm_bonus"], errors="coerce").fillna(0).astype(int)
            logger.info("POTM bonus matched rows: %s", int((participants_match_df["potm_bonus"] > 0).sum()))
            participants_match_df = participants_match_df.drop(columns=["potm_key"], errors="ignore")
            participants_match_df["fantasy_points_final"] = pd.to_numeric(participants_match_df["fantasy_points_final"], errors="coerce").fillna(0) + participants_match_df["potm_bonus"]
        else:
            participants_match_df["potm_bonus"] = 0
            logger.warning("Match Schedule sheet missing required columns: match_id and potm (case-insensitive)")
    except Exception as e:
        participants_match_df["potm_bonus"] = 0
        logger.warning("Unable to apply POTM bonus from Match Schedule sheet: %s", e)
    logger.debug("participants_match_df head:\n%s", participants_match_df.head())

    # STEP 7 - Apply autosub/chips to merged participant-match data
    participants_match_df = apply_chips(participants_match_df)
    participants_match_df["subbed_out"] = pd.to_numeric(participants_match_df.get("subbed_out", 0), errors="coerce").fillna(0).astype(int)
    participants_match_df["subbed_in"] = pd.to_numeric(participants_match_df.get("subbed_in", 0), errors="coerce").fillna(0).astype(int)
    participants_match_df["final_playing_xi"] = pd.to_numeric(participants_match_df.get("final_playing_xi", participants_match_df["is_playing_xi"]), errors="coerce").fillna(pd.to_numeric(participants_match_df["is_playing_xi"], errors="coerce").fillna(0)).astype(int)



    # STEP 8 - Sanity checks + totals snapshot
    participants_match_df["final_points"] = pd.to_numeric(participants_match_df.get("final_points", 0), errors="coerce").fillna(0)
    participants_match_df["match_id"] = norm_match_id(participants_match_df.get("match_id", ""))
    participants_match_df["counted_points"] = participants_match_df["final_points"].where(
        participants_match_df["final_playing_xi"] == 1, 0
    )
    participants_match_df["counted_points_type"] = participants_match_df["final_playing_xi"].map({1: "PLAYING_XI", 0: "SUB"}).fillna("SUB")

    xi_counts = participants_match_df.groupby(["participant_group", "round"])["final_playing_xi"].sum()
    bad_xi = xi_counts[xi_counts > 11]
    if not bad_xi.empty:
        logger.error("Invalid XI counts (>11):\n%s", bad_xi.to_string())
        raise ValueError("Sanity check failed: final_playing_xi exceeds 11 for one or more participant_group+round.")

    short_xi = xi_counts[xi_counts < 11]
    if not short_xi.empty:
        logger.warning(
            "Short XI counts (<11) detected (allowed by contest rules):\n%s",
            short_xi.to_string(),
        )

    negative_points = participants_match_df[participants_match_df["counted_points"] < 0]
    if not negative_points.empty:
        logger.warning(
            "Negative counted_points rows detected (allowed by scoring rules):\n%s",
            negative_points[["participant_group", "round", "player_name", "counted_points"]].head(20).to_string(index=False),
        )

    missing_match_id = participants_match_df["match_id"].astype(str).str.strip().isin(["", "nan", "None"])
    played_flag = pd.to_numeric(participants_match_df.get("played_match_flag", 0), errors="coerce").fillna(0).astype(int)
    points_pairs = participants_match_df.get("points_breakdown_str", pd.Series("", index=participants_match_df.index)).astype(str).str.strip().str.upper()
    should_have_match = (played_flag == 1) | (points_pairs != "DID NOT PLAY")

    missing_required_match_id = missing_match_id & should_have_match
    missing_optional_match_id = missing_match_id & (~should_have_match)

    if missing_required_match_id.any():
        logger.error(
            "Rows with null/empty match_id where a match is expected: %s",
            int(missing_required_match_id.sum()),
        )
        logger.error(
            "Sample rows missing required match_id:\n%s",
            participants_match_df.loc[
                missing_required_match_id,
                ["participant_group", "round", "player_name", "team_code", "team_round", "played_match_flag", "points_breakdown_str"],
            ].head(20).to_string(index=False),
        )
        raise ValueError("Sanity check failed: required match_id missing for rows that should map to a played match.")

    if missing_optional_match_id.any():
        logger.warning(
            "Rows with null/empty match_id where no played match is implied (allowed): %s",
            int(missing_optional_match_id.sum()),
        )

    totals_snapshot = (
        participants_match_df.groupby(["participant_group", "round"], as_index=False)["counted_points"]
        .sum()
        .sort_values(["participant_group", "round"])
    )
    totals_pivot = totals_snapshot.pivot(index="participant_group", columns="round", values="counted_points").fillna(0).astype(int)
    totals_pivot.columns = [f"R{c}" for c in totals_pivot.columns]
    totals_pivot["total"] = totals_pivot.sum(axis=1)
    totals_pivot = totals_pivot.sort_values("total", ascending=False)
    totals_pivot.to_csv(project_root / "totals_snapshot.csv")
    logger.info("Totals snapshot (pivot):\n%s", totals_pivot.to_csv(sep='\t'))
    logger.info("Wrote totals_snapshot.csv — open directly in Excel for tab-separated view.")

    columns_to_print = ['participant_group', 'round', 'player_name', 'team', 'team_code', 'position', 'effective_position', 'is_playing_xi', 'sub_order', 
                        'captain_flag', 'vice_flag', 'fantasy_points_final', 'potm_bonus', 'points_multiplier', 'counted_points', 'counted_points_type', 'subbed_out', 'subbed_in', 'final_playing_xi', 
                        'team_round', 'match_date', 'match_id', 
                        'chip_converter', 'chip_best11', 'chip_flexicap', 'chip_matchwinner', 'points_breakdown_str']

    participants_match_df = participants_match_df[columns_to_print]

    logger.info("Sanity check | rows: %d | groups: %d | rounds: %d | missing_match_id: %d | negative_points: %d | xi_gt_11: %d",
        len(participants_match_df),
        participants_match_df["participant_group"].nunique(),
        participants_match_df["round"].nunique(),
        int(missing_required_match_id.sum()),
        int((participants_match_df["counted_points"] < 0).sum()),
        int(len(bad_xi)),
    )

    role_order = {"WK": 1, "BAT": 2, "AR": 3, "BOWL": 4}
    participants_match_df["_role_sort"] = participants_match_df["effective_position"].str.upper().map(role_order).fillna(5)
    participants_match_df = participants_match_df.sort_values(
        ["round", "participant_group", "final_playing_xi", "_role_sort", "player_name"],
        ascending=[True, True, False, True, True],
    ).drop(columns=["_role_sort"])

    participants_match_df.to_csv(project_root / "participants_match_df.csv", index=False)

    logger.info("Wrote outputs to project root.")


if __name__ == "__main__":
    main()
