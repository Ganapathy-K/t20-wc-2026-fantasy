from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent.parent  # avoids hardcoding the path

FANTASY_SQUAD_CSV = ROOT / "participants_match_df.csv"
PLAYER_STATS_CSV  = ROOT / "icc_mens_t20wc_2026_9teams_match_points_batch.csv"
API_AUDIT_PATH    = r"C:/Users/ganap/Downloads/Fantasy_Points_T20WC_2025_26_from_api.xlsx"

def get_standings() -> pd.DataFrame:
    """Overall leaderboard: one row per group, sorted by total points."""
    squad_df = pd.read_csv(FANTASY_SQUAD_CSV)
    standings_df = squad_df.groupby("participant_group", as_index=False)["counted_points"].sum()
    standings_df = standings_df.rename(columns={"counted_points": "total_points"})
    standings_df = standings_df.sort_values("total_points", ascending=False).reset_index(drop=True)
    standings_df.insert(0, "rank", standings_df.index + 1)  # 1-based rank
    return standings_df


def get_round_summary() -> pd.DataFrame:
    """Points per group per round, with cumulative total and round rank — FPL gameweek history style."""
    squad_df = pd.read_csv(FANTASY_SQUAD_CSV)
    summary_df = squad_df.groupby(["round", "participant_group"], as_index=False)["counted_points"].sum()
    summary_df = summary_df.rename(columns={"counted_points": "round_points"})
    summary_df["cumulative_points"] = summary_df.groupby("participant_group")["round_points"].cumsum()
    summary_df["round_rank"] = summary_df.groupby("round")["round_points"].rank(ascending=False, method="min").astype(int)
    return summary_df.sort_values(["round", "round_rank"]).reset_index(drop=True)


def get_top_players(top_n: int = 10, role: str | None = None) -> pd.DataFrame:
    """Top players by total fantasy points across all matches.

    Parameters
    ----------
    top_n : int
        Number of players to return (default 10).
    role : str | None
        Filter to a single role: 'BAT', 'BOWL', 'AR', or 'WK'.
        Pass None to include all roles.
    """
    stats_df = pd.read_csv(PLAYER_STATS_CSV)

    if role is not None:
        stats_df = stats_df[stats_df["role"].str.upper() == role.upper()] # Keep roles upper case for consistency

    agg = (
        stats_df.groupby(["canonical_player_name", "role", "nation_code"], as_index=False)
        .agg(total_points=("fantasy_points_final", "sum"), matches_played=("fantasy_points_final", "count"),)
    ) # group by player, role, nation; sum points & matches per player
    agg["avg_points"] = (agg["total_points"] / agg["matches_played"]).round(1)
    # sort by total points, take top N, reset index for ranking
    agg = agg.sort_values("total_points", ascending=False).head(top_n).reset_index(drop=True)
    agg.insert(0, "rank", agg.index + 1) # rank players on total points, add index+ 1 every player
    return agg


def get_chip_summary() -> pd.DataFrame:
    """Chip usage per group — pivot of which round each chip was played, or — if unused."""
    df = pd.read_csv(FANTASY_SQUAD_CSV)
    chip_cols = ["chip_best11", "chip_flexicap", "chip_matchwinner", "chip_converter"]

    # Max per group+round: 1 if any player in that group/round has the chip active
    active = df.groupby(["participant_group", "round"])[chip_cols].max().reset_index()

    # Melt to long, keep only active chips, label round as "R<n>"
    melted = active.melt(id_vars=["participant_group", "round"], var_name="chip", value_name="used")
    melted = melted[melted["used"] > 0].copy()
    melted["chip"] = melted["chip"].str.replace("chip_", "", regex=False).str.upper()
    melted["round_label"] = "R" + melted["round"].astype(str)

    # Pivot: groups as rows, chips as columns, value = round used
    pivot = melted.pivot_table(index="participant_group", columns="chip", values="round_label", aggfunc=", ".join)
    pivot = pivot.reindex(columns=["BEST11", "FLEXICAP", "MATCHWINNER", "CONVERTER"])  # consistent column order
    pivot = pivot.fillna("—").reset_index()
    pivot.columns.name = None
    return pivot
