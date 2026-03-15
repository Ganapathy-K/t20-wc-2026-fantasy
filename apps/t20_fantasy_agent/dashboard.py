import sys
import importlib.util
from pathlib import Path
import pandas as pd
import streamlit as st

# load data_layer by absolute path — execute first, then register (Python 3.14 safe)
sys.modules.pop("data_layer", None)
_dl_path = Path(__file__).parent / "data_layer.py"
_dl_spec = importlib.util.spec_from_file_location("data_layer", _dl_path)
_dl_mod  = importlib.util.module_from_spec(_dl_spec)
_dl_spec.loader.exec_module(_dl_mod)   # execute before registering
sys.modules["data_layer"] = _dl_mod

get_standings     = _dl_mod.get_standings
get_round_summary = _dl_mod.get_round_summary
FANTASY_SQUAD_CSV = _dl_mod.FANTASY_SQUAD_CSV
PLAYER_STATS_CSV  = _dl_mod.PLAYER_STATS_CSV
API_AUDIT_PATH    = _dl_mod.API_AUDIT_PATH

# browser tab title, icon, and use full page width
st.set_page_config(page_title="T20 WC 2026 Fantasy", page_icon="🏏", layout="wide")

# renders the H1 heading at the top of the page
st.header("🏏 ICC T20 World Cup 2026 — Fantasy Dashboard")

# create 4 tabs — each variable holds a context manager for that tab's content
tab_rounds, tab_group, tab_players, tab_audit = st.tabs([
    "🏆📅 Round History & Standings",
    "👤 Group View",
    "🔍 Player Points",
    "🔎 Audit",
])

with tab_rounds:
    round_df = get_round_summary()

    # pivot: rows = groups, columns = rounds
    pivot_df = round_df.pivot(index="participant_group", columns="round", values="round_points")
    pivot_df = pivot_df.fillna(0).astype(int)
    pivot_df.columns = [f"R{c}" for c in pivot_df.columns]
    pivot_df["Total"] = pivot_df.sum(axis=1)
    pivot_df = pivot_df.sort_values("Total", ascending=False)

    st.dataframe(pivot_df, use_container_width=True)

with tab_group:
    # load the full squad CSV
    squad_df = pd.read_csv(FANTASY_SQUAD_CSV)

    # group dropdown + round buttons
    grp_col, round_col = st.columns([1, 2])
    selected_group = grp_col.selectbox("Participant Group", sorted(squad_df["participant_group"].unique()))
    round_options  = sorted(squad_df["round"].unique())
    selected_round = round_col.segmented_control("Round", round_options, default=round_options[-1])

    # filter to selected group + round, copy to avoid SettingWithCopyWarning
    squad_group_round_df = squad_df[
        (squad_df["participant_group"] == selected_group) &
        (squad_df["round"] == selected_round)
    ].copy()

    # annotate player name: (C), (VC), 🔀 for converter
    def _annotate(player_row):
        name = player_row["player_name"]
        fpts = player_row["fantasy_points_final"]
        cpts = player_row["counted_points"]
        converter = " 🔀" if player_row["chip_converter"] == 1 else ""
        if fpts > 0 and (cpts / fpts) >= 1.8:
            return name + converter + " (C)"
        if player_row["vice_flag"] == 1:
            return name + converter + " (VC)"
        return name + converter
    squad_group_round_df["player_name"] = squad_group_round_df.apply(_annotate, axis=1)

    # sort XI by role then points descending
    role_order = {"WK": 0, "BAT": 1, "AR": 2, "BOWL": 3}
    squad_group_round_df["role_sort"] = squad_group_round_df["effective_position"].map(role_order)

    playing_xi_df = squad_group_round_df[squad_group_round_df["final_playing_xi"] == 1].sort_values(["role_sort", "fantasy_points_final"], ascending=[True, False])
    bench_df   = squad_group_round_df[squad_group_round_df["final_playing_xi"] == 0].sort_values("sub_order")

    # chips + autosub captions shown above the tables
    chip_map = {
        "chip_best11":   "⭐ Best 11",
        "chip_flexicap": "🧢 Flexi Cap",
    }
    active_chips = [label for col, label in chip_map.items() if squad_group_round_df[col].any()]

    # matchwinner: show player + counted points (potm_bonus not tracked in JSON pipeline)
    mw_rows = squad_group_round_df[squad_group_round_df["chip_matchwinner"] == 1]
    if not mw_rows.empty:
        mw = mw_rows.iloc[0]
        active_chips.append(f"🏆 Match Winner: {mw['player_name']} ({int(mw['fantasy_points_final'])} pts)")

    converted = squad_group_round_df[squad_group_round_df["chip_converter"] == 1]["player_name"].tolist()
    if converted:
        active_chips.append(f"🔀 Converter: {', '.join(converted)}")
    if active_chips:
        st.caption("Chip(s) used: " + " | ".join(active_chips))

    subs_in  = squad_group_round_df[squad_group_round_df["subbed_in"]  == 1]["player_name"].tolist()
    subs_out = squad_group_round_df[squad_group_round_df["subbed_out"] == 1]["player_name"].tolist()
    if subs_in:
        pairs = " | ".join(f"{o} → {i}" for o, i in zip(subs_out, subs_in))
        st.caption(f"↩ Autosub: {pairs}")

    # prepare display DataFrames before rendering
    # fantasy_points_final omitted from XI display (raw pre-captain pts; verified matchwinner chip worked correctly)
    cols_xi       = ["player_name", "effective_position", "counted_points", "points_breakdown_str"]
    cols_bench    = ["player_name", "effective_position", "sub_order", "fantasy_points_final", "points_breakdown_str"]
    xi_display_df    = playing_xi_df[cols_xi].reset_index(drop=True)
    bench_display_df = bench_df[cols_bench].reset_index(drop=True)
    bench_height  = max(178, 38 + len(bench_df) * 35)

    st.markdown("**Playing XI**")
    st.dataframe(xi_display_df, use_container_width=True, hide_index=True, height=423,
        column_config={
            "player_name":          st.column_config.TextColumn("Player",    width="medium"),
            "effective_position":   st.column_config.TextColumn("Pos",       width="small"),
            "counted_points":       st.column_config.NumberColumn("Points",  width="small"),
            "points_breakdown_str":     st.column_config.TextColumn("Breakdown", width="large"),
        })

    st.markdown("**Bench**")
    st.dataframe(bench_display_df, use_container_width=True, hide_index=True, height=bench_height,
        column_config={
            "player_name":          st.column_config.TextColumn("Player",    width="medium"),
            "effective_position":   st.column_config.TextColumn("Pos",       width="small"),
            "sub_order":            st.column_config.NumberColumn("Sub #",   width="small"),
            "fantasy_points_final": st.column_config.NumberColumn("Points",  width="small"),
            "points_breakdown_str":     st.column_config.TextColumn("Breakdown", width="large"),
        })

    st.metric("Round Total", int(squad_group_round_df["counted_points"].sum()))

with tab_players:
    # load the batch CSV which has per-player per-match stats
    stats_df = pd.read_csv(PLAYER_STATS_CSV)

    # filter dropdowns
    role_col, nation_col, player_col = st.columns([1, 1, 2])
    role_options   = ["All"] + sorted(stats_df["role"].dropna().str.upper().unique().tolist())
    nation_options = ["All"] + sorted(stats_df["nation_code"].dropna().unique().tolist())

    selected_role   = role_col.selectbox("Role", role_options)
    selected_nation = nation_col.selectbox("Nation", nation_options)

    # apply filters before building player list
    stats_role_nation_df = stats_df.copy()
    if selected_role != "All":
        stats_role_nation_df = stats_role_nation_df[stats_role_nation_df["role"].str.upper() == selected_role]
    if selected_nation != "All":
        stats_role_nation_df = stats_role_nation_df[stats_role_nation_df["nation_code"] == selected_nation]

    players         = sorted(stats_role_nation_df["canonical_player_name"].dropna().unique())
    selected_player = player_col.selectbox("Player", players)

    # filter to selected player
    selected_player_matchwise_df = stats_role_nation_df[stats_role_nation_df["canonical_player_name"] == selected_player]

    # summary metrics
    col_total, col_matches, col_avg = st.columns(3)
    col_total.metric("Total Points", int(selected_player_matchwise_df["fantasy_points_final"].sum()))
    col_matches.metric("Matches", len(selected_player_matchwise_df))
    col_avg.metric("Avg / Match", round(selected_player_matchwise_df["fantasy_points_final"].mean(), 1))

    # per-match breakdown — pick whichever date/label column has data
    selected_player_matchwise_display_df = selected_player_matchwise_df.copy()
    selected_player_matchwise_display_df["Match Date"] = selected_player_matchwise_display_df.get("match_date_y",  selected_player_matchwise_display_df.get("match_date_x"))
    selected_player_matchwise_display_df["Match"]      = selected_player_matchwise_display_df.get("match_label_y", selected_player_matchwise_display_df.get("match_label_x"))

    match_cols    = ["Match Date", "Match", "fantasy_points_final", "points_breakdown_str"]
    match_display_df = selected_player_matchwise_display_df[match_cols].sort_values("Match Date").reset_index(drop=True)
    st.dataframe(match_display_df, use_container_width=True, hide_index=True,
        column_config={
            "fantasy_points_final": st.column_config.NumberColumn("Points"),
            "points_breakdown_str":     st.column_config.TextColumn("Breakdown"),
        })

with tab_audit:
    API_PATH = API_AUDIT_PATH

    if not Path(API_PATH).exists():
        st.info("Audit data is not available in this deployment. Run locally with the API Excel file to use this tab.")
        api_df = None
    else:
        try:
            api_df = pd.read_excel(API_PATH, sheet_name="Fantasy Points")
        except Exception as e:
            st.error(f"Could not load API file: {e}")
            api_df = None

    if api_df is not None:
        audit_stats_df = pd.read_csv(PLAYER_STATS_CSV)

        # aggregate our data by player + match
        audit_stats_df["match_name_key"] = audit_stats_df["match_label_y"].str.lower().str.strip()
        json_points_df = audit_stats_df.groupby(["canonical_player_name", "match_name_key"], as_index=False)["fantasy_points_final"].sum()
        json_points_df["join_key"] = json_points_df["canonical_player_name"].str.lower().str.strip()

        # aggregate API data by player + match, subtract POTM bonus
        api_df["match_name_key"] = api_df["Match Name"].str.lower().str.strip()
        api_points_df = api_df.groupby(["Player Name", "match_name_key", "Round"], as_index=False).agg(
            api_points=("Fantasy Points", "sum"),
            potm=("Pts POTM", "sum")
        )
        api_points_df["api_points"] = api_points_df["api_points"] - api_points_df["potm"]
        api_points_df["join_key"]   = api_points_df["Player Name"].str.lower().str.strip()
        api_points_df = api_points_df.rename(columns={"Round": "round_num"})

        # join on player + match name
        audit_merged_df = json_points_df.merge(
            api_points_df[["join_key", "match_name_key", "round_num", "api_points"]],
            on=["join_key", "match_name_key"],
            how="inner"
        )
        audit_merged_df["delta"] = audit_merged_df["fantasy_points_final"] - audit_merged_df["api_points"]
        source_point_difference_df = audit_merged_df[audit_merged_df["delta"] != 0].sort_values("delta", key=abs, ascending=False)

        col_matched, col_diffs, col_exact = st.columns(3)
        col_matched.metric("Matched rows", len(audit_merged_df))
        col_diffs.metric("Differences", len(source_point_difference_df))
        col_exact.metric("Exact matches", len(audit_merged_df) - len(source_point_difference_df))

        if source_point_difference_df.empty:
            st.success("No differences found.")
        else:
            audit_cols    = ["canonical_player_name", "round_num", "match_name_key",
                             "fantasy_points_final", "api_points", "delta"]
            audit_main_display_df = source_point_difference_df[audit_cols].reset_index(drop=True)
            st.dataframe(audit_main_display_df, use_container_width=True, hide_index=True)

        # --- Standings Impact ---
        st.divider()
        st.subheader("Standings Impact")
        st.caption(
            "Net effect of all player-level deltas on each group's total. "
            "Delta is applied at raw points level (pre-captain multiplier) — a conservative approximation."
        )

        # aggregate audit deltas to round level (audit_merged_df has round_num from the API side)
        round_player_delta_df = audit_merged_df.groupby(
            ["canonical_player_name", "round_num"], as_index=False
        )["delta"].sum()
        round_player_delta_df["join_key"] = round_player_delta_df["canonical_player_name"].str.lower().str.strip()

        # squad: one row per player per round per group, XI only
        # FANTASY_SQUAD_CSV uses "player_name"; normalise for join
        squad_impact_df = pd.read_csv(FANTASY_SQUAD_CSV)
        squad_xi_impact_df = squad_impact_df[squad_impact_df["final_playing_xi"] == 1][
            ["participant_group", "player_name", "round", "counted_points"]
        ].copy()
        squad_xi_impact_df["join_key"] = squad_xi_impact_df["player_name"].str.lower().str.strip()

        # join player+round deltas onto the squad
        merged_impact_df = squad_xi_impact_df.merge(
            round_player_delta_df[["join_key", "round_num", "delta"]],
            left_on=["join_key", "round"],
            right_on=["join_key", "round_num"],
            how="left"
        )
        merged_impact_df["delta"] = merged_impact_df["delta"].fillna(0)

        # group totals + net delta per group
        standings_impact_df = merged_impact_df.groupby("participant_group", as_index=False).agg(
            our_total=("counted_points", "sum"),
            net_delta=("delta", "sum")
        )
        standings_impact_df["api_adjusted"] = (
            standings_impact_df["our_total"] + standings_impact_df["net_delta"]
        ).round(0).astype(int)
        standings_impact_df["net_delta"]  = standings_impact_df["net_delta"].round(0).astype(int)
        standings_impact_df["our_rank"]   = standings_impact_df["our_total"].rank(ascending=False, method="min").astype(int)
        standings_impact_df["api_rank"]   = standings_impact_df["api_adjusted"].rank(ascending=False, method="min").astype(int)
        standings_impact_df["rank_change"] = standings_impact_df["api_rank"] - standings_impact_df["our_rank"]
        standings_impact_df = standings_impact_df.sort_values("our_rank").reset_index(drop=True)

        # round coverage info
        api_rounds   = sorted(audit_merged_df["round_num"].dropna().unique().astype(int).tolist())
        total_rounds = sorted(squad_impact_df["round"].dropna().unique().astype(int).tolist())
        covered_str  = ", ".join(f"R{r}" for r in api_rounds)
        missing      = [r for r in total_rounds if r not in api_rounds]
        missing_str  = (", ".join(f"R{r}" for r in missing)) if missing else "none"
        st.info(
            f"API data covers {covered_str} ({len(api_rounds)} of {len(total_rounds)} rounds). "
            f"Missing rounds (delta = 0): {missing_str}."
        )

        # summary banner
        rank_changes = (standings_impact_df["rank_change"] != 0).sum()
        if rank_changes == 0:
            st.success("No rank changes — standings are identical under both scoring systems.")
        else:
            st.warning(f"{rank_changes} group(s) would change rank if API points were used.")

        st.dataframe(
            standings_impact_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "participant_group": st.column_config.TextColumn("Group",          width="medium"),
                "our_total":         st.column_config.NumberColumn("Our Total",    width="small"),
                "net_delta":         st.column_config.NumberColumn("Net Δ",        width="small"),
                "api_adjusted":      st.column_config.NumberColumn("API Adjusted", width="small"),
                "our_rank":          st.column_config.NumberColumn("Our Rank",     width="small"),
                "api_rank":          st.column_config.NumberColumn("API Rank",     width="small"),
                "rank_change":       st.column_config.NumberColumn("Rank Δ",       width="small"),
            }
        )

