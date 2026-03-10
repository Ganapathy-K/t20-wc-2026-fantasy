from scoring_rules_config_dict import SCORING_RULES_CONFIG_DICT

# threshold bonus helper block
def compute_bonus_from_threshold_rules(metric_value_float, threshold_rules_list):
    bonus_points_int = 0
    for rule_dict in threshold_rules_list:
        lower_ok_bool = metric_value_float >= rule_dict["min_inclusive"]
        upper_ok_bool = True if rule_dict["max_exclusive"] is None else metric_value_float < rule_dict["max_exclusive"]
        if lower_ok_bool and upper_ok_bool:
            bonus_points_int += rule_dict["points"]
    return bonus_points_int

# component scoring core block
def compute_component_points_dict(player_match_stats_dict):

    rules_config_dict = SCORING_RULES_CONFIG_DICT

    # initialize atomic component point counters
    batting_points_int = 0
    bowling_points_int = 0
    fielding_points_int = 0
    appearance_points_int = 0
    strike_rate_points_int = 0
    economy_rate_points_int = 0
    dot_ball_points_int = 0
    # atomic batting components
    runs_points_int = 0
    boundary_points_int = 0
    six_points_int = 0
    milestone_bonus_points_int = 0
    duck_points_int = 0
    # atomic bowling components
    wicket_points_int = 0
    lbw_bowled_bonus_points_int = 0
    maiden_points_int = 0
    wicket_haul_bonus_points_int = 0
    # atomic fielding components
    catch_points_int = 0
    catch_3_bonus_points_int = 0
    stumping_points_int = 0
    runout_direct_points_int = 0
    runout_indirect_points_int = 0

    # batting block
    batting_rules_dict = rules_config_dict["batting_rules_dict"]
    runs_points_int = player_match_stats_dict["runs"] * batting_rules_dict["points_per_run_int"]
    boundary_points_int = player_match_stats_dict["fours"] * batting_rules_dict["boundary_four_bonus_int"]
    six_points_int = player_match_stats_dict["sixes"] * batting_rules_dict["six_bonus_int"]
    batting_points_int += runs_points_int + boundary_points_int + six_points_int

    # batting milestone — highest crossed only
    milestone_bonus_points_int = 0
    for milestone_runs_int, milestone_points_int in batting_rules_dict["milestone_bonus_by_runs_dict"].items():
        if player_match_stats_dict["runs"] >= milestone_runs_int:
            milestone_bonus_points_int = max(milestone_bonus_points_int, milestone_points_int)
    batting_points_int += milestone_bonus_points_int
    if player_match_stats_dict["runs"] == 0 and player_match_stats_dict["is_out"] == 1 and player_match_stats_dict["role"] in batting_rules_dict["duck_applicable_roles_set"]:
        duck_points_int = batting_rules_dict["duck_penalty_points_int"]
        batting_points_int += duck_points_int

    # bowling block
    bowling_rules_dict = rules_config_dict["bowling_rules_dict"]

    wicket_points_int = player_match_stats_dict["wickets"] * bowling_rules_dict["points_per_wicket_int"]
    lbw_bowled_bonus_points_int = player_match_stats_dict["bowled_lbw_wickets"] * bowling_rules_dict["lbw_bowled_bonus_int"]
    maiden_points_int = player_match_stats_dict.get("maidens", 0) * bowling_rules_dict["maiden_over_bonus_int"]

    wicket_haul_bonus_points_int = 0
    for wicket_count_int, haul_bonus_int in bowling_rules_dict["wicket_haul_bonus_by_count_dict"].items():
        if player_match_stats_dict["wickets"] >= wicket_count_int:
            wicket_haul_bonus_points_int = max(wicket_haul_bonus_points_int, haul_bonus_int)

    bowling_points_int += wicket_points_int + lbw_bowled_bonus_points_int + maiden_points_int + wicket_haul_bonus_points_int

    # dot balls
    dot_ball_points_int += player_match_stats_dict["dot_balls"] * rules_config_dict["dot_ball_rules_dict"]["points_per_dot_ball_int"]

    # fielding block
    fielding_rules_dict = rules_config_dict["fielding_rules_dict"]
    catch_points_int = player_match_stats_dict["catches"] * fielding_rules_dict["points_per_catch_int"]
    catch_3_bonus_points_int = (player_match_stats_dict["catches"] // 3) * fielding_rules_dict["three_catch_bonus_int"]
    stumping_points_int = player_match_stats_dict["stumpings"] * fielding_rules_dict["points_per_stumping_int"]
    runout_direct_points_int = player_match_stats_dict["runouts_direct"] * fielding_rules_dict["points_per_runout_direct_int"]
    runout_indirect_points_int = player_match_stats_dict["runouts_indirect"] * fielding_rules_dict["points_per_runout_indirect_int"]
    fielding_points_int += catch_points_int + catch_3_bonus_points_int + stumping_points_int + runout_direct_points_int + runout_indirect_points_int

    # strike rate
    if player_match_stats_dict["balls_faced"] >= 10:
        strike_rate_points_int += compute_bonus_from_threshold_rules(player_match_stats_dict["strike_rate"], rules_config_dict["strike_rate_bonus_rules_list"])

    # economy
    if player_match_stats_dict["legal_balls_bowled"] >= 12:
        economy_rate_points_int += compute_bonus_from_threshold_rules(player_match_stats_dict["bowling_economy"], rules_config_dict["economy_rate_bonus_rules_list"])

    # appearance
    if player_match_stats_dict["played_match_flag"] == 1:
        appearance_points_int += rules_config_dict["appearance_rules_dict"]["points_per_played_match_int"]

    base_total_points_int = batting_points_int + bowling_points_int + fielding_points_int + strike_rate_points_int + economy_rate_points_int + dot_ball_points_int + appearance_points_int

    # captain multiplier
    captain_rules_dict = rules_config_dict["captain_rules_dict"]
    multiplier_int = 1
    if player_match_stats_dict["is_captain_flag"] == 1:
        multiplier_int = captain_rules_dict["captain_points_multiplier_int"]
    elif player_match_stats_dict["is_vicecaptain_flag"] == 1:
        multiplier_int = captain_rules_dict["vicecaptain_points_multiplier_int"]

    final_total_points_int = base_total_points_int * multiplier_int

    return {
        # --- batting atomic ---
        "runs_points_int": runs_points_int,
        "boundary_points_int": boundary_points_int,
        "six_points_int": six_points_int,
        "milestone_bonus_points_int": milestone_bonus_points_int,
        "duck_points_int": duck_points_int,
        # --- bowling atomic ---
        "wicket_points_int": wicket_points_int,
        "lbw_bowled_bonus_points_int": lbw_bowled_bonus_points_int,
        "maiden_points_int": maiden_points_int,
        "wicket_haul_bonus_points_int": wicket_haul_bonus_points_int,
        "dot_ball_points_int": dot_ball_points_int,
        # --- fielding atomic ---
        "catch_points_int": catch_points_int,
        "catch_3_bonus_points_int": catch_3_bonus_points_int,
        "stumping_points_int": stumping_points_int,
        "runout_direct_points_int": runout_direct_points_int,
        "runout_indirect_points_int": runout_indirect_points_int,
        # --- rate / participation ---
        "strike_rate_points_int": strike_rate_points_int,
        "economy_rate_points_int": economy_rate_points_int,
        "appearance_points_int": appearance_points_int,
        # --- rolled up (derived) ---
        "batting_points_int": batting_points_int,
        "bowling_points_int": bowling_points_int,
        "fielding_points_int": fielding_points_int,
        # --- totals ---
        "base_total_points_int": base_total_points_int,
        "captain_multiplier_int": multiplier_int,
        "final_total_points_int": final_total_points_int,
    }

# total only (backward compatible)
def compute_player_match_fantasy_points(player_match_stats_dict):
    return compute_component_points_dict(player_match_stats_dict)["final_total_points_int"]


# audit mode
def compute_player_match_fantasy_points_with_breakdown(player_match_stats_dict):
    component_points_dict = compute_component_points_dict(player_match_stats_dict)
    return component_points_dict["final_total_points_int"], component_points_dict

if __name__ == "__main__":
    print("scoring_engine_functions.py seems to work...")