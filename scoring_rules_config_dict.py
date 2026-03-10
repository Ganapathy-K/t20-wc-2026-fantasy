SCORING_RULES_CONFIG_DICT = {

    # appearance rules block
    "appearance_rules_dict": {
        "points_per_played_match_int": 4
    },

    # captain multiplier rules block
    "captain_rules_dict": {
        "captain_points_multiplier_int": 2,
        "vicecaptain_points_multiplier_int": 1
    },

    # batting rules block
    "batting_rules_dict": {
        "points_per_run_int": 1,
        "boundary_four_bonus_int": 4,
        "six_bonus_int": 6,
        "milestone_bonus_by_runs_dict": {25:4, 50:8, 75:12, 100:16},
        "duck_penalty_points_int": -2,
        "duck_applicable_roles_set": {"BAT","AR","WK"}
    },

    # bowling rules block
    "bowling_rules_dict": {
        "points_per_wicket_int": 30,
        "lbw_bowled_bonus_int": 8,
        "maiden_over_bonus_int": 12,
        "wicket_haul_bonus_by_count_dict": {3:4,4:8,5:12}
    },

    # fielding rules block
    "fielding_rules_dict": {
        "points_per_catch_int": 8,
        "three_catch_bonus_int": 4,
        "points_per_stumping_int": 12,
        "points_per_runout_direct_int": 12,
        "points_per_runout_indirect_int": 6
    },

    # strike rate threshold rules block
    "strike_rate_bonus_rules_list": [
        {"min_inclusive":170,"max_exclusive":None,"points":6},
        {"min_inclusive":150,"max_exclusive":170,"points":4},
        {"min_inclusive":130,"max_exclusive":150,"points":2},
        {"min_inclusive":60,"max_exclusive":70,"points":-2},
        {"min_inclusive":50,"max_exclusive":60,"points":-4},
        {"min_inclusive":0,"max_exclusive":50,"points":-6}
    ],

    # economy rate threshold rules block
    "economy_rate_bonus_rules_list": [
        {"min_inclusive":0,"max_exclusive":5,"points":6},
        {"min_inclusive":5,"max_exclusive":6,"points":4},
        {"min_inclusive":6,"max_exclusive":7,"points":2},
        {"min_inclusive":10,"max_exclusive":11,"points":-2},
        {"min_inclusive":11,"max_exclusive":12,"points":-4},
        {"min_inclusive":12,"max_exclusive":None,"points":-6}
    ],

    # dot ball rules block
    "dot_ball_rules_dict": {
        "points_per_dot_ball_int": 1
    }
}
