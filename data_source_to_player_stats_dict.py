import json

from pipeline_utils import get_delivery_info_from_wrapper, is_legal_delivery_bool_from_delivery

# Dismissal kinds that credit the bowler with a wicket (whitelist approach).
# "retired hurt" and "run out" are intentionally excluded.
BOWLER_WICKET_KINDS = {"bowled", "caught", "lbw", "stumped", "hit wicket", "caught and bowled"}

# empty player stats builder block
def build_empty_player_match_stats():
    return {
        "runs": 0,
        "balls_faced": 0,
        "fours": 0,
        "sixes": 0,
        "is_out": 0,
        "wickets": 0,
        "bowled_lbw_wickets": 0,
        "maidens": 0,
        "legal_balls_bowled": 0,
        "runs_conceded_by_bowler": 0,
        "dot_balls": 0,
        "catches": 0,
        "stumpings": 0,
        "runouts_direct": 0,
        "runouts_indirect": 0,
        "strike_rate": 0.0,
        "bowling_economy": 0.0,
        "role": "BAT",
        "played_match_flag": 0,
        "is_captain_flag": 0,
        "is_vicecaptain_flag": 0
    }


# player stats extraction block
def extract_player_match_stats(match_file_path_str): 

    with open(match_file_path_str, "r") as match_file:
        match_data_dict = json.load(match_file)

    player_name_to_stats_dict = {}

    # pre-seed all playing XI players so zero-event players are included
    teams_players_dict = match_data_dict.get("info", {}).get("players", {})
    for team_players in teams_players_dict.values():
        for player_name_str in team_players:
            if player_name_str not in player_name_to_stats_dict:
                player_name_to_stats_dict[player_name_str] = build_empty_player_match_stats()
            player_name_to_stats_dict[player_name_str]["played_match_flag"] = 1

    for innings_dict in match_data_dict["innings"]:

        for over_dict in innings_dict["overs"]:
            over_runs_off_bat_int = 0
            over_extras_conceded_int = 0
            over_bowler_name_str = None
            legal_balls_in_over_by_bowler_dict = {}
            for delivery_wrapper_dict in over_dict["deliveries"]:
                delivery_info_dict = get_delivery_info_from_wrapper(delivery_wrapper_dict)

                batter_name_str = delivery_info_dict["batter"]
                bowler_name_str = delivery_info_dict["bowler"]

                runs_dict = delivery_info_dict.get("runs", {})
                extras_dict = delivery_info_dict.get("extras", {})

                if over_bowler_name_str is None:
                    over_bowler_name_str = bowler_name_str

                over_runs_off_bat_int += runs_dict.get("batter", 0)
                over_extras_conceded_int += extras_dict.get("wides", 0) + extras_dict.get("noballs", 0)

                for player_name_str in [batter_name_str, bowler_name_str]:
                    if player_name_str not in player_name_to_stats_dict:
                        player_name_to_stats_dict[player_name_str] = build_empty_player_match_stats()
                batter_stats_dict = player_name_to_stats_dict[batter_name_str]
                bowler_stats_dict = player_name_to_stats_dict[bowler_name_str]
                batter_stats_dict["played_match_flag"] = 1
                bowler_stats_dict["played_match_flag"] = 1

                # batting accumulation block
                batter_stats_dict["runs"] += runs_dict["batter"]

                if is_legal_delivery_bool_from_delivery(delivery_info_dict):
                    batter_stats_dict["balls_faced"] += 1
                if runs_dict["batter"] == 4 and not runs_dict.get("non_boundary", False):
                    batter_stats_dict["fours"] += 1
                if runs_dict["batter"] == 6 and not runs_dict.get("non_boundary", False):
                    batter_stats_dict["sixes"] += 1

                # bowling accumulation block
                if is_legal_delivery_bool_from_delivery(delivery_info_dict):
                    bowler_stats_dict["legal_balls_bowled"] += 1
                    legal_balls_in_over_by_bowler_dict[bowler_name_str] = legal_balls_in_over_by_bowler_dict.get(bowler_name_str, 0) + 1
                    # extras_dict = delivery_info_dict.get("extras", {})
                    is_true_dot_ball_bool = runs_dict.get("batter", 0) == 0 and extras_dict.get("wides", 0) == 0 and extras_dict.get("noballs", 0) == 0
                    if is_true_dot_ball_bool:
                        bowler_stats_dict["dot_balls"] += 1

                extras_dict = delivery_info_dict.get("extras", {})
                bye_runs = extras_dict.get("byes", 0)
                legbye_runs = extras_dict.get("legbyes", 0)
                runs_conceded = runs_dict["total"] - bye_runs - legbye_runs
                bowler_stats_dict["runs_conceded_by_bowler"] += runs_conceded

                # wicket and fielding block
                if "wickets" in delivery_info_dict:
                    for wicket_dict in delivery_info_dict["wickets"]:
                        dismissal_kind_str = wicket_dict["kind"]
                        fielder_list = wicket_dict.get("fielders", [])
                        # only mark batter as out for genuine dismissals (not retired hurt)
                        if dismissal_kind_str != "retired hurt":
                            batter_stats_dict["is_out"] = 1
                        if dismissal_kind_str in BOWLER_WICKET_KINDS:
                            bowler_stats_dict["wickets"] += 1
                        if dismissal_kind_str in {"bowled", "lbw"}:
                            bowler_stats_dict["bowled_lbw_wickets"] += 1
                        if dismissal_kind_str == "caught":
                            for fielder_dict in fielder_list:
                                fielder_name_str = fielder_dict["name"]
                                if fielder_name_str not in player_name_to_stats_dict:
                                    player_name_to_stats_dict[fielder_name_str] = build_empty_player_match_stats()
                                player_name_to_stats_dict[fielder_name_str]["catches"] += 1
                        elif dismissal_kind_str == "caught and bowled":
                            # bowler is the catcher — Cricsheet lists no fielder for C&B
                            bowler_stats_dict["catches"] += 1
                        if dismissal_kind_str == "stumped":
                            for fielder_dict in fielder_list:
                                fielder_name_str = fielder_dict["name"]
                                if fielder_name_str not in player_name_to_stats_dict:
                                    player_name_to_stats_dict[fielder_name_str] = build_empty_player_match_stats()
                                player_name_to_stats_dict[fielder_name_str]["stumpings"] += 1
                        if dismissal_kind_str == "run out":
                            if len(fielder_list) == 1:
                                fielder_name_str = fielder_list[0]["name"]
                                if fielder_name_str not in player_name_to_stats_dict:
                                    player_name_to_stats_dict[fielder_name_str] = build_empty_player_match_stats()
                                player_name_to_stats_dict[fielder_name_str]["runouts_direct"] += 1
                            else:
                                for fielder_dict in fielder_list:
                                    fielder_name_str = fielder_dict["name"]
                                    if fielder_name_str not in player_name_to_stats_dict:
                                        player_name_to_stats_dict[fielder_name_str] = build_empty_player_match_stats()
                                    player_name_to_stats_dict[fielder_name_str]["runouts_indirect"] += 1

            # maiden over detection block — requires full 6 legal deliveries (partial overs don't count)
            if over_bowler_name_str is not None:
                legal_balls_this_over = legal_balls_in_over_by_bowler_dict.get(over_bowler_name_str, 0)
                if over_runs_off_bat_int == 0 and over_extras_conceded_int == 0 and legal_balls_this_over >= 6:
                    bowler_stats_dict = player_name_to_stats_dict.setdefault(over_bowler_name_str, {})
                    bowler_stats_dict["maidens"] = bowler_stats_dict.get("maidens", 0) + 1

    # derived metrics block
    for player_stats_dict in player_name_to_stats_dict.values():

        if player_stats_dict["balls_faced"] > 0:
            player_stats_dict["strike_rate"] = round(player_stats_dict["runs"] * 100 / player_stats_dict["balls_faced"], 2)

        if player_stats_dict["legal_balls_bowled"] > 0:
            player_stats_dict["bowling_economy"] = round(player_stats_dict["runs_conceded_by_bowler"] * 6 / player_stats_dict["legal_balls_bowled"], 2)

    return player_name_to_stats_dict

if __name__ == "__main__":
    test_match_file_path_str = r"D:/Data Science/Visual Studio Code/icc_mens_t20_wc_2026_matchlogs_json/1512721.json"
    player_name_to_stats_dict = extract_player_match_stats(test_match_file_path_str)
    print("Players extracted:", len(player_name_to_stats_dict))
    first_player_name_str = next(iter(player_name_to_stats_dict))
    print("Sample player:", first_player_name_str)
    print("Sample stats:", player_name_to_stats_dict[first_player_name_str])
