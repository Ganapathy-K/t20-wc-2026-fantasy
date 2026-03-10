# ICC T20 World Cup 2026 — Fantasy Cricket Scoring System

An end-to-end fantasy cricket scoring pipeline for a private draft-auction league played across the ICC T20 World Cup 2026. Covers the full tournament (9 rounds, ~40 matches, 9 participating teams) with automated points calculation, chip logic, a Streamlit results dashboard, and a Gemini AI agent for natural language queries.

---

## Problem Statement

Running a fantasy cricket league manually is error-prone and time-consuming. This system automates the entire workflow:

- Parse official match data (Cricsheet JSON) into structured player stats
- Apply a custom fantasy scoring ruleset (batting, bowling, fielding, POTM)
- Handle 8 participant groups with squad selections, bench substitutions, and special chips
- Produce a verified leaderboard and per-round breakdown
- Expose results via an interactive dashboard and a conversational AI agent

---

## Pipeline Architecture

```
Cricsheet JSONs
      │
      ▼
build_player_name_mapping_from_sources.py   ← fuzzy-match JSON names → master player list
      │
      ▼
run_batch_pipeline.py
  └── match_file_to_points_dataframe.py     ← per-match stats extraction
  └── data_source_to_player_stats_dict.py   ← batting / bowling / fielding parsing
  └── scoring_engine_functions.py           ← apply scoring_rules_config_dict.py
      │
      ▼
icc_mens_t20wc_2026_9teams_match_points_batch.csv   (gitignored)
      │
      ▼
contest_scoring_v2.py                       ← load Excel lineups + apply chips
  └── pipeline_utils.py
      │
      ▼
participants_match_df.csv                   ← full per-player per-round scored data
group_overall_totals.csv                    ← final leaderboard
      │
      ▼
apps/t20_fantasy_agent/
  ├── dashboard.py                          ← Streamlit results dashboard
  └── agent.py                             ← Gemini AI agent (natural language queries)
```

---

## Scoring Rules

| Category | Rule |
|---|---|
| Appearance | +4 pts |
| Runs | +1 pt each; milestones +4/8/12/16 at 25/50/75/100; duck −2 |
| Fours / Sixes | +4 / +6 pts each |
| Wickets | +30 pts; +8 bonus for bowled/LBW |
| Maiden over | +12 pts (full 6-ball overs only) |
| Catch / Stumping | +8 / +12 pts |
| Run-out (direct/indirect) | +12 / +6 pts |
| Dot balls | +1 pt each |
| Captain | 2× multiplier on all points |

## Chips

| Chip | Effect |
|---|---|
| Autosub | Replace non-playing XI with bench players who played |
| Best 11 | System picks best 11 from squad; top scorer gets 2× |
| Flexi Cap | 2× for the highest scorer in the round |
| Converter | Permanently change a player's role from a round onward |
| Match Winner | 3× if your POTM pick is from the winning team |

---

## Key Engineering Challenges Solved

### 1. Afghanistan matches — no Cricsheet JSON
Afghanistan's matches were not available in Cricsheet. Opponent team players (NZ in R1, SA in R2) had missing `role` and `team_code` values in the AFG Excel sheet. Fixed by enriching those rows from the master Players List at runtime in `contest_scoring_v2.py`.

### 2. Abandoned match handling
NZ vs PAK Super 8 (match 1512759) was abandoned without a ball bowled. The schedule correctly assigns `team_round=5` for both teams with 0 points — no manual intervention needed.

### 3. Scoring edge cases
- **Retired hurt**: excluded from bowler wicket credit (whitelist approach)
- **Partial overs**: overs with fewer than 6 legal deliveries do not qualify for maiden bonus
- **Name disambiguation**: Cricsheet player names differ from ESPN/Cricbuzz names; resolved via rapidfuzz fuzzy matching (cutoff 88%) against the master Excel player list

### 4. Cross-system audit & standings proof
A second participant independently calculated fantasy points using a different API source. The Audit tab in the dashboard maps all player-level deltas back to participant groups, computes net impact per group, and proves the final standings are identical under both scoring systems.

---

## Dashboard

Run with:
```bash
streamlit run apps/t20_fantasy_agent/dashboard.py
```

| Tab | Description |
|---|---|
| Round History & Standings | Pivot table: groups × rounds, sorted by total — the final leaderboard |
| Group View | XI + bench per group per round; chips used, autosubs, round total |
| Player Points | Per-player stats filtered by role / nation; match-by-match breakdown |
| Audit | Delta comparison vs external API source; standings impact analysis |

---

## AI Agent

An ADK-style Gemini agent (`gemini-2.5-flash`) answers natural language questions about the tournament data using tool calls into `data_layer.py`.

```bash
python apps/t20_fantasy_agent/agent.py
```

Example queries:
- *"Who were the top 5 bowlers by total fantasy points?"*
- *"Which group had the highest points in Round 6?"*
- *"Show me all all-rounders from India."*

Requires a `.env` file in the project root:
```
GOOGLE_API_KEY_WT20_2026=your_key_here
```

---

## Project Structure

```
t20_wc_2026_fantasy/
├── apps/
│   └── t20_fantasy_agent/
│       ├── agent.py                  # Gemini AI agent
│       ├── dashboard.py              # Streamlit dashboard
│       └── data_layer.py             # Data access layer
├── icc_mens_t20_world_cup_male_json/ # Cricsheet JSONs (gitignored)
├── data/outputs/                     # Timestamped run archives (gitignored)
├── logs/                             # Pipeline logs (gitignored)
├── build_player_name_mapping_from_sources.py
├── contest_scoring_v2.py
├── data_source_to_player_stats_dict.py
├── match_file_to_points_dataframe.py
├── pipeline_utils.py
├── run_batch_pipeline.py
├── scoring_engine_functions.py
├── scoring_rules_config_dict.py
├── .env                              # API keys (gitignored)
└── .gitignore
```

---

## Setup

```bash
pip install pandas openpyxl rapidfuzz streamlit google-generativeai python-dotenv
```

The Excel workbook (`ICC T20 WC 2026 Auction Game.xlsx`) containing participant squads, lineups, and chip selections is not included in this repository. The pipeline reads it from a configurable path in `pipeline_utils.py`.

---

## Tech Stack

- **Python 3.13** — pandas, openpyxl, rapidfuzz
- **Streamlit 1.55** — results dashboard
- **Google Gemini 2.5 Flash** — AI agent
- **Cricsheet** — match data source (open JSON format)
