import os
from google import genai
from data_layer import get_standings, get_round_summary, get_top_players, get_chip_summary


# Tool functions — Gemini reads the docstrings to decide when to call each one
def _standings() -> str:
    """Returns the overall fantasy leaderboard — total points per group, sorted by rank."""
    return get_standings().to_markdown(index=False)


def _round_summary() -> str:
    """Returns points per group per round, with cumulative total and round rank."""
    return get_round_summary().to_markdown(index=False)


def _top_players(top_n: int = 10, role: str = "") -> str:
    """Returns top players ranked by total fantasy points. Includes matches_played and avg_points (points per match) for value analysis. Optionally filter by role.

    Args:
        top_n: Number of players to return. Defaults to 10.
        role: Filter by role: BAT, BOWL, AR, or WK. Leave empty for all roles.
    """
    return get_top_players(top_n=top_n, role=role if role else None).to_markdown(index=False)


def _chip_summary() -> str:
    """Returns chip usage per group — which round each chip (BEST11, FLEXICAP, MATCHWINNER, CONVERTER) was played, or — if unused."""
    return get_chip_summary().to_markdown(index=False)


def run_agent(question: str) -> str:
    # Connect to Gemini API
    client = genai.Client(api_key=os.environ["GOOGLE_API_KEY_WT20_2026"])

    # Gemini handles the tool loop automatically — no while loop needed
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=question,
        config={"tools": [_standings, _round_summary, _top_players, _chip_summary],
                "automatic_function_calling": {"disable": False},
                "system_instruction": "Answer concisely. Return tables as-is. No bullet points, no prose explanation unless explicitly asked."},
    )
    return response.text


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    print(run_agent("Which players offer the best value? Consider points per match and total points."))
