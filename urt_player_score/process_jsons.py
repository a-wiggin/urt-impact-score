import json
import os
import time
import glob
import json

import tqdm
import polars as pl


def process_jsons(folder_path: str, game_type="TS", limit: int = 100) -> pl.DataFrame:
    """
    Reads all JSON files in a folder and combines them into a single Polars DataFrame.

    Args:
        folder_path: The path to the folder containing the JSON files.

    Returns:
        A Polars DataFrame containing the data from all JSON files.
    """
    if game_type == "TS":
        game_type_code = 4
    else:
        raise ValueError

    all_data = []
    for filename in tqdm.tqdm(glob.glob(os.path.join(folder_path, "*.json"))[:limit]):
        # df = pl.read_json(filename)
        # all_data.append(df)
        # print(filename)
        game_id = os.path.basename(filename).split(".")[0]
        # if int(game_id) < 18000:
        #     continue
        with open(filename, "r") as f:
            data = json.load(f)
            data["game_id"] = game_id
            all_data.append(data)

    df_all_data = pl.from_dicts(all_data)
    # df_out = pl.json_normalize(df_out, max_level=10, infer_schema_length=10000)
    df_all_data = df_all_data.select(
        pl.col("game_id"),
        pl.col("stats").struct.unnest(),
    ).filter(
        GameType=game_type_code,
    )
    df_games = df_all_data.select(
        "game_id",
        "Map",
        "GameType",
        "ScoreRed",
        "ScoreBlue",
        "TimeLimit",
    ).unique()

    df_players = (
        df_all_data.select("game_id", pl.col("Players"))
        .explode("Players")
        .select("game_id", pl.col("Players").struct.unnest())
        .select("game_id", "Auth", "Name", "Team", "PlayerNo", "Kills", "Deaths")
    )
    df_players = df_players.unique()

    df_rounds = (
        df_all_data.select("game_id", pl.col("Rounds"))
        .explode("Rounds")
        .select("game_id", pl.col("Rounds").struct.unnest())
    )

    df_kills = (
        df_rounds.select("game_id", "Round", pl.col("KillLog"))
        .explode("KillLog")
        .select("game_id", "Round", pl.col("KillLog").struct.unnest())
    )

    # remove duplicate kills
    df_kills = df_kills.unique()
    # force right order
    df_kills = df_kills.sort(
        "game_id", "Round", "Seconds", descending=(False, False, True)
    )

    if game_type == "TS":
        player_counts = (
            df_players.filter(pl.col("Team") != "spectator")
            .group_by("game_id")
            .agg(pl.len().alias("nb_of_players"))
        )
        print("number of players in games")
        print(player_counts["nb_of_players"].value_counts().sort("nb_of_players"))

        # remove games with irregular number of players, ie != 10
        irregular_games = player_counts.filter(pl.col("nb_of_players") != 10)[
            "game_id"
        ].to_list()
        print(
            f"ignoring {len(irregular_games)} games with irregular number of players of total of {len(df_games)}"
        )
        df_games = df_games.filter(~pl.col("game_id").is_in(irregular_games))
        df_players = df_players.filter(~pl.col("game_id").is_in(irregular_games))
        df_rounds = df_rounds.filter(~pl.col("game_id").is_in(irregular_games))
        df_kills = df_kills.filter(~pl.col("game_id").is_in(irregular_games))
        # TODO investigate why there are game with odd numbers like > 10, or < 10 =! 2 or 4

        # remove rounds where the same player dies twice
        killed_counts = df_kills.group_by("game_id", "Round", "Killed").agg(
            pl.len().alias("killed_count")
        )
        irregular_game_rounds = (
            killed_counts.filter(pl.col("killed_count") > 1)
            .select(
                pl.concat_str("game_id", pl.lit("-"), "Round").alias("game_round_id")
            )["game_round_id"]
            .to_list()
        )
        print(
            f"ignoring {len(irregular_game_rounds)} rounds with players dying more than once of total of {len(df_rounds)}"
        )
        condition_expr = (
            ~pl.concat_str("game_id", pl.lit("-"), "Round")
            .alias("game_round_id")
            .is_in(irregular_game_rounds)
        )
        df_rounds = df_rounds.filter(condition_expr)
        df_kills = df_kills.filter(condition_expr)

        # remove kill entries without Killed or Killer
        df_kills = df_kills.filter(
            (pl.col("Killed").is_not_null()) & (pl.col("Killer").is_not_null())
        )

        # remove any game where a player registered as spectator shows in the kill log
        spectators = (
            df_players.filter(pl.col("Team") == "spectator")
            .select("game_id", "PlayerNo")
            .unique()
        )

        df_killed_spectators = spectators.join(
            df_kills,
            left_on=["game_id", "PlayerNo"],
            right_on=["game_id", "Killed"],
            how="inner",
        )
        df_killer_spectators = spectators.join(
            df_kills,
            left_on=["game_id", "PlayerNo"],
            right_on=["game_id", "Killer"],
            how="inner",
        )
        invalid_games2 = (
            df_killed_spectators["game_id"].unique().to_list()
            + df_killer_spectators["game_id"].unique().to_list()
        )
        print(
            f"ignoring {len(invalid_games2)} games with spectators of total of {len(df_games)}"
        )
        df_games = df_games.filter(~pl.col("game_id").is_in(invalid_games2))
        df_players = df_players.filter(~pl.col("game_id").is_in(invalid_games2))
        df_rounds = df_rounds.filter(~pl.col("game_id").is_in(invalid_games2))
        df_kills = df_kills.filter(~pl.col("game_id").is_in(invalid_games2))

    return df_games, df_players, df_rounds, df_kills


def merge_kills_players(df_players, df_kills):
    # add info about killer
    df_kills_xl = df_kills.join(
        df_players,
        left_on=["game_id", "Killer"],
        right_on=["game_id", "PlayerNo"],
        how="left",
        validate="m:1",
    ).rename({"Team": "KillerTeam"})
    # add info about killed
    df_kills_xl = (
        df_kills_xl.join(
            df_players,
            left_on=["game_id", "Killed"],
            right_on=["game_id", "PlayerNo"],
            how="left",
            validate="m:1",
        )
        .rename({"Team": "KilledTeam", "Auth_right": "Auth_killed"})
        .with_columns(
            pl.when(pl.col("KilledTeam") == "red")
            .then(pl.lit(-1))
            .otherwise(pl.lit(0))
            .alias("AliveRed"),
            pl.when(pl.col("KilledTeam") == "blue")
            .then(pl.lit(-1))
            .otherwise(pl.lit(0))
            .alias("AliveBlue"),
        )
    )

    # calc cumulative sum
    df_kills_xl = df_kills_xl.with_columns(
        pl.col("AliveRed").cum_sum().over(["game_id", "Round"]) + 5,
        pl.col("AliveBlue").cum_sum().over(["game_id", "Round"]) + 5,
    )

    return df_kills_xl


def get_last_round(df_kills_xl):
    df_teams_round_end = df_kills_xl.group_by(
        "game_id",
        "Round",
    ).agg(
        pl.col("KillerTeam").last().alias("winner"),
        pl.col("AliveRed").last().alias("AliveRed"),
        pl.col("AliveBlue").last().alias("AliveBlue"),
    )

    return df_teams_round_end


def order_team_counts(col: pl.Expr):
    return (
        pl.when(col.str.split("-").list.get(0) > col.str.split("-").list.get(1))
        .then(col)
        .otherwise(
            pl.concat_str(
                col.str.split("-").list.get(1),
                pl.lit("-"),
                col.str.split("-").list.get(0),
            )
        )
    )


def transition_probs(df_kills_xl):
    agg_df = (
        df_kills_xl.filter(
            pl.col("team_counts_before_kill_ordered").str.split("-").list.get(1) == "1"
        )
        .group_by(
            "team_counts_before_kill_ordered",
            (
                pl.col("team_counts_after_kill_ordered").str.split("-").list.get(1)
                == "0"
            ).alias("round_won"),
        )
        .agg(pl.len().alias("count"))
        .pivot(index="team_counts_before_kill_ordered", on="round_won", values="count")
    )
    return agg_df


def ultimate_tran_probs(df_kills_xl):
    ultimate_tran_prob = (
        df_kills_xl.group_by(
            "team_counts_before_kill", "team_counts_before_kill_ordered", "Winner"
        )
        .agg(pl.len().alias("count"))
        .pivot(
            index=[
                "team_counts_before_kill",
                "team_counts_before_kill_ordered",
            ],
            on="Winner",
            values="count",
        )
        .with_columns(
            (
                pl.col("team_counts_before_kill").str.split("-").list.get(0)
                > pl.col("team_counts_before_kill").str.split("-").list.get(1)
            ).alias("is_ordered"),
            (
                pl.col("team_counts_before_kill").str.split("-").list.get(0)
                < pl.col("team_counts_before_kill").str.split("-").list.get(1)
            ).alias("is_not_ordered"),
            (
                pl.col("team_counts_before_kill").str.split("-").list.get(0)
                == pl.col("team_counts_before_kill").str.split("-").list.get(1)
            ).alias("is_tie"),
        )
        .with_columns(
            pl.when(pl.col("is_ordered"))
            .then(pl.col("R"))
            .otherwise(pl.col("B"))
            .alias("LeftWinCount"),
            pl.when(pl.col("is_ordered"))
            .then(pl.col("B"))
            .otherwise(pl.col("R"))
            .alias("RightWinCount"),
        )
    )

    ultimate_tran_prob_joined = (
        ultimate_tran_prob.group_by("team_counts_before_kill_ordered")
        .agg(
            pl.col("LeftWinCount").sum().alias("LeftWinCount"),
            pl.col("RightWinCount").sum().alias("RightWinCount"),
        )
        .with_columns(
            (
                pl.col("LeftWinCount")
                / (pl.col("LeftWinCount") + pl.col("RightWinCount"))
            ).alias("pct_win_left")
        )
    )

    return ultimate_tran_prob_joined


def get_win_prob_deltas(df_kills_xl, ultimate_tran_prob_joined):
    print("cols", df_kills_xl.columns)
    kill_deltas = (
        df_kills_xl.select(
            "game_id",
            "Auth",
            "Auth_killed",
            "Name",
            "KillerTeam",
            "KilledTeam",
            pl.col("team_counts_before_kill"),
            pl.col("team_counts_after_kill"),
            pl.col("team_counts_before_kill_ordered"),
            pl.col("team_counts_after_kill_ordered"),
        )
        .join(
            ultimate_tran_prob_joined, on="team_counts_before_kill_ordered", how="left"
        )
        .rename({"pct_win_left": "pct_win_before"})
        .join(
            ultimate_tran_prob_joined,
            left_on="team_counts_after_kill_ordered",
            right_on="team_counts_before_kill_ordered",
            how="left",
        )
        .rename({"pct_win_left": "pct_win_after"})
        .with_columns(
            (pl.col("pct_win_after") - pl.col("pct_win_before")).alias("pct_win_delta"),
        )
        .with_columns(
            # since after and before relate to the winning team, and the killer might be on the losing one
            # we must adjust to make sure killing the other is always positive
            pl.when(pl.col("KillerTeam") != pl.col("KilledTeam"))
            .then(abs(pl.col("pct_win_delta")))
            .otherwise(-1 * abs(pl.col("pct_win_delta")))
            .alias("pct_win_delta")
        )
        .select(
            "game_id",
            "Auth",
            "Auth_killed",
            "Name",
            pl.col("pct_win_after"),
            pl.col("pct_win_before"),
            "pct_win_delta",
        )
    )

    return kill_deltas


def main_calculations(df_games, df_players, df_rounds, df_kills):

    df_kills_xl = merge_kills_players(df_players, df_kills)

    # TODO check that round winner from json matches team with cumsum
    # TODO check that team size is between 0 and 5
    df_kills_xl = df_kills_xl.with_columns(
        pl.when(pl.col("KilledTeam") == "red")
        .then(
            pl.concat_str(
                (pl.col("AliveRed") + 1).cast(pl.Utf8),
                pl.lit("-"),
                (pl.col("AliveBlue")).cast(pl.Utf8),
            )
        )
        .otherwise(
            pl.concat_str(
                (pl.col("AliveRed")).cast(pl.Utf8),
                pl.lit("-"),
                (pl.col("AliveBlue") + 1).cast(pl.Utf8),
            )
        )
        .alias("team_counts_before_kill"),
        pl.concat_str(
            (pl.col("AliveRed")).cast(pl.Utf8),
            pl.lit("-"),
            (pl.col("AliveBlue")).cast(pl.Utf8),
        ).alias("team_counts_after_kill"),
    ).with_columns(
        order_team_counts(pl.col("team_counts_before_kill")).alias(
            "team_counts_before_kill_ordered"
        ),
        order_team_counts(pl.col("team_counts_after_kill")).alias(
            "team_counts_after_kill_ordered"
        ),
    )

    # transition_probs(df_kills_xl)

    # take team size at round end
    df_teams_round_end = get_last_round(df_kills_xl)
    # TODO check that all team sizes are 0 in the last round
    stats = (
        df_teams_round_end.with_columns(
            pl.when(pl.col("AliveRed") == 0)
            .then(pl.col("AliveBlue"))
            .otherwise(pl.col("AliveRed"))
            .alias("WinnerAlive")
        )
        .group_by("WinnerAlive")
        .agg(
            pl.len().alias("count"),
        )
        .with_columns((pl.col("count") / len(df_teams_round_end)).alias("percentage"))
    )

    # ultimate win probabilities
    df_kills_xl = df_kills_xl.join(
        df_rounds.select("game_id", "Round", "Winner"),
        on=["game_id", "Round"],
    )

    # remove kills after round is won
    df_kills_xl = df_kills_xl.filter(
        pl.col("team_counts_before_kill_ordered").str.split("-").list.get(1) != "0"
    )

    ultimate_tran_prob_joined = ultimate_tran_probs(df_kills_xl)
    with pl.Config(tbl_cols=-1, tbl_rows=30):
        print(
            ultimate_tran_prob_joined.filter(
                pl.col("team_counts_before_kill_ordered").str.len_chars() > 2
            ).select(
                pl.col("team_counts_before_kill_ordered").alias("score_normalized"),
                (pl.col("pct_win_left") * 100).round(0).alias("win probability as %"),
            )
        )

    # print all columns and all row

    # print(df_kills_xl.columns)
    # print(
    #     df_kills_xl.filter(
    #         pl.col("team_counts_before_kill_ordered").str.split("-").list.get(0) > "5"
    #     )
    # )
    # with pl.Config(tbl_cols=-1):
    #     print(df_kills_xl.filter(pl.col("game_id") == "14023", pl.col("Round") == 1))

    kill_deltas = get_win_prob_deltas(df_kills_xl, ultimate_tran_prob_joined)

    avg_delta_per_kill = (
        kill_deltas.group_by(
            "Auth",
        )
        .agg(
            pl.len(),
            pl.col("Name").unique(),
            pl.col("pct_win_delta").mean().alias("pct_win_delta_avg"),
        )
        .sort("pct_win_delta_avg", descending=True)
    )

    with pl.Config(tbl_cols=-1, tbl_rows=30):
        limit = len(df_games) * 1.5
        print(
            avg_delta_per_kill.filter(
                (pl.col("len") > limit) | (pl.col("Auth") == "end3r")
            )
        )

    sum_delta_killers = kill_deltas.group_by("Auth", "game_id").agg(
        pl.col("Name").unique(),
        pl.col("pct_win_delta").sum().alias("pct_win_delta_sum_kills"),
    )
    sum_delta_killed = kill_deltas.group_by("Auth_killed", "game_id").agg(
        pl.col("Name").unique(),
        (-1 * pl.col("pct_win_delta").sum()).alias("pct_win_delta_sum_deaths"),
    )
    sum_deltas = sum_delta_killers.join(
        sum_delta_killed,
        left_on=["Auth", "game_id"],
        right_on=["Auth_killed", "game_id"],
        how="full",
    ).with_columns(
        (pl.col("pct_win_delta_sum_kills") + pl.col("pct_win_delta_sum_deaths")).alias(
            "pct_win_delta_sum"
        ),
    )
    avg_sum_delta_per_player = (
        sum_deltas.group_by(
            "Auth",
        )
        .agg(
            pl.len(),
            pl.col("Name"),
            pl.col("pct_win_delta_sum").mean().alias("pct_win_delta_avg_per_game"),
            (
                pl.col("pct_win_delta_sum_kills").sum()
                / (-1 * pl.col("pct_win_delta_sum_deaths").sum())
            ).alias("pct_win_delta_ratio"),
        )
        .with_columns(
            pl.col("Name").list.eval(pl.element().explode().drop_nulls()).list.unique()
        )
        .sort("pct_win_delta_ratio", descending=True)
    )

    with pl.Config(tbl_cols=-1, tbl_rows=50):
        limit = len(df_games) / 20
        print(
            avg_sum_delta_per_player.filter(
                (pl.col("len") > limit) | (pl.col("Auth") == "end3r")
            ).rename({"len": "number of games"})
        )

    with pl.Config(tbl_cols=-1, tbl_rows=50):
        limit = len(df_games) / 20
        print(
            avg_sum_delta_per_player.filter(
                (pl.col("len") > limit) | (pl.col("Auth") == "end3r")
            )
            .rename({"len": "number of games"})
            .select(
                "Auth",
                "number of games",
                "pct_win_delta_ratio",
            )
        )
    # print(ultimate_tran_prob_joined.sort("team_counts_before_kill_ordered"))

    # print(df_kills_xl.group_by(
    #     "team_counts_before_kill",
    #     "Winner"
    # ).agg(
    #     pl.len().alias("count")
    # ).pivot(
    #     index="team_counts_before_kill",
    #     on="Winner",
    #     values="count"
    # ).filter(pl.col("team_counts_before_kill").str.split("-").list.get(0)=="5"))
    print(kill_deltas.columns)
    # with pl.Config(tbl_cols=-1, tbl_rows=50):
    #     print(
    #         kill_deltas.filter(
    #             (pl.col("Auth") == "end3r") | (pl.col("Auth_killed") == "end3r")
    #         )
    #         .group_by("game_id")
    #         .agg(pl.col("pct_win_delta").sum())
    #         .sort(pl.col("game_id").cast(pl.Int16))
    #     )

    #     print(
    #         kill_deltas.filter(pl.col("game_id") == "18980")
    #         .group_by("Auth")
    #         .agg(pl.col("pct_win_delta").sum())
    #         .sort("pct_win_delta", descending=True)
    #     )
    #     print(
    #         df_players.filter(pl.col("game_id") == "18980")
    #         .select(
    #             "Auth",
    #             "Team",
    #             "Kills",
    #             "Deaths",
    #             (pl.col("Kills") / pl.col("Deaths")).alias("KD"),
    #         )
    #         .sort(pl.col("Kills") / pl.col("Deaths"), descending=True)
    #     )

    with pl.Config(tbl_cols=-1, tbl_rows=50):
        print(
            sum_deltas.filter(
                (pl.col("Auth") == "end3r") | (pl.col("Auth_killed") == "end3r")
            )
            .group_by("game_id")
            .agg(pl.col("pct_win_delta_sum").sum())
            .sort(pl.col("game_id").cast(pl.Int16))
        )

        print(
            sum_deltas.filter(pl.col("game_id") == "18980")
            .group_by("Auth")
            .agg(
                pl.col("pct_win_delta_sum").sum().round(2).alias("sum player impact"),
                (
                    pl.col("pct_win_delta_sum_kills").sum()
                    / (-1 * pl.col("pct_win_delta_sum_deaths").sum())
                )
                .round(2)
                .alias("impact ratio"),
            )
            .sort("impact ratio", descending=True)
        )
        print(
            df_players.filter(pl.col("game_id") == "18980")
            .select(
                "Auth",
                "Team",
                "Kills",
                "Deaths",
                (pl.col("Kills") / pl.col("Deaths")).round(2).alias("KD"),
            )
            .sort(pl.col("Kills") / pl.col("Deaths"), descending=True)
        )


if __name__ == "__main__":
    nb_files_to_process = None
    game_type = "TS"
    use_cache = True
    # read file df_players{nb}.parquet if it exists
    if (
        use_cache
        and os.path.exists(f"df_players_{nb_files_to_process}_{game_type}.parquet")
        and os.path.exists(f"df_games_{nb_files_to_process}_{game_type}.parquet")
        and os.path.exists(f"df_rounds_{nb_files_to_process}_{game_type}.parquet")
        and os.path.exists(f"df_kills_{nb_files_to_process}_{game_type}.parquet")
    ):
        print("Reading parquet files")
        df_players = pl.read_parquet(
            f"df_players_{nb_files_to_process}_{game_type}.parquet"
        )
        df_games = pl.read_parquet(
            f"df_games_{nb_files_to_process}_{game_type}.parquet"
        )
        df_rounds = pl.read_parquet(
            f"df_rounds_{nb_files_to_process}_{game_type}.parquet"
        )
        df_kills = pl.read_parquet(
            f"df_kills_{nb_files_to_process}_{game_type}.parquet"
        )
    else:
        print("Reading JSON files")
        df_games, df_players, df_rounds, df_kills = process_jsons(
            "/home/lucio/mypyprojects/urt_impact_score/urt-player-score/my_jsons",
            game_type=game_type,
            limit=nb_files_to_process,
        )
        # save to parquet
        if use_cache:
            df_players.write_parquet(
                f"df_players_{nb_files_to_process}_{game_type}.parquet"
            )
            df_games.write_parquet(
                f"df_games_{nb_files_to_process}_{game_type}.parquet"
            )
            df_rounds.write_parquet(
                f"df_rounds_{nb_files_to_process}_{game_type}.parquet"
            )
            df_kills.write_parquet(
                f"df_kills_{nb_files_to_process}_{game_type}.parquet"
            )

    start_time = time.time()
    print("Starting main calculations")

    main_calculations(df_games, df_players, df_rounds, df_kills)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time:.2f} seconds")
