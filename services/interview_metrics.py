from __future__ import annotations

import pandas as pd


def build_interview_day_summary(slot_df: pd.DataFrame, interview_df: pd.DataFrame) -> pd.DataFrame:
    if slot_df.empty or interview_df.empty:
        return pd.DataFrame()

    base = slot_df.copy()
    base["日付"] = pd.to_datetime(base["日付"]).dt.date
    base["差枚"] = pd.to_numeric(base["差枚"], errors="coerce").fillna(0)
    base["G数"] = pd.to_numeric(base["G数"], errors="coerce").fillna(0)
    base["Win"] = (base["差枚"] > 0).astype(int)
    hall_col = "hall_name" if "hall_name" in base.columns else "店舗"

    day_summary = (
        base.groupby([hall_col, "日付"])
        .agg(
            avg_diff=("差枚", "mean"),
            avg_games=("G数", "mean"),
            win_rate=("Win", "mean"),
            records=("差枚", "count"),
        )
        .reset_index()
        .rename(columns={hall_col: "hall_name", "日付": "event_date"})
    )

    interviews = interview_df.copy()
    interviews["event_date"] = pd.to_datetime(interviews["event_date"]).dt.date

    merged = interviews.merge(day_summary, on=["hall_name", "event_date"], how="left")
    return merged.sort_values(["event_date", "hall_name"], ascending=[False, True])


def build_media_summary(interview_df: pd.DataFrame) -> pd.DataFrame:
    if interview_df.empty or "media_name" not in interview_df.columns:
        return pd.DataFrame()

    df = interview_df.copy()
    df["total_diff"] = pd.to_numeric(df.get("total_diff"), errors="coerce")
    df["avg_diff_per_unit"] = pd.to_numeric(df.get("avg_diff_per_unit"), errors="coerce")

    return (
        df.groupby("media_name")
        .agg(
            events=("media_name", "count"),
            avg_total_diff=("total_diff", "mean"),
            avg_diff_per_unit=("avg_diff_per_unit", "mean"),
        )
        .reset_index()
        .sort_values("events", ascending=False)
    )


def build_coverage_summary(interview_df: pd.DataFrame) -> pd.DataFrame:
    if interview_df.empty or "coverage_name" not in interview_df.columns:
        return pd.DataFrame()

    df = interview_df.copy()
    df["total_diff"] = pd.to_numeric(df.get("total_diff"), errors="coerce")

    return (
        df.groupby("coverage_name")
        .agg(events=("coverage_name", "count"), avg_total_diff=("total_diff", "mean"))
        .reset_index()
        .sort_values("events", ascending=False)
    )


def build_media_reliability_summary(interview_day_df: pd.DataFrame) -> pd.DataFrame:
    if interview_day_df.empty or "media_name" not in interview_day_df.columns:
        return pd.DataFrame()

    df = interview_day_df.copy()
    df = df[df["media_name"].notna()].copy()
    if df.empty:
        return pd.DataFrame()

    df["avg_diff"] = pd.to_numeric(df.get("avg_diff"), errors="coerce").fillna(0)
    df["avg_games"] = pd.to_numeric(df.get("avg_games"), errors="coerce").fillna(0)
    df["win_rate"] = pd.to_numeric(df.get("win_rate"), errors="coerce").fillna(0)
    df["is_positive_day"] = (df["avg_diff"] > 0).astype(int)
    df["reliability_score"] = (df["avg_diff"] / 100.0) + (df["win_rate"] * 100.0) + (df["avg_games"] / 1000.0)

    return (
        df.groupby("media_name")
        .agg(
            events=("media_name", "count"),
            avg_diff=("avg_diff", "mean"),
            avg_games=("avg_games", "mean"),
            avg_win_rate=("win_rate", "mean"),
            positive_rate=("is_positive_day", "mean"),
            reliability_score=("reliability_score", "mean"),
        )
        .reset_index()
        .sort_values(["reliability_score", "events"], ascending=[False, False])
    )


def build_coverage_replay_summary(interview_day_df: pd.DataFrame) -> pd.DataFrame:
    if interview_day_df.empty or "coverage_name" not in interview_day_df.columns:
        return pd.DataFrame()

    df = interview_day_df.copy()
    df = df[df["coverage_name"].notna()].copy()
    if df.empty:
        return pd.DataFrame()

    df["avg_diff"] = pd.to_numeric(df.get("avg_diff"), errors="coerce").fillna(0)
    df["avg_games"] = pd.to_numeric(df.get("avg_games"), errors="coerce").fillna(0)
    df["win_rate"] = pd.to_numeric(df.get("win_rate"), errors="coerce").fillna(0)
    df["is_positive_day"] = (df["avg_diff"] > 0).astype(int)

    return (
        df.groupby("coverage_name")
        .agg(
            events=("coverage_name", "count"),
            avg_diff=("avg_diff", "mean"),
            avg_games=("avg_games", "mean"),
            avg_win_rate=("win_rate", "mean"),
            positive_rate=("is_positive_day", "mean"),
        )
        .reset_index()
        .sort_values(["positive_rate", "avg_diff", "events"], ascending=[False, False, False])
    )


def build_new_machine_interview_overlap(new_machine_df: pd.DataFrame, interview_df: pd.DataFrame) -> pd.DataFrame:
    if new_machine_df.empty or interview_df.empty:
        return pd.DataFrame()

    left = new_machine_df.copy()
    left["導入/初稼働日"] = pd.to_datetime(left["導入/初稼働日"]).dt.date

    right = interview_df.copy()
    right["event_date"] = pd.to_datetime(right["event_date"]).dt.date

    merged = left.merge(
        right[
            [
                "hall_name",
                "event_date",
                "media_name",
                "coverage_name",
                "category_name",
                "is_special_day",
                "is_circle_day",
            ]
        ],
        left_on=["hall_name", "導入/初稼働日"],
        right_on=["hall_name", "event_date"],
        how="left",
    )
    merged["取材重複"] = merged["event_date"].notna()
    merged["特日重複"] = merged["is_special_day"].fillna(False).astype(bool)
    merged["○の日重複"] = merged["is_circle_day"].fillna(False).astype(bool)
    merged["重複パターン"] = "通常日"
    merged.loc[merged["取材重複"], "重複パターン"] = "取材重複"
    merged.loc[merged["特日重複"], "重複パターン"] = "特日重複"
    merged.loc[merged["取材重複"] & merged["特日重複"], "重複パターン"] = "取材×特日"
    return merged


def build_special_overlap_summary(new_machine_overlap_df: pd.DataFrame) -> pd.DataFrame:
    if new_machine_overlap_df.empty:
        return pd.DataFrame()

    df = new_machine_overlap_df.copy()
    df["平均差枚数"] = pd.to_numeric(df["平均差枚数"], errors="coerce").fillna(0)
    df["平均回転数"] = pd.to_numeric(df["平均回転数"], errors="coerce").fillna(0)
    df["勝率"] = pd.to_numeric(df["勝率"], errors="coerce").fillna(0)

    return (
        df.groupby("重複パターン")
        .agg(
            機種数=("機種名", "count"),
            平均差枚数=("平均差枚数", "mean"),
            平均回転数=("平均回転数", "mean"),
            平均勝率=("勝率", "mean"),
        )
        .reset_index()
        .sort_values("平均差枚数", ascending=False)
    )
