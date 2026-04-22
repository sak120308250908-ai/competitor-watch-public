from __future__ import annotations

import pandas as pd


def build_store_competitor_summary(store_df: pd.DataFrame) -> pd.DataFrame:
    if store_df.empty:
        return pd.DataFrame()

    df = store_df.copy()
    df["差枚"] = pd.to_numeric(df["差枚"], errors="coerce").fillna(0)
    df["G数"] = pd.to_numeric(df["G数"], errors="coerce").fillna(0)
    df["Win"] = (df["差枚"] > 0).astype(int)
    hall_col = "hall_name" if "hall_name" in df.columns else "店舗"

    return (
        df.groupby(hall_col)
        .agg(
            avg_diff=("差枚", "mean"),
            avg_games=("G数", "mean"),
            win_rate=("Win", "mean"),
            records=("差枚", "count"),
        )
        .reset_index()
        .rename(columns={hall_col: "店舗"})
        .sort_values("avg_diff", ascending=False)
    )


def build_weekday_strength_summary(store_df: pd.DataFrame) -> pd.DataFrame:
    if store_df.empty:
        return pd.DataFrame()

    df = store_df.copy()
    df["差枚"] = pd.to_numeric(df["差枚"], errors="coerce").fillna(0)
    df["Win"] = (df["差枚"] > 0).astype(int)
    hall_col = "hall_name" if "hall_name" in df.columns else "店舗"

    return (
        df.groupby([hall_col, "Weekday"])
        .agg(avg_diff=("差枚", "mean"), win_rate=("Win", "mean"))
        .reset_index()
        .rename(columns={hall_col: "店舗"})
    )


def build_specialday_strength_summary(store_df: pd.DataFrame) -> pd.DataFrame:
    if store_df.empty or "Day" not in store_df.columns:
        return pd.DataFrame()

    df = store_df.copy()
    df["差枚"] = pd.to_numeric(df["差枚"], errors="coerce").fillna(0)
    df["Win"] = (df["差枚"] > 0).astype(int)
    hall_col = "hall_name" if "hall_name" in df.columns else "店舗"

    return (
        df.groupby([hall_col, "Day"])
        .agg(avg_diff=("差枚", "mean"), win_rate=("Win", "mean"))
        .reset_index()
        .rename(columns={hall_col: "店舗"})
    )


def build_competitor_score(summary_df: pd.DataFrame) -> pd.DataFrame:
    if summary_df.empty:
        return pd.DataFrame()

    df = summary_df.copy()
    df["competitor_score"] = (
        df["avg_diff"].fillna(0) * 0.5
        + df["avg_games"].fillna(0) * 0.0001
        + df["win_rate"].fillna(0) * 1000
    )
    return df.sort_values("competitor_score", ascending=False)
