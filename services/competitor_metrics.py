from __future__ import annotations

import unicodedata

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


def _normalize_machine_name(name: str) -> str:
    text = unicodedata.normalize("NFKC", str(name))
    return " ".join(text.split()).strip()


def build_machine_candidate_summary(store_df: pd.DataFrame) -> pd.DataFrame:
    if store_df.empty or "機種名" not in store_df.columns:
        return pd.DataFrame()

    df = store_df.copy()
    df["機種名"] = df["機種名"].fillna("不明")
    df["_machine_key"] = df["機種名"].apply(_normalize_machine_name)
    df["G数"] = pd.to_numeric(df["G数"], errors="coerce").fillna(0)

    return (
        df.groupby("_machine_key")
        .agg(
            機種名=("機種名", lambda x: x.mode().iloc[0] if not x.mode().empty else x.iloc[0]),
            総回転数=("G数", "sum"),
            台データ件数=("機種名", "count"),
        )
        .reset_index(drop=True)
        .sort_values(["総回転数", "台データ件数"], ascending=[False, False])
    )


def build_machine_watch_summary(store_df: pd.DataFrame, machine_name: str) -> pd.DataFrame:
    if store_df.empty or not machine_name:
        return pd.DataFrame()

    target_key = _normalize_machine_name(machine_name)
    df = store_df.copy()
    df["機種名"] = df["機種名"].fillna("不明")
    df["_machine_key"] = df["機種名"].apply(_normalize_machine_name)
    df = df[df["_machine_key"] == target_key].copy()
    if df.empty:
        return pd.DataFrame()

    df["差枚"] = pd.to_numeric(df["差枚"], errors="coerce").fillna(0)
    df["G数"] = pd.to_numeric(df["G数"], errors="coerce").fillna(0)
    df["Win"] = (df["差枚"] > 0).astype(int)
    hall_col = "hall_name" if "hall_name" in df.columns else "店舗"

    return (
        df.groupby(hall_col)
        .agg(
            平均差枚数=("差枚", "mean"),
            平均回転数=("G数", "mean"),
            勝率=("Win", "mean"),
            台データ件数=("差枚", "count"),
        )
        .reset_index()
        .rename(columns={hall_col: "店名"})
        .sort_values("平均差枚数", ascending=False)
    )


def build_machine_watch_daily(store_df: pd.DataFrame, machine_name: str) -> pd.DataFrame:
    if store_df.empty or not machine_name:
        return pd.DataFrame()

    target_key = _normalize_machine_name(machine_name)
    df = store_df.copy()
    df["機種名"] = df["機種名"].fillna("不明")
    df["_machine_key"] = df["機種名"].apply(_normalize_machine_name)
    df = df[df["_machine_key"] == target_key].copy()
    if df.empty:
        return pd.DataFrame()

    df["差枚"] = pd.to_numeric(df["差枚"], errors="coerce").fillna(0)
    df["G数"] = pd.to_numeric(df["G数"], errors="coerce").fillna(0)
    df["Win"] = (df["差枚"] > 0).astype(int)
    df["日付"] = pd.to_datetime(df["日付"]).dt.date
    hall_col = "hall_name" if "hall_name" in df.columns else "店舗"

    return (
        df.groupby([hall_col, "日付"])
        .agg(
            平均差枚数=("差枚", "mean"),
            平均回転数=("G数", "mean"),
            勝率=("Win", "mean"),
        )
        .reset_index()
        .rename(columns={hall_col: "店名"})
        .sort_values(["日付", "店名"], ascending=[False, True])
    )


def build_machine_watch_weekday(store_df: pd.DataFrame, machine_name: str) -> pd.DataFrame:
    if store_df.empty or not machine_name:
        return pd.DataFrame()

    target_key = _normalize_machine_name(machine_name)
    df = store_df.copy()
    df["機種名"] = df["機種名"].fillna("不明")
    df["_machine_key"] = df["機種名"].apply(_normalize_machine_name)
    df = df[df["_machine_key"] == target_key].copy()
    if df.empty:
        return pd.DataFrame()

    df["差枚"] = pd.to_numeric(df["差枚"], errors="coerce").fillna(0)
    df["G数"] = pd.to_numeric(df["G数"], errors="coerce").fillna(0)
    df["Win"] = (df["差枚"] > 0).astype(int)
    if "Weekday" not in df.columns:
        df["Weekday"] = pd.to_datetime(df["日付"]).dt.day_name()
    hall_col = "hall_name" if "hall_name" in df.columns else "店舗"

    return (
        df.groupby([hall_col, "Weekday"])
        .agg(
            平均差枚数=("差枚", "mean"),
            平均回転数=("G数", "mean"),
            勝率=("Win", "mean"),
        )
        .reset_index()
        .rename(columns={hall_col: "店名"})
    )
