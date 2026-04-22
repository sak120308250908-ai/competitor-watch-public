from __future__ import annotations

import unicodedata

import pandas as pd


BRAND_PATTERNS = [
    ("プレイランドキャッスル", "プレイランドキャッスル"),
    ("キャッスル", "キャッスル"),
    ("MEGAスロットコンコルド", "コンコルド"),
    ("MEGAコンコルド", "コンコルド"),
    ("コンコルド", "コンコルド"),
    ("マルハン", "マルハン"),
    ("ラッキープラザ", "ラッキープラザ"),
    ("ラッキー1番", "ラッキー1番"),
    ("キング観光サウザンド", "キング観光"),
    ("キング観光", "キング観光"),
    ("キング666", "キング666"),
    ("ホームラン", "ホームラン"),
    ("ZENT", "ZENT"),
    ("ウイング", "ウイング"),
    ("ミカド", "ミカド"),
    ("タイホー", "タイホー"),
    ("KYORAKU", "KYORAKU"),
    ("ZEAL", "ZEAL"),
    ("フジコー", "フジコー"),
    ("プレイランドサンワ", "プレイランドサンワ"),
    ("プレイランド平和", "プレイランド平和"),
    ("プレイランド第一平和", "プレイランド平和"),
    ("ABC", "ABC"),
    ("楽園", "楽園"),
    ("がちゃぽん", "がちゃぽん"),
    ("キクヤ", "キクヤ"),
]


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


def infer_brand_name(hall_name: str) -> str:
    normalized = unicodedata.normalize("NFKC", str(hall_name)).strip()
    for prefix, brand in BRAND_PATTERNS:
        if normalized.startswith(prefix):
            return brand
    return normalized.replace("店", "")[:12] or "不明"


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


def build_multi_machine_watch_summary(store_df: pd.DataFrame, machine_names: list[str]) -> pd.DataFrame:
    if store_df.empty or not machine_names:
        return pd.DataFrame()

    machine_keys = {_normalize_machine_name(name) for name in machine_names if name}
    df = store_df.copy()
    df["機種名"] = df["機種名"].fillna("不明")
    df["_machine_key"] = df["機種名"].apply(_normalize_machine_name)
    df = df[df["_machine_key"].isin(machine_keys)].copy()
    if df.empty:
        return pd.DataFrame()

    df["差枚"] = pd.to_numeric(df["差枚"], errors="coerce").fillna(0)
    df["G数"] = pd.to_numeric(df["G数"], errors="coerce").fillna(0)
    df["Win"] = (df["差枚"] > 0).astype(int)
    hall_col = "hall_name" if "hall_name" in df.columns else "店舗"

    summary = (
        df.groupby([hall_col, "機種名"])
        .agg(
            平均差枚数=("差枚", "mean"),
            平均回転数=("G数", "mean"),
            勝率=("Win", "mean"),
            台データ件数=("差枚", "count"),
        )
        .reset_index()
        .rename(columns={hall_col: "店名"})
    )
    return summary.sort_values(["機種名", "平均差枚数"], ascending=[True, False])


def build_multi_machine_store_rankings(multi_machine_df: pd.DataFrame) -> pd.DataFrame:
    if multi_machine_df.empty:
        return pd.DataFrame()

    rankings = multi_machine_df.copy()
    rankings["順位"] = rankings.groupby("機種名")["平均差枚数"].rank(method="dense", ascending=False)
    return rankings.sort_values(["機種名", "順位", "平均差枚数"], ascending=[True, True, False])


def build_brand_summary(store_df: pd.DataFrame) -> pd.DataFrame:
    if store_df.empty:
        return pd.DataFrame()

    df = store_df.copy()
    df["差枚"] = pd.to_numeric(df["差枚"], errors="coerce").fillna(0)
    df["G数"] = pd.to_numeric(df["G数"], errors="coerce").fillna(0)
    df["Win"] = (df["差枚"] > 0).astype(int)
    hall_col = "hall_name" if "hall_name" in df.columns else "店舗"
    df["ブランド"] = df[hall_col].apply(infer_brand_name)

    return (
        df.groupby("ブランド")
        .agg(
            平均差枚数=("差枚", "mean"),
            平均回転数=("G数", "mean"),
            勝率=("Win", "mean"),
            台データ件数=("差枚", "count"),
            店舗数=(hall_col, "nunique"),
        )
        .reset_index()
        .sort_values("平均差枚数", ascending=False)
    )


def build_area_summary(store_df: pd.DataFrame, hall_area_map: dict[str, str]) -> pd.DataFrame:
    if store_df.empty or not hall_area_map:
        return pd.DataFrame()

    df = store_df.copy()
    df["差枚"] = pd.to_numeric(df["差枚"], errors="coerce").fillna(0)
    df["G数"] = pd.to_numeric(df["G数"], errors="coerce").fillna(0)
    df["Win"] = (df["差枚"] > 0).astype(int)
    hall_col = "hall_name" if "hall_name" in df.columns else "店舗"
    df["エリア"] = df[hall_col].map(hall_area_map).fillna("不明")

    return (
        df.groupby("エリア")
        .agg(
            平均差枚数=("差枚", "mean"),
            平均回転数=("G数", "mean"),
            勝率=("Win", "mean"),
            台データ件数=("差枚", "count"),
            店舗数=(hall_col, "nunique"),
        )
        .reset_index()
        .sort_values("平均差枚数", ascending=False)
    )
