from __future__ import annotations

import unicodedata

import pandas as pd


def _normalize_machine_name(name: str) -> str:
    text = unicodedata.normalize("NFKC", str(name))
    text = text.replace("\u301c", "〜").replace("\uff5e", "〜")
    return " ".join(text.split()).strip()


def get_tier_label(count: int) -> str:
    if count == 1:
        return "1台機種"
    if 2 <= count <= 4:
        return "2-4台機種"
    if 5 <= count <= 9:
        return "5-9台機種"
    if 10 <= count <= 19:
        return "10-19台機種"
    return "20台以上機種"


def build_store_new_machine_summary(store_df: pd.DataFrame) -> pd.DataFrame:
    if store_df.empty:
        return pd.DataFrame()

    df = store_df.copy()
    df["日付"] = pd.to_datetime(df["日付"])
    df["機種名"] = df["機種名"].fillna("不明")
    df["G数"] = pd.to_numeric(df["G数"], errors="coerce").fillna(0)
    df["差枚"] = pd.to_numeric(df["差枚"], errors="coerce").fillna(0)
    df["Win"] = (df["差枚"] > 0).astype(int)
    df["_機種名_norm"] = df["機種名"].apply(_normalize_machine_name)
    hall_col = "hall_name" if "hall_name" in df.columns else "店舗"

    results = []
    for hall_name, hall_df in df.groupby(hall_col):
        min_date = hall_df["日付"].min()
        first_appearance = hall_df.groupby("_機種名_norm")["日付"].min().reset_index()
        first_appearance = first_appearance.rename(columns={"_機種名_norm": "machine_key"})
        new_machines = first_appearance[
            (first_appearance["日付"] > min_date + pd.Timedelta(days=1))
            & (first_appearance["machine_key"] != "不明")
        ]["machine_key"].tolist()

        for machine_key in new_machines:
            m_df = hall_df[hall_df["_機種名_norm"] == machine_key].sort_values("日付")
            active_m_df = m_df[m_df["G数"] > 0]
            if active_m_df.empty:
                continue
            first_active_date = active_m_df["日付"].iloc[0]
            target_df = m_df[m_df["日付"] == first_active_date]
            if target_df.empty:
                continue
            display_name = target_df["機種名"].mode().iloc[0] if target_df["機種名"].notna().any() else machine_key
            unit_count = int(len(target_df))
            avg_games = int(round(target_df["G数"].mean()))
            avg_diff = int(round(target_df["差枚"].mean()))
            win_rate = round(target_df["Win"].mean() * 100, 1)
            results.append(
                {
                    "hall_name": hall_name,
                    "導入/初稼働日": first_active_date.date(),
                    "機種名": display_name,
                    "台数": unit_count,
                    "平均回転数": avg_games,
                    "平均差枚数": avg_diff,
                    "勝率": win_rate,
                    "Tier": get_tier_label(unit_count),
                }
            )

    if not results:
        return pd.DataFrame()
    return pd.DataFrame(results).sort_values(["導入/初稼働日", "hall_name"], ascending=[False, True])


def build_tier_summary(new_machine_df: pd.DataFrame) -> pd.DataFrame:
    if new_machine_df.empty:
        return pd.DataFrame()

    df = new_machine_df.copy()
    df["台数"] = pd.to_numeric(df["台数"], errors="coerce").fillna(0)
    df["平均差枚数"] = pd.to_numeric(df["平均差枚数"], errors="coerce").fillna(0)
    df["平均回転数"] = pd.to_numeric(df["平均回転数"], errors="coerce").fillna(0)
    df["勝率"] = pd.to_numeric(df["勝率"], errors="coerce").fillna(0)
    df["総差枚"] = df["台数"] * df["平均差枚数"]

    tier_order = ["1台機種", "2-4台機種", "5-9台機種", "10-19台機種", "20台以上機種"]
    rows = []
    for tier in tier_order:
        t_df = df[df["Tier"] == tier]
        if t_df.empty:
            rows.append({"導入規模": tier, "総集計台数": 0, "平均回転数": 0, "総差枚": 0, "平均差枚数": 0, "勝率": 0.0})
            continue
        total_units = int(t_df["台数"].sum())
        total_diff = int(round(t_df["総差枚"].sum()))
        avg_games = int(round((t_df["台数"] * t_df["平均回転数"]).sum() / total_units)) if total_units else 0
        avg_diff = int(round(total_diff / total_units)) if total_units else 0
        win_rate = round((t_df["勝率"] * t_df["台数"]).sum() / total_units, 1) if total_units else 0.0
        rows.append(
            {
                "導入規模": tier,
                "総集計台数": total_units,
                "平均回転数": avg_games,
                "総差枚": total_diff,
                "平均差枚数": avg_diff,
                "勝率": win_rate,
            }
        )
    return pd.DataFrame(rows)
