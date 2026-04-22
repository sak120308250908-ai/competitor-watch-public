import pandas as pd


def generate_weekly_competitor_comment(df: pd.DataFrame) -> str:
    if df.empty:
        return "今週の競合比較データはまだありません。"
    top = df.iloc[0]
    return f"今週は {top['店舗']} が平均差枚で先行しています。"


def generate_interview_comment(df: pd.DataFrame) -> str:
    if df.empty:
        return "取材実績データはまだありません。"
    if "media_name" in df.columns and df["media_name"].notna().any():
        top_media = df["media_name"].mode().iloc[0]
        return f"直近では {top_media} の取材出現が目立っています。"
    return "直近の取材実績は確認できています。"


def generate_new_machine_comment(df: pd.DataFrame) -> str:
    if df.empty:
        return "新台データはまだありません。"
    top = df.sort_values("平均差枚数", ascending=False).iloc[0]
    return f"新台では {top['機種名']} が最も強い初日成績でした。"


def generate_new_machine_overlap_comment(df: pd.DataFrame) -> str:
    if df.empty or "取材重複" not in df.columns:
        return "新台と取材の重複データはまだありません。"

    work = df.copy()
    work["平均差枚数"] = pd.to_numeric(work["平均差枚数"], errors="coerce").fillna(0)
    work["勝率"] = pd.to_numeric(work["勝率"], errors="coerce").fillna(0)
    overlap_df = work[work["取材重複"] == True]
    non_overlap_df = work[work["取材重複"] != True]

    if overlap_df.empty:
        return "今回の条件では、取材と重なった新台は確認できませんでした。"

    overlap_avg = overlap_df["平均差枚数"].mean()
    overlap_win = overlap_df["勝率"].mean()
    if non_overlap_df.empty:
        top = overlap_df.sort_values("平均差枚数", ascending=False).iloc[0]
        return f"取材重複の新台では {top['機種名']} が最も強く、平均差枚は {int(round(top['平均差枚数'])):,} 枚でした。"

    non_overlap_avg = non_overlap_df["平均差枚数"].mean()
    diff = overlap_avg - non_overlap_avg
    if diff >= 0:
        return f"取材重複ありの新台は、重複なしより平均差枚で {int(round(diff)):,} 枚上振れしています。 重複あり平均勝率は {overlap_win:.1f}% です。"
    return f"今回の条件では、取材重複ありの新台は重複なしより平均差枚で {int(round(abs(diff))):,} 枚弱めでした。 ただし重複あり平均勝率は {overlap_win:.1f}% です。"


def generate_metric_guide() -> list[tuple[str, str]]:
    return [
        ("平均差枚数", "1台あたりの平均差枚です。プラスほど出玉が強く、マイナスほど弱いと見ます。"),
        ("勝率", "差枚がプラスだった台の割合です。高いほど多くの台が勝っていた日です。"),
        ("平均回転数", "どれだけ回されたかの指標です。高いほど稼働が付き、注目度が高い見方です。"),
        ("競合スコア", "平均差枚・勝率・回転数をまとめた比較用スコアです。店舗同士の相対比較に使います。"),
        ("媒体信頼度", "媒体ごとの平均差枚・勝率・回転数を合成した目安です。高いほど結果が伴いやすい媒体です。"),
        ("取材名別再現率", "その取材名が入った日にプラス結果になった比率です。高いほど再現しやすい企画です。"),
        ("取材重複", "新台初日と取材日が重なっていたかどうかです。重複ありは仕掛けの有無を見る材料です。"),
        ("導入規模", "1台、2-4台、5-9台、10-19台、20台以上に分けた導入台数帯です。店の力の入れ方を見ます。"),
    ]
