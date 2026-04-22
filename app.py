import pandas as pd
import plotly.express as px
import streamlit as st
from supabase import create_client

from services.competitor_metrics import (
    build_competitor_score,
    build_specialday_strength_summary,
    build_store_competitor_summary,
    build_weekday_strength_summary,
)
from services.interview_metrics import (
    build_coverage_summary,
    build_interview_day_summary,
    build_media_summary,
    build_new_machine_interview_overlap,
)
from services.interview_repository import fetch_interview_events
from services.new_machine_competitor import build_store_new_machine_summary, build_tier_summary
from services.reporting import (
    generate_interview_comment,
    generate_new_machine_comment,
    generate_new_machine_overlap_comment,
    generate_weekly_competitor_comment,
)
from services.store_normalizer import get_store_query_names, normalize_store_series


st.set_page_config(page_title="競合店ウォッチ", page_icon="📡", layout="wide")


DEFAULT_HALLS = [
    "プレイランドキャッスル知多東海店",
    "プレイランドキャッスル上社店",
    "プレイランドキャッスル記念橋南店",
]


@st.cache_resource
def init_connection():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])


supabase = init_connection()


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_interview_pool() -> pd.DataFrame:
    return fetch_interview_events(supabase_client=supabase)


@st.cache_data(ttl=3600, show_spinner="店舗データを取得中...")
def fetch_slot_data(halls: list[str]) -> pd.DataFrame:
    rows = []
    for hall in halls:
        for query_name in get_store_query_names(hall):
            offset = 0
            limit = 1000
            while True:
                result = (
                    supabase.table("slot_data")
                    .select("*")
                    .eq("店舗", query_name)
                    .range(offset, offset + limit - 1)
                    .execute()
                )
                data = result.data or []
                if not data:
                    break
                rows.extend(data)
                if len(data) < limit:
                    break
                offset += limit

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df = df.drop_duplicates()

    df["日付"] = pd.to_datetime(df["日付"])
    df["差枚"] = pd.to_numeric(df["差枚"], errors="coerce").fillna(0)
    df["G数"] = pd.to_numeric(df["G数"], errors="coerce").fillna(0)
    df["Weekday"] = df["日付"].dt.day_name()
    df["Day"] = df["日付"].dt.day
    df["hall_name"] = normalize_store_series(df["店舗"])
    return df


def format_percent(series: pd.Series) -> pd.Series:
    return (series.fillna(0) * 100).round(1).astype(str) + "%"


def format_signed_number(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce").fillna(0).round().astype(int)
    return numeric.map(lambda value: f"{value:+,}")


def format_plain_number(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce").fillna(0).round().astype(int)
    return numeric.map(lambda value: f"{value:,}")


def signed_text_color(value) -> str:
    text = str(value).strip()
    if text.startswith("+"):
        return "color: #128a43; font-weight: 600;"
    if text.startswith("-"):
        return "color: #c62828; font-weight: 600;"
    return ""


def style_signed_columns(df: pd.DataFrame, columns: list[str]):
    target_columns = [column for column in columns if column in df.columns]
    if not target_columns:
        return df
    return df.style.map(signed_text_color, subset=target_columns)


interview_pool = fetch_interview_pool()
hall_candidates = sorted(interview_pool["hall_name"].dropna().unique().tolist()) if not interview_pool.empty else DEFAULT_HALLS
default_halls = [hall for hall in DEFAULT_HALLS if hall in hall_candidates] or hall_candidates[:3]

st.sidebar.title("📡 競合店ウォッチ")
plan = st.sidebar.selectbox("表示モード", ["basic", "a", "b"], index=1)
selected_halls = st.sidebar.multiselect("比較店舗", hall_candidates, default=default_halls)

if interview_pool.empty:
    st.warning("interview_events が空です。先に Notion 同期を実行してください。")
    st.stop()

interview_pool["event_date"] = pd.to_datetime(interview_pool["event_date"])
min_date = interview_pool["event_date"].min().date()
max_date = interview_pool["event_date"].max().date()
date_range = st.sidebar.date_input(
    "期間",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

selected_media = st.sidebar.multiselect(
    "媒体フィルタ",
    sorted(interview_pool["media_name"].dropna().unique().tolist()),
)
selected_coverage = st.sidebar.multiselect(
    "取材名フィルタ",
    sorted(interview_pool["coverage_name"].dropna().unique().tolist()),
)

st.title("📡 競合店ウォッチ")
st.caption("競合比較と取材分析、新台ウォッチをまとめた公開向けダッシュボードです。")

if not selected_halls:
    st.warning("比較する店舗を1つ以上選択してください。")
    st.stop()

slot_df = fetch_slot_data(selected_halls)
interview_df = interview_pool[interview_pool["hall_name"].isin(selected_halls)].copy()

if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
    interview_df = interview_df[
        (interview_df["event_date"].dt.date >= start_date)
        & (interview_df["event_date"].dt.date <= end_date)
    ]
    if not slot_df.empty:
        slot_df = slot_df[
            (slot_df["日付"].dt.date >= start_date)
            & (slot_df["日付"].dt.date <= end_date)
        ]

if selected_media:
    interview_df = interview_df[interview_df["media_name"].isin(selected_media)]
if selected_coverage:
    interview_df = interview_df[interview_df["coverage_name"].isin(selected_coverage)]

if slot_df.empty:
    st.warning("対象店舗の slot_data が見つかりませんでした。")
    st.stop()

store_summary = build_store_competitor_summary(slot_df)
score_df = build_competitor_score(store_summary)
weekday_df = build_weekday_strength_summary(slot_df)
specialday_df = build_specialday_strength_summary(slot_df)
interview_day_df = build_interview_day_summary(slot_df, interview_df)
media_df = build_media_summary(interview_df)
coverage_df = build_coverage_summary(interview_df)
new_machine_df = build_store_new_machine_summary(slot_df)
new_machine_overlap_df = build_new_machine_interview_overlap(new_machine_df, interview_df)
tier_summary_df = build_tier_summary(new_machine_df)

col1, col2, col3, col4 = st.columns(4)
col1.metric("比較店舗数", f"{len(selected_halls)}店")
col2.metric("取材レコード数", f"{len(interview_df):,}件")
col3.metric("分析対象台データ", f"{len(slot_df):,}件")
col4.metric("期間", f"{start_date} - {end_date}" if isinstance(date_range, tuple) else str(date_range))

st.markdown("### 今週の要点")
st.write(generate_weekly_competitor_comment(score_df))
st.write(generate_interview_comment(interview_df))

st.markdown("### 店舗比較")
display_score_df = score_df.rename(
    columns={
        "店舗": "店名",
        "avg_diff": "平均差枚数",
        "avg_games": "平均回転数",
        "win_rate": "勝率",
        "records": "件数",
        "competitor_score": "競合スコア",
    }
).copy()
if "勝率" in display_score_df.columns:
    display_score_df["勝率"] = format_percent(display_score_df["勝率"])
if "平均差枚数" in display_score_df.columns:
    display_score_df["平均差枚数"] = format_signed_number(display_score_df["平均差枚数"])
if "平均回転数" in display_score_df.columns:
    display_score_df["平均回転数"] = format_plain_number(display_score_df["平均回転数"])
if "競合スコア" in display_score_df.columns:
    display_score_df["競合スコア"] = display_score_df["競合スコア"].round(1)
st.dataframe(style_signed_columns(display_score_df, ["平均差枚数"]), use_container_width=True, hide_index=True)
fig = px.bar(
    score_df,
    x="店舗",
    y="avg_diff",
    color="avg_diff",
    title="平均差枚数の比較",
    color_continuous_scale="RdYlGn",
)
st.plotly_chart(fig, use_container_width=True)

weekday_col, interview_col = st.columns(2)
with weekday_col:
    st.markdown("### 曜日傾向")
    if not weekday_df.empty:
        fig_week = px.line(weekday_df, x="Weekday", y="avg_diff", color="店舗", markers=True)
        st.plotly_chart(fig_week, use_container_width=True)

with interview_col:
    st.markdown("### 取材日サマリー")
    if not interview_day_df.empty:
        summary_cols = [c for c in ["event_date", "hall_name", "media_name", "coverage_name", "avg_diff", "avg_games", "win_rate"] if c in interview_day_df.columns]
        view_df = interview_day_df[summary_cols].copy()
        if "win_rate" in view_df.columns:
            view_df["win_rate"] = format_percent(view_df["win_rate"])
        if "avg_diff" in view_df.columns:
            view_df["avg_diff"] = format_signed_number(view_df["avg_diff"])
        if "avg_games" in view_df.columns:
            view_df["avg_games"] = format_plain_number(view_df["avg_games"])
        view_df = view_df.rename(
            columns={
                "event_date": "日付",
                "hall_name": "店名",
                "media_name": "媒体",
                "coverage_name": "取材名",
                "avg_diff": "平均差枚数",
                "avg_games": "平均回転数",
                "win_rate": "勝率",
            }
        )
        view_df = view_df.sort_values("日付", ascending=False)
        st.dataframe(style_signed_columns(view_df, ["平均差枚数"]), use_container_width=True, hide_index=True)
    else:
        st.info("この条件では取材日データがありません。")

if plan in {"a", "b"}:
    st.markdown("### 新台ウォッチ")
    if not new_machine_overlap_df.empty:
        st.write(generate_new_machine_comment(new_machine_df))
        st.write(generate_new_machine_overlap_comment(new_machine_overlap_df))

        overlap_stats = (
            new_machine_overlap_df.assign(
                平均差枚数=pd.to_numeric(new_machine_overlap_df["平均差枚数"], errors="coerce").fillna(0),
                勝率=pd.to_numeric(new_machine_overlap_df["勝率"], errors="coerce").fillna(0),
                取材重複=new_machine_overlap_df["取材重複"].fillna(False),
            )
            .groupby("取材重複")
            .agg(
                機種数=("機種名", "count"),
                平均差枚数=("平均差枚数", "mean"),
                平均勝率=("勝率", "mean"),
            )
            .reset_index()
        )
        overlap_stats["取材重複"] = overlap_stats["取材重複"].map({True: "あり", False: "なし"})
        overlap_stats["平均差枚数"] = format_signed_number(overlap_stats["平均差枚数"])
        overlap_stats["平均勝率"] = overlap_stats["平均勝率"].round(1).astype(str) + "%"

        top_overlap_df = (
            new_machine_overlap_df[new_machine_overlap_df["取材重複"] == True]
            .sort_values("平均差枚数", ascending=False)
            .head(10)
            .copy()
        )

        summary_col, top_col = st.columns([1, 2])
        with summary_col:
            st.markdown("#### 取材重複比較")
            st.dataframe(style_signed_columns(overlap_stats, ["平均差枚数"]), use_container_width=True, hide_index=True)
        with top_col:
            if not top_overlap_df.empty:
                top_overlap_view = top_overlap_df[
                    ["導入/初稼働日", "hall_name", "機種名", "台数", "平均差枚数", "勝率", "media_name", "coverage_name"]
                ].rename(
                    columns={
                        "hall_name": "店名",
                        "media_name": "媒体",
                        "coverage_name": "取材名",
                    }
                )
                top_overlap_view["平均差枚数"] = format_signed_number(top_overlap_view["平均差枚数"])
                top_overlap_view["勝率"] = top_overlap_view["勝率"].fillna(0).round(1).astype(str) + "%"
                st.markdown("#### 取材重複の注目新台")
                st.dataframe(style_signed_columns(top_overlap_view, ["平均差枚数"]), use_container_width=True, hide_index=True)

        nm_view = new_machine_overlap_df[
            [
                "導入/初稼働日",
                "hall_name",
                "機種名",
                "台数",
                "平均差枚数",
                "平均回転数",
                "勝率",
                "Tier",
                "取材重複",
                "media_name",
                "coverage_name",
            ]
        ].copy()
        nm_view = nm_view.rename(
            columns={
                "hall_name": "店名",
                "Tier": "導入規模",
                "media_name": "媒体",
                "coverage_name": "取材名",
            }
        )
        nm_view["平均差枚数"] = format_signed_number(nm_view["平均差枚数"])
        nm_view["平均回転数"] = format_plain_number(nm_view["平均回転数"])
        nm_view["勝率"] = nm_view["勝率"].fillna(0).round(1).astype(str) + "%"
        nm_view["取材重複"] = nm_view["取材重複"].map({True: "あり", False: "なし"})
        nm_view = nm_view.sort_values("導入/初稼働日", ascending=False)
        st.dataframe(style_signed_columns(nm_view, ["平均差枚数"]), use_container_width=True, hide_index=True)

        if not tier_summary_df.empty:
            tier_view = tier_summary_df.copy()
            if "平均差枚数" in tier_view.columns:
                tier_view["平均差枚数"] = format_signed_number(tier_view["平均差枚数"])
            if "平均回転数" in tier_view.columns:
                tier_view["平均回転数"] = format_plain_number(tier_view["平均回転数"])
            tier_view["勝率"] = tier_view["勝率"].fillna(0).round(1).astype(str) + "%"
            st.markdown("#### 導入台数別サマリー")
            st.dataframe(style_signed_columns(tier_view, ["平均差枚数"]), use_container_width=True, hide_index=True)
    else:
        st.info("この条件では新台データがありません。")

    st.markdown("### 取材ウォッチ")
    detail_cols = [
        c
        for c in [
            "event_date",
            "hall_name",
            "media_name",
            "coverage_name",
            "category_name",
            "total_diff",
            "avg_diff_per_unit",
            "games",
            "rating",
            "machine_name",
        ]
        if c in interview_df.columns
    ]
    detail_df = interview_df[detail_cols].copy().rename(
        columns={
            "event_date": "日付",
            "hall_name": "店名",
            "media_name": "媒体",
            "coverage_name": "取材名",
            "category_name": "カテゴリ",
            "total_diff": "総差枚",
            "avg_diff_per_unit": "台平均差枚",
            "games": "回転数",
            "rating": "評価",
            "machine_name": "機種名",
        }
    )
    if "総差枚" in detail_df.columns:
        detail_df["総差枚"] = format_signed_number(detail_df["総差枚"])
    if "台平均差枚" in detail_df.columns:
        detail_df["台平均差枚"] = format_signed_number(detail_df["台平均差枚"])
    if "回転数" in detail_df.columns:
        detail_df["回転数"] = format_plain_number(detail_df["回転数"])
    detail_df = detail_df.sort_values("日付", ascending=False)
    st.dataframe(style_signed_columns(detail_df, ["総差枚", "台平均差枚"]), use_container_width=True, hide_index=True)

    sub1, sub2 = st.columns(2)
    with sub1:
        if not media_df.empty:
            media_view = media_df.rename(
                columns={
                    "media_name": "媒体",
                    "events": "件数",
                    "avg_total_diff": "平均総差枚",
                    "avg_diff_per_unit": "平均台差枚",
                }
            ).copy()
            if "平均総差枚" in media_view.columns:
                media_view["平均総差枚"] = format_signed_number(media_view["平均総差枚"])
            if "平均台差枚" in media_view.columns:
                media_view["平均台差枚"] = format_signed_number(media_view["平均台差枚"])
            st.markdown("#### 媒体別サマリー")
            st.dataframe(style_signed_columns(media_view, ["平均総差枚", "平均台差枚"]), use_container_width=True, hide_index=True)
    with sub2:
        if not coverage_df.empty:
            coverage_view = coverage_df.rename(
                columns={
                    "coverage_name": "取材名",
                    "events": "件数",
                    "avg_total_diff": "平均総差枚",
                }
            ).copy()
            if "平均総差枚" in coverage_view.columns:
                coverage_view["平均総差枚"] = format_signed_number(coverage_view["平均総差枚"])
            st.markdown("#### 取材名別サマリー")
            st.dataframe(style_signed_columns(coverage_view, ["平均総差枚"]), use_container_width=True, hide_index=True)

if plan == "b":
    st.markdown("### 特日傾向")
    st.dataframe(specialday_df, use_container_width=True, hide_index=True)
    st.info("Bプラン想定: 今後はCSV/PDF出力、通知、週次レポートをここへ追加します。")
