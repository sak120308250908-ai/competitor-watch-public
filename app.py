import pandas as pd
import plotly.express as px
import streamlit as st
import time
from supabase import create_client

from services import competitor_metrics as competitor_metrics_module
from services.interview_metrics import (
    build_coverage_summary,
    build_coverage_replay_summary,
    build_interview_day_summary,
    build_media_reliability_summary,
    build_media_summary,
    build_new_machine_interview_overlap,
    build_special_overlap_summary,
)
from services.interview_repository import fetch_interview_events
from services.new_machine_competitor import build_store_new_machine_summary, build_tier_summary
from services.reporting import (
    generate_alerts,
    generate_interview_comment,
    generate_metric_guide,
    generate_new_machine_comment,
    generate_new_machine_overlap_comment,
    generate_weekly_report,
    generate_weekly_competitor_comment,
)
from services.store_normalizer import get_store_query_names, normalize_store_series


build_area_summary = getattr(
    competitor_metrics_module,
    "build_area_summary",
    lambda *args, **kwargs: pd.DataFrame(),
)
build_brand_summary = getattr(
    competitor_metrics_module,
    "build_brand_summary",
    lambda *args, **kwargs: pd.DataFrame(),
)
build_machine_candidate_summary = getattr(
    competitor_metrics_module,
    "build_machine_candidate_summary",
    lambda *args, **kwargs: pd.DataFrame(),
)
build_machine_watch_daily = getattr(
    competitor_metrics_module,
    "build_machine_watch_daily",
    lambda *args, **kwargs: pd.DataFrame(),
)
build_multi_machine_store_rankings = getattr(
    competitor_metrics_module,
    "build_multi_machine_store_rankings",
    lambda *args, **kwargs: pd.DataFrame(),
)
build_multi_machine_watch_summary = getattr(
    competitor_metrics_module,
    "build_multi_machine_watch_summary",
    lambda *args, **kwargs: pd.DataFrame(),
)
build_machine_watch_summary = getattr(
    competitor_metrics_module,
    "build_machine_watch_summary",
    lambda *args, **kwargs: pd.DataFrame(),
)
build_machine_watch_weekday = getattr(
    competitor_metrics_module,
    "build_machine_watch_weekday",
    lambda *args, **kwargs: pd.DataFrame(),
)
build_competitor_score = getattr(
    competitor_metrics_module,
    "build_competitor_score",
    lambda *args, **kwargs: pd.DataFrame(),
)
build_specialday_strength_summary = getattr(
    competitor_metrics_module,
    "build_specialday_strength_summary",
    lambda *args, **kwargs: pd.DataFrame(),
)
build_store_competitor_summary = getattr(
    competitor_metrics_module,
    "build_store_competitor_summary",
    lambda *args, **kwargs: pd.DataFrame(),
)
build_weekday_strength_summary = getattr(
    competitor_metrics_module,
    "build_weekday_strength_summary",
    lambda *args, **kwargs: pd.DataFrame(),
)


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


def execute_with_retry(request_builder, retries: int = 3, base_sleep: float = 1.0):
    last_error = None
    for attempt in range(retries):
        try:
            return request_builder.execute()
        except Exception as error:
            last_error = error
            if attempt == retries - 1:
                raise
            time.sleep(base_sleep * (attempt + 1))
    raise last_error


@st.cache_data(ttl=3600, show_spinner="店舗データを取得中...")
def fetch_slot_data(halls: list[str]) -> pd.DataFrame:
    rows = []
    for hall in halls:
        for query_name in get_store_query_names(hall):
            offset = 0
            limit = 1000
            while True:
                request = (
                    supabase.table("slot_data")
                    .select("*")
                    .eq("店舗", query_name)
                    .range(offset, offset + limit - 1)
                )
                result = execute_with_retry(request)
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


def get_default_halls(interview_pool: pd.DataFrame, fallback_halls: list[str]) -> list[str]:
    if interview_pool.empty:
        return fallback_halls[:3]

    work = interview_pool.copy()
    work["event_date"] = pd.to_datetime(work["event_date"])
    recent_cutoff = work["event_date"].max() - pd.Timedelta(days=30)
    recent = work[work["event_date"] >= recent_cutoff]
    ranked = (
        recent.groupby("hall_name")
        .size()
        .sort_values(ascending=False)
        .index.tolist()
    )
    default_halls = [hall for hall in ranked if hall] or fallback_halls[:3]
    return default_halls[:3]


def get_query_halls(hall_candidates: list[str]) -> list[str]:
    raw_value = st.query_params.get("halls", "")
    if not raw_value:
        return []
    requested = [value.strip() for value in str(raw_value).split(",") if value.strip()]
    return [hall for hall in requested if hall in hall_candidates]


def sync_hall_query_params(selected_halls: list[str]) -> None:
    serialized = ",".join(selected_halls)
    if serialized:
        st.query_params["halls"] = serialized
    elif "halls" in st.query_params:
        del st.query_params["halls"]


def download_csv_button(df: pd.DataFrame, label: str, file_prefix: str) -> None:
    if df.empty:
        return
    csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        label=label,
        data=csv_bytes,
        file_name=f"{file_prefix}_{pd.Timestamp.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
        use_container_width=True,
    )


interview_pool = fetch_interview_pool()
hall_candidates = sorted(interview_pool["hall_name"].dropna().unique().tolist()) if not interview_pool.empty else DEFAULT_HALLS
fallback_halls = [hall for hall in DEFAULT_HALLS if hall in hall_candidates] or hall_candidates[:3]
default_halls = get_default_halls(interview_pool, fallback_halls)
query_halls = get_query_halls(hall_candidates)

if "saved_hall_sets" not in st.session_state:
    st.session_state["saved_hall_sets"] = {}
if "selected_halls" not in st.session_state:
    st.session_state["selected_halls"] = query_halls or default_halls

st.sidebar.title("📡 競合店ウォッチ")
plan = st.sidebar.selectbox("表示モード", ["basic", "a", "b"], index=1)
selected_halls = st.sidebar.multiselect("比較店舗", hall_candidates, key="selected_halls")

st.sidebar.markdown("#### 店舗セット")
set_name = st.sidebar.text_input("セット名", placeholder="例: 愛知キャッスル")
save_set = st.sidebar.button("この店舗セットを保存")
saved_set_names = list(st.session_state["saved_hall_sets"].keys())
selected_set_name = st.sidebar.selectbox("保存済みセット", [""] + saved_set_names)
load_set = st.sidebar.button("保存済みセットを読み込む")

if save_set and set_name and selected_halls:
    st.session_state["saved_hall_sets"][set_name] = selected_halls
    st.sidebar.success(f"「{set_name}」を保存しました。")

if load_set and selected_set_name:
    st.session_state["selected_halls"] = st.session_state["saved_hall_sets"][selected_set_name]
    st.rerun()

if interview_pool.empty:
    st.warning("interview_events が空です。先に Notion 同期を実行してください。")
    st.stop()

interview_pool["event_date"] = pd.to_datetime(interview_pool["event_date"])
min_date = interview_pool["event_date"].min().date()
max_date = interview_pool["event_date"].max().date()
default_start_date = max(min_date, max_date - pd.Timedelta(days=30))
date_range = st.sidebar.date_input(
    "期間",
    value=(default_start_date, max_date),
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
sync_hall_query_params(selected_halls)

st.title("📡 競合店ウォッチ")
st.caption("競合比較と取材分析、新台ウォッチをまとめた公開向けダッシュボードです。")
st.info("まずは 1. 比較店舗 2. 期間 3. 指標の見方 を確認すると使いやすいです。初期表示は直近30日です。")
share_url = st.query_params.to_dict()
if share_url.get("halls"):
    st.caption(f"共有用URL: `?halls={share_url['halls']}`")

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
machine_candidate_df = build_machine_candidate_summary(slot_df)
interview_day_df = build_interview_day_summary(slot_df, interview_df)
media_df = build_media_summary(interview_df)
coverage_df = build_coverage_summary(interview_df)
media_reliability_df = build_media_reliability_summary(interview_day_df)
coverage_replay_df = build_coverage_replay_summary(interview_day_df)
hall_area_map = (
    interview_pool[["hall_name", "prefecture"]]
    .dropna()
    .drop_duplicates(subset=["hall_name"])
    .set_index("hall_name")["prefecture"]
    .to_dict()
)
brand_summary_df = build_brand_summary(slot_df)
area_summary_df = build_area_summary(slot_df, hall_area_map)
new_machine_df = build_store_new_machine_summary(slot_df)
new_machine_overlap_df = build_new_machine_interview_overlap(new_machine_df, interview_df)
special_overlap_df = build_special_overlap_summary(new_machine_overlap_df)
tier_summary_df = build_tier_summary(new_machine_df)

machine_candidates = machine_candidate_df["機種名"].tolist() if not machine_candidate_df.empty else []
default_machine = machine_candidates[0] if machine_candidates else None
selected_machine = st.sidebar.selectbox("機種ウォッチ", [""] + machine_candidates, index=1 if default_machine else 0)
default_core_machines = machine_candidates[:5]
selected_core_machines = st.sidebar.multiselect("主力機種まとめ比較", machine_candidates, default=default_core_machines)
machine_summary_df = build_machine_watch_summary(slot_df, selected_machine) if selected_machine else pd.DataFrame()
machine_daily_df = build_machine_watch_daily(slot_df, selected_machine) if selected_machine else pd.DataFrame()
machine_weekday_df = build_machine_watch_weekday(slot_df, selected_machine) if selected_machine else pd.DataFrame()
multi_machine_df = build_multi_machine_watch_summary(slot_df, selected_core_machines)
multi_machine_rank_df = build_multi_machine_store_rankings(multi_machine_df)

col1, col2, col3, col4 = st.columns(4)
col1.metric("比較店舗数", f"{len(selected_halls)}店")
col2.metric("取材レコード数", f"{len(interview_df):,}件")
col3.metric("分析対象台データ", f"{len(slot_df):,}件")
col4.metric("期間", f"{start_date} - {end_date}" if isinstance(date_range, tuple) else str(date_range))

st.markdown("### 今週の要点")
st.write(generate_weekly_competitor_comment(score_df))
st.write(generate_interview_comment(interview_df))
alert_messages = generate_alerts(
    score_df=score_df,
    media_reliability_df=media_reliability_df,
    coverage_replay_df=coverage_replay_df,
    special_overlap_df=special_overlap_df,
)

if alert_messages:
    st.markdown("### 注目アラート")
    alert_cols = st.columns(len(alert_messages))
    for col, message in zip(alert_cols, alert_messages):
        col.info(message)

weekly_report_text = generate_weekly_report(
    score_df=score_df,
    media_reliability_df=media_reliability_df,
    coverage_replay_df=coverage_replay_df,
    new_machine_overlap_df=new_machine_overlap_df,
    special_overlap_df=special_overlap_df,
)

with st.expander("週次レポート文"):
    st.text_area("共有用の要約文", weekly_report_text, height=220)
    st.download_button(
        label="週次レポートをTXT出力",
        data=weekly_report_text.encode("utf-8"),
        file_name=f"weekly_report_{pd.Timestamp.now().strftime('%Y%m%d')}.txt",
        mime="text/plain",
        use_container_width=True,
    )

with st.expander("指標の見方"):
    for title, description in generate_metric_guide():
        st.markdown(f"- **{title}**: {description}")

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
download_csv_button(display_score_df, "店舗比較をCSV出力", "store_comparison")
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

st.markdown("### 法人・エリア比較")
st.caption("法人はホール名からのブランド推定、エリアは取材データの都道府県を使っています。")
group_col1, group_col2 = st.columns(2)
with group_col1:
    if not brand_summary_df.empty:
        brand_view = brand_summary_df.copy()
        brand_view["平均差枚数"] = format_signed_number(brand_view["平均差枚数"])
        brand_view["平均回転数"] = format_plain_number(brand_view["平均回転数"])
        brand_view["勝率"] = format_percent(brand_view["勝率"])
        st.markdown("#### 法人比較")
        download_csv_button(brand_view, "法人比較をCSV出力", "brand_summary")
        st.dataframe(
            style_signed_columns(brand_view, ["平均差枚数"]),
            use_container_width=True,
            hide_index=True,
        )
with group_col2:
    if not area_summary_df.empty:
        area_view = area_summary_df.copy()
        area_view["平均差枚数"] = format_signed_number(area_view["平均差枚数"])
        area_view["平均回転数"] = format_plain_number(area_view["平均回転数"])
        area_view["勝率"] = format_percent(area_view["勝率"])
        st.markdown("#### エリア比較")
        download_csv_button(area_view, "エリア比較をCSV出力", "area_summary")
        st.dataframe(
            style_signed_columns(area_view, ["平均差枚数"]),
            use_container_width=True,
            hide_index=True,
        )

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
        download_csv_button(view_df, "取材日サマリーをCSV出力", "interview_day_summary")
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
                平均回転数=("平均回転数", "mean"),
                平均勝率=("勝率", "mean"),
            )
            .reset_index()
        )
        overlap_stats["取材重複"] = overlap_stats["取材重複"].map({True: "あり", False: "なし"})
        overlap_stats["平均差枚数"] = format_signed_number(overlap_stats["平均差枚数"])
        overlap_stats["平均回転数"] = format_plain_number(overlap_stats["平均回転数"])
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
            download_csv_button(overlap_stats, "取材重複比較をCSV出力", "new_machine_overlap_summary")
            st.dataframe(style_signed_columns(overlap_stats, ["平均差枚数"]), use_container_width=True, hide_index=True)
        with top_col:
            if not top_overlap_df.empty:
                top_overlap_view = top_overlap_df[
                    ["導入/初稼働日", "hall_name", "機種名", "台数", "平均差枚数", "平均回転数", "勝率", "media_name", "coverage_name"]
                ].rename(
                    columns={
                        "hall_name": "店名",
                        "media_name": "媒体",
                        "coverage_name": "取材名",
                    }
                )
                top_overlap_view["平均差枚数"] = format_signed_number(top_overlap_view["平均差枚数"])
                top_overlap_view["平均回転数"] = format_plain_number(top_overlap_view["平均回転数"])
                top_overlap_view["勝率"] = top_overlap_view["勝率"].fillna(0).round(1).astype(str) + "%"
                st.markdown("#### 取材重複の注目新台")
                download_csv_button(top_overlap_view, "注目新台をCSV出力", "top_new_machine_overlap")
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
        download_csv_button(nm_view, "新台ウォッチをCSV出力", "new_machine_watch")
        st.dataframe(style_signed_columns(nm_view, ["平均差枚数"]), use_container_width=True, hide_index=True)

        if not tier_summary_df.empty:
            tier_view = tier_summary_df.copy()
            if "平均差枚数" in tier_view.columns:
                tier_view["平均差枚数"] = format_signed_number(tier_view["平均差枚数"])
            if "平均回転数" in tier_view.columns:
                tier_view["平均回転数"] = format_plain_number(tier_view["平均回転数"])
            tier_view["勝率"] = tier_view["勝率"].fillna(0).round(1).astype(str) + "%"
            st.markdown("#### 導入台数別サマリー")
            download_csv_button(tier_view, "導入台数別サマリーをCSV出力", "new_machine_tier_summary")
            st.dataframe(style_signed_columns(tier_view, ["平均差枚数"]), use_container_width=True, hide_index=True)

        if not special_overlap_df.empty:
            special_overlap_view = special_overlap_df.copy()
            special_overlap_view["平均差枚数"] = format_signed_number(special_overlap_view["平均差枚数"])
            special_overlap_view["平均回転数"] = format_plain_number(special_overlap_view["平均回転数"])
            special_overlap_view["平均勝率"] = format_percent(special_overlap_view["平均勝率"] / 100.0)
            st.markdown("#### 特日 × 取材 × 新台")
            st.caption("新台初日が、通常日・取材日・特日・取材×特日に重なった時の差を見ます。")
            download_csv_button(special_overlap_view, "重なり分析をCSV出力", "special_overlap_summary")
            st.dataframe(
                style_signed_columns(special_overlap_view, ["平均差枚数"]),
                use_container_width=True,
                hide_index=True,
            )

        if not new_machine_overlap_df.empty:
            combo_view = new_machine_overlap_df[
                [
                    "導入/初稼働日",
                    "hall_name",
                    "機種名",
                    "台数",
                    "平均差枚数",
                    "平均回転数",
                    "勝率",
                    "重複パターン",
                    "media_name",
                    "coverage_name",
                ]
            ].copy()
            combo_view = combo_view.rename(
                columns={
                    "hall_name": "店名",
                    "media_name": "媒体",
                    "coverage_name": "取材名",
                }
            )
            combo_view["平均差枚数"] = format_signed_number(combo_view["平均差枚数"])
            combo_view["平均回転数"] = format_plain_number(combo_view["平均回転数"])
            combo_view["勝率"] = combo_view["勝率"].fillna(0).round(1).astype(str) + "%"
            st.markdown("#### 重なり別の新台一覧")
            download_csv_button(combo_view, "重なり別新台一覧をCSV出力", "special_overlap_details")
            st.dataframe(
                style_signed_columns(combo_view.sort_values("導入/初稼働日", ascending=False), ["平均差枚数"]),
                use_container_width=True,
                hide_index=True,
            )
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
    download_csv_button(detail_df, "取材ウォッチをCSV出力", "interview_watch")
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
                    "avg_games": "平均回転数",
                }
            ).copy()
            if "平均総差枚" in media_view.columns:
                media_view["平均総差枚"] = format_signed_number(media_view["平均総差枚"])
            if "平均台差枚" in media_view.columns:
                media_view["平均台差枚"] = format_signed_number(media_view["平均台差枚"])
            if "平均回転数" in media_view.columns:
                media_view["平均回転数"] = format_plain_number(media_view["平均回転数"])
            st.markdown("#### 媒体別サマリー")
            download_csv_button(media_view, "媒体別サマリーをCSV出力", "media_summary")
            st.dataframe(style_signed_columns(media_view, ["平均総差枚", "平均台差枚"]), use_container_width=True, hide_index=True)
    with sub2:
        if not coverage_df.empty:
            coverage_view = coverage_df.rename(
                columns={
                    "coverage_name": "取材名",
                    "events": "件数",
                    "avg_total_diff": "平均総差枚",
                    "avg_games": "平均回転数",
                }
            ).copy()
            if "平均総差枚" in coverage_view.columns:
                coverage_view["平均総差枚"] = format_signed_number(coverage_view["平均総差枚"])
            if "平均回転数" in coverage_view.columns:
                coverage_view["平均回転数"] = format_plain_number(coverage_view["平均回転数"])
            st.markdown("#### 取材名別サマリー")
            download_csv_button(coverage_view, "取材名別サマリーをCSV出力", "coverage_summary")
            st.dataframe(style_signed_columns(coverage_view, ["平均総差枚"]), use_container_width=True, hide_index=True)

    st.markdown("### 取材の強さを見る指標")
    guide_col1, guide_col2 = st.columns(2)
    with guide_col1:
        st.markdown("#### 媒体信頼度")
        st.caption("平均差枚・勝率・回転数を合成した媒体ごとの目安です。高いほど結果が伴いやすい媒体です。")
        if not media_reliability_df.empty:
            media_reliability_view = media_reliability_df.rename(
                columns={
                    "media_name": "媒体",
                    "events": "件数",
                    "avg_diff": "平均差枚数",
                    "avg_games": "平均回転数",
                    "avg_win_rate": "平均勝率",
                    "positive_rate": "プラス率",
                    "reliability_score": "媒体信頼度",
                }
            ).copy()
            media_reliability_view["平均差枚数"] = format_signed_number(media_reliability_view["平均差枚数"])
            media_reliability_view["平均回転数"] = format_plain_number(media_reliability_view["平均回転数"])
            media_reliability_view["平均勝率"] = format_percent(media_reliability_view["平均勝率"])
            media_reliability_view["プラス率"] = format_percent(media_reliability_view["プラス率"])
            media_reliability_view["媒体信頼度"] = pd.to_numeric(media_reliability_view["媒体信頼度"], errors="coerce").round(1)
            download_csv_button(media_reliability_view, "媒体信頼度をCSV出力", "media_reliability")
            st.dataframe(
                style_signed_columns(media_reliability_view, ["平均差枚数"]),
                use_container_width=True,
                hide_index=True,
            )
    with guide_col2:
        st.markdown("#### 取材名別再現率")
        st.caption("その取材名が入った日にプラス結果になった割合です。高いほど再現しやすい企画です。")
        if not coverage_replay_df.empty:
            coverage_replay_view = coverage_replay_df.rename(
                columns={
                    "coverage_name": "取材名",
                    "events": "件数",
                    "avg_diff": "平均差枚数",
                    "avg_games": "平均回転数",
                    "avg_win_rate": "平均勝率",
                    "positive_rate": "再現率",
                }
            ).copy()
            coverage_replay_view["平均差枚数"] = format_signed_number(coverage_replay_view["平均差枚数"])
            coverage_replay_view["平均回転数"] = format_plain_number(coverage_replay_view["平均回転数"])
            coverage_replay_view["平均勝率"] = format_percent(coverage_replay_view["平均勝率"])
            coverage_replay_view["再現率"] = format_percent(coverage_replay_view["再現率"])
            download_csv_button(coverage_replay_view, "取材名別再現率をCSV出力", "coverage_replay")
            st.dataframe(
                style_signed_columns(coverage_replay_view, ["平均差枚数"]),
                use_container_width=True,
                hide_index=True,
            )

st.markdown("### 機種ウォッチ")
st.caption("主力機種を1つ選ぶと、店舗横比較と日別推移を見られます。")
if not multi_machine_rank_df.empty:
    st.markdown("#### 主力機種まとめ比較")
    st.caption("複数の主力機種をまとめて比較します。『この機種ならこの店』の見え方を作るための表です。")
    multi_machine_view = multi_machine_rank_df.copy()
    multi_machine_view["平均差枚数"] = format_signed_number(multi_machine_view["平均差枚数"])
    multi_machine_view["平均回転数"] = format_plain_number(multi_machine_view["平均回転数"])
    multi_machine_view["勝率"] = format_percent(multi_machine_view["勝率"])
    multi_machine_view["順位"] = pd.to_numeric(multi_machine_view["順位"], errors="coerce").fillna(0).astype(int)
    download_csv_button(multi_machine_view, "主力機種まとめ比較をCSV出力", "multi_machine_watch")
    st.dataframe(
        style_signed_columns(multi_machine_view, ["平均差枚数"]),
        use_container_width=True,
        hide_index=True,
    )

    diff_pivot_df = multi_machine_df.pivot_table(index="機種名", columns="店名", values="平均差枚数", aggfunc="mean")
    if not diff_pivot_df.empty:
        diff_pivot_view = diff_pivot_df.reset_index().copy()
        for column in diff_pivot_view.columns[1:]:
            diff_pivot_view[column] = format_signed_number(diff_pivot_view[column])
        st.markdown("#### 主力機種 × 店舗 差枚マトリクス")
        st.dataframe(
            style_signed_columns(diff_pivot_view, list(diff_pivot_view.columns[1:])),
            use_container_width=True,
            hide_index=True,
        )

    games_pivot_df = multi_machine_df.pivot_table(index="機種名", columns="店名", values="平均回転数", aggfunc="mean")
    if not games_pivot_df.empty:
        games_pivot_view = games_pivot_df.reset_index().copy()
        for column in games_pivot_view.columns[1:]:
            games_pivot_view[column] = format_plain_number(games_pivot_view[column])
        st.markdown("#### 主力機種 × 店舗 平均回転数マトリクス")
        st.dataframe(
            games_pivot_view,
            use_container_width=True,
            hide_index=True,
        )

if selected_machine and not machine_summary_df.empty:
    machine_summary_view = machine_summary_df.copy()
    machine_summary_view["平均差枚数"] = format_signed_number(machine_summary_view["平均差枚数"])
    machine_summary_view["平均回転数"] = format_plain_number(machine_summary_view["平均回転数"])
    machine_summary_view["勝率"] = format_percent(machine_summary_view["勝率"])
    download_csv_button(machine_summary_view, "機種ウォッチをCSV出力", "machine_watch_summary")
    st.dataframe(
        style_signed_columns(machine_summary_view, ["平均差枚数"]),
        use_container_width=True,
        hide_index=True,
    )

    if not machine_daily_df.empty:
        machine_daily_chart = machine_daily_df.copy()
        fig_machine = px.line(
            machine_daily_chart,
            x="日付",
            y="平均差枚数",
            color="店名",
            markers=True,
            title=f"{selected_machine} の日別推移",
        )
        st.plotly_chart(fig_machine, use_container_width=True)

        machine_daily_view = machine_daily_df.copy()
        machine_daily_view["平均差枚数"] = format_signed_number(machine_daily_view["平均差枚数"])
        machine_daily_view["平均回転数"] = format_plain_number(machine_daily_view["平均回転数"])
        machine_daily_view["勝率"] = format_percent(machine_daily_view["勝率"])
        download_csv_button(machine_daily_view, "機種の日別推移をCSV出力", "machine_watch_daily")
        st.dataframe(
            style_signed_columns(machine_daily_view, ["平均差枚数"]),
            use_container_width=True,
            hide_index=True,
        )

    if not machine_weekday_df.empty:
        st.markdown("#### 機種 × 曜日傾向")
        weekday_chart_df = machine_weekday_df.copy()
        fig_machine_weekday = px.line(
            weekday_chart_df,
            x="Weekday",
            y="平均差枚数",
            color="店名",
            markers=True,
            title=f"{selected_machine} の曜日別傾向",
        )
        st.plotly_chart(fig_machine_weekday, use_container_width=True)

        machine_weekday_view = machine_weekday_df.copy()
        machine_weekday_view["平均差枚数"] = format_signed_number(machine_weekday_view["平均差枚数"])
        machine_weekday_view["平均回転数"] = format_plain_number(machine_weekday_view["平均回転数"])
        machine_weekday_view["勝率"] = format_percent(machine_weekday_view["勝率"])
        download_csv_button(machine_weekday_view, "機種の曜日傾向をCSV出力", "machine_watch_weekday")
        st.dataframe(
            style_signed_columns(machine_weekday_view, ["平均差枚数"]),
            use_container_width=True,
            hide_index=True,
        )
else:
    st.info("比較したい機種をサイドバーの「機種ウォッチ」から選択してください。")

if plan == "b":
    st.markdown("### 特日傾向")
    st.dataframe(specialday_df, use_container_width=True, hide_index=True)
    st.info("Bプラン想定: 今後はCSV/PDF出力、通知、週次レポートをここへ追加します。")
