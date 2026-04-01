"""PCA Life Insurance — Real-Time Policy Sales Dashboard."""

import os
from datetime import timedelta

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

# --- Page config ---
st.set_page_config(
    page_title="PCA Sales Dashboard",
    page_icon="📊",
    layout="wide",
)

# --- Database connection ---
def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "pca_sales"),
        user=os.getenv("POSTGRES_USER", "pca"),
        password=os.getenv("POSTGRES_PASSWORD", "pca_secret"),
    )


def run_query(sql: str) -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql(sql, conn)
    finally:
        conn.close()


# --- Header ---
st.title("PCA Life Insurance — Sales Dashboard")
st.caption("Real-time policy sales performance powered by Kafka + dbt + PostgreSQL")

# --- Auto-refresh ---
st.sidebar.markdown("Auto-refresh every 60 seconds")
st.sidebar.markdown("---")
st.sidebar.markdown("**Data pipeline**")
st.sidebar.markdown("Producer → Kafka → Consumer → PostgreSQL → dbt → Dashboard")

@st.fragment(run_every=timedelta(seconds=10))
def live_dashboard():
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    st.caption(f"Last refreshed: {now}")

    # --- KPI Section ---
    st.header("Today's performance")

    kpi_sql = """
        select
            coalesce(sum(premium_amount), 0) as total_premium,
            coalesce(count(*), 0) as total_policies,
            coalesce(avg(premium_amount), 0)::integer as avg_premium,
            count(distinct agent_id) as active_agents
        from staging.stg_policy_sales
        where date_id = current_date;
    """
    kpi = run_query(kpi_sql)

    if not kpi.empty:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Premium (TWD)", f"{int(kpi['total_premium'].iloc[0]):,}")
        col2.metric("Policies Sold", int(kpi['total_policies'].iloc[0]))
        col3.metric("Avg Premium (TWD)", f"{int(kpi['avg_premium'].iloc[0]):,}")
        col4.metric("Active Agents", int(kpi['active_agents'].iloc[0]))


    # # --- Today vs Yesterday ---
    # st.header("Today vs yesterday")

    # compare_sql = """
    #     with today as (
    #         select
    #             count(*) as policies,
    #             coalesce(sum(premium_amount), 0) as premium
    #         from staging.stg_policy_sales
    #         where date_id = current_date
    #     ),
    #     yesterday as (
    #         select
    #             count(*) as policies,
    #             coalesce(sum(premium_amount), 0) as premium
    #         from staging.stg_policy_sales
    #         where date_id = current_date - 1
    #     )
    #     select
    #         today.policies as today_policies,
    #         today.premium as today_premium,
    #         yesterday.policies as yday_policies,
    #         yesterday.premium as yday_premium
    #     from today, yesterday;
    # """
    # comp = run_query(compare_sql)

    # if not comp.empty:
    #     row = comp.iloc[0]
    #     col1, col2 = st.columns(2)

    #     yday_policies = int(row['yday_policies'])
    #     today_policies = int(row['today_policies'])
    #     policy_delta = today_policies - yday_policies if yday_policies > 0 else None
    #     col1.metric("Policies sold", today_policies, delta=policy_delta)

    #     yday_premium = int(row['yday_premium'])
    #     today_premium = int(row['today_premium'])
    #     premium_delta = today_premium - yday_premium if yday_premium > 0 else None
    #     col2.metric("Premium written (TWD)", f"{today_premium:,}", delta=f"{premium_delta:,}" if premium_delta else None)


    # # --- Charts row ---
    chart_col1, chart_col2 = st.columns(2)

    # Hourly Sales Trend
    with chart_col1:
        st.subheader("Hourly Sales Trend (today)")
        hourly_sql = """
            select
                hour_ts,
                sum(total_premium) as total_premium,
                sum(policy_count) as policy_count
            from marts.agg_hourly_sales_by_region
            where hour_ts::date = current_date
            group by hour_ts
            order by hour_ts;
        """
        hourly = run_query(hourly_sql)
        if not hourly.empty:
            fig = px.bar(
                hourly,
                x="hour_ts",
                y="total_premium",
                labels={"hour_ts": "Hour", "total_premium": "Premium (TWD)"},
            )
            fig.update_layout(
                showlegend=False,
                height=350,
                xaxis_tickformat="%H:%M",
                xaxis_range=[
                    hourly['hour_ts'].iloc[0].normalize(),           # today 00:00
                    hourly['hour_ts'].iloc[0].normalize() + pd.Timedelta(days=1),  # tomorrow 00:00
                ],
                xaxis_dtick=3600000,  # one tick per hour
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Waiting for hourly data...")

    # Sales By Product Type
    with chart_col2:
        st.subheader("Sales By Product Type (today)")
        product_sql = """
            select
                product_type,
                sum(policy_count) as policy_count,
                sum(total_premium) as total_premium
            from marts.agg_daily_sales_by_product_type
            where date_id = current_date
            group by product_type
            order by total_premium desc;
        """
        mix = run_query(product_sql)
        if not mix.empty:
            fig = px.pie(
                mix,
                values="total_premium",
                names="product_type",
                hole=0.4,
            )
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Waiting for product data...")


    # --- Agent Leaderboard ---
    st.header("Agent Leaderboard (today)")

    leaderboard_sql = """
        select
            sa.agent_id,
            a.agent_name,
            a.team,
            a.branch,
            sa.daily_premium,
            sa.daily_count,
            rank() over (order by sa.daily_premium desc) as rank
        from marts.agg_daily_sales_by_agent sa
        inner join marts.dim_agents a on sa.agent_id = a.agent_id
        where sa.date_id = current_date
        order by rank;
    """
    leaderboard = run_query(leaderboard_sql)
    if not leaderboard.empty:
        leaderboard['daily_premium'] = leaderboard['daily_premium'].apply(lambda x: f"{int(x):,}")
        st.dataframe(
            leaderboard[['rank', 'agent_name', 'team', 'branch', 'daily_count', 'daily_premium']],
            column_config={
                "rank": st.column_config.NumberColumn("Rank", width="small"),
                "agent_name": "Agent",
                "team": "Team",
                "branch": "Branch",
                "daily_count": st.column_config.NumberColumn("Policies", width="small"),
                "daily_premium": "Premium (TWD)",
            },
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.info("Waiting for agent data...")


    # --- Regional breakdown ---
    st.header("Sales by region (today)")

    region_sql = """
        select
            r.region_name,
            sum(h.total_premium) as total_premium,
            sum(h.policy_count) as policy_count
        from marts.agg_hourly_sales_by_region h
        inner join marts.dim_regions r on h.region_id = r.region_id
        where h.hour_ts::date = current_date
        group by r.region_name
        order by total_premium desc;
    """
    region = run_query(region_sql)
    if not region.empty:
        fig = px.bar(
            region,
            x="region_name",
            y="total_premium",
            color="region_name",
            text="total_premium",           # changed from "policy_count"
            labels={"region_name": "Region", "total_premium": "Premium (TWD)"},
        )
        fig.update_layout(showlegend=False, height=350)
        fig.update_traces(texttemplate="%{text:,.0f}", textposition="inside")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Waiting for regional data...")

# --- Run the fragment ---
live_dashboard()
