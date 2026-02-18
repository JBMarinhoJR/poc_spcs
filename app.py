import os
import platform
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
import snowflake.connector
import streamlit as st

st.set_page_config(
    page_title="SPCS Streamlit Experience",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    .block-container { padding-top: 1.5rem; }
    .hero {
        background: linear-gradient(120deg, #0f172a, #1d4ed8);
        color: #ffffff;
        border-radius: 16px;
        padding: 20px 24px;
        margin-bottom: 1rem;
    }
    .hero h1 { margin: 0; font-size: 2rem; }
    .hero p { margin: 0.3rem 0 0 0; opacity: 0.9; }
    </style>
    """,
    unsafe_allow_html=True,
)


def env_or_default(name: str, default: str) -> str:
    value = os.getenv(name, default)
    return value if value else default


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


APP_CONFIG = {
    "account": env_or_default("SNOWFLAKE_ACCOUNT", "MLWWZGB-YR87884"),
    "default_warehouse": env_or_default("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "default_database": env_or_default("SNOWFLAKE_DB", "POC_SPCS_DB"),
    "default_schema": env_or_default("SNOWFLAKE_SCHEMA", "POC_SPCS_SCHEMA"),
    "orders_table": env_or_default(
        "APP_ORDERS_TABLE", "POC_SPCS_DB.POC_SPCS_SCHEMA.APP_DEMO_ORDERS"
    ),
    "service_database": env_or_default("SPCS_SERVICE_DB", "POC_SPCS_DB"),
    "service_schema": env_or_default("SPCS_SERVICE_SCHEMA", "POC_SPCS_SCHEMA"),
    "service_name": env_or_default("SPCS_SERVICE_NAME", "POC_STREAMLIT_SERVICE"),
    "allow_password_login": env_bool("ALLOW_PASSWORD_LOGIN", False),
}

SERVICE_TOKEN_PATH = Path("/snowflake/session/token")


def get_state(name: str, default):
    if name not in st.session_state:
        st.session_state[name] = default
    return st.session_state[name]


def run_df(conn, sql: str) -> pd.DataFrame:
    cur = conn.cursor()
    try:
        cur.execute(sql)
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)
    finally:
        cur.close()


def get_request_header(header_name: str) -> str:
    try:
        headers = st.context.headers
    except Exception:
        return ""

    for k, v in headers.items():
        if str(k).lower() == header_name.lower():
            return str(v)
    return ""


def read_service_oauth_token() -> str:
    if not SERVICE_TOKEN_PATH.exists():
        return ""
    return SERVICE_TOKEN_PATH.read_text(encoding="utf-8").strip()


def connect_with_password(user: str, password: str, role: str, warehouse: str):
    return snowflake.connector.connect(
        account=APP_CONFIG["account"],
        user=user,
        password=password,
        role=role,
        warehouse=warehouse,
        database=APP_CONFIG["default_database"],
        schema=APP_CONFIG["default_schema"],
    )


def connect_with_oauth_token(oauth_token: str):
    host = os.getenv("SNOWFLAKE_HOST", "").strip()
    if not host:
        raise RuntimeError("SNOWFLAKE_HOST is not set in this container.")

    return snowflake.connector.connect(
        account=APP_CONFIG["account"],
        host=host,
        authenticator="oauth",
        token=oauth_token,
        warehouse=APP_CONFIG["default_warehouse"],
        database=APP_CONFIG["default_database"],
        schema=APP_CONFIG["default_schema"],
    )


def connect_inside_spcs():
    service_token = read_service_oauth_token()
    if not service_token:
        raise RuntimeError("Service token not found at /snowflake/session/token.")

    caller_token = get_request_header("Sf-Context-Current-User-Token")
    errors = []

    if caller_token:
        try:
            conn = connect_with_oauth_token(f"{service_token}.{caller_token}")
            return conn, "caller"
        except Exception as exc:
            errors.append(f"caller context failed: {exc}")

    try:
        conn = connect_with_oauth_token(service_token)
        return conn, "service"
    except Exception as exc:
        errors.append(f"service token failed: {exc}")

    raise RuntimeError(" | ".join(errors))


def build_session_df(conn) -> pd.DataFrame:
    return run_df(
        conn,
        "SELECT CURRENT_USER() AS USER_NAME, CURRENT_ROLE() AS ROLE_NAME, "
        "CURRENT_DATABASE() AS DB_NAME, CURRENT_SCHEMA() AS SCHEMA_NAME, "
        "CURRENT_TIMESTAMP() AS LOGIN_AT",
    )


def fetch_market_snapshot() -> dict:
    url = (
        "https://api.coingecko.com/api/v3/simple/price"
        "?ids=bitcoin,ethereum,solana"
        "&vs_currencies=usd"
        "&include_24hr_change=true"
    )
    try:
        response = requests.get(url, timeout=8)
        response.raise_for_status()
        data = response.json()
        return {
            "ok": True,
            "btc_usd": data["bitcoin"]["usd"],
            "btc_change": data["bitcoin"].get("usd_24h_change", 0.0),
            "eth_usd": data["ethereum"]["usd"],
            "eth_change": data["ethereum"].get("usd_24h_change", 0.0),
            "sol_usd": data["solana"]["usd"],
            "sol_change": data["solana"].get("usd_24h_change", 0.0),
            "source": "CoinGecko API",
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def show_header():
    st.markdown(
        """
        <div class="hero">
          <h1>SPCS Streamlit Experience</h1>
          <p>Snowflake caller context, row-level security, and service observability in one UI</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def show_sidebar():
    st.sidebar.title("SPCS Control")
    st.sidebar.caption("Snowpark Container Services demo app")
    st.sidebar.write(f"Account: `{APP_CONFIG['account']}`")
    st.sidebar.write(
        f"DB/Schema: `{APP_CONFIG['default_database']}.{APP_CONFIG['default_schema']}`"
    )
    st.sidebar.write(f"Service: `{APP_CONFIG['service_name']}`")
    mode = st.session_state.get("auth_mode", "not connected")
    st.sidebar.write(f"Auth mode: `{mode}`")


def show_local_login() -> None:
    st.subheader("Local Login (password)")
    st.caption(
        "Use this only for local tests. Inside SPCS trial, prefer Snowflake service/caller token."
    )
    with st.form("local_login_form", clear_on_submit=False):
        user = st.text_input("Snowflake User", value="")
        password = st.text_input("Password", value="", type="password")
        role = st.text_input("Role", value="SPCS_DEMO_RLS_ROLE")
        warehouse = st.text_input("Warehouse", value=APP_CONFIG["default_warehouse"])
        submitted = st.form_submit_button("Connect")

    if not submitted:
        return
    if not user or not password:
        st.error("User and password are required.")
        return

    try:
        conn = connect_with_password(user, password, role, warehouse)
        st.session_state["conn"] = conn
        st.session_state["auth_mode"] = "password"
        st.session_state["session_df"] = build_session_df(conn)
        st.success("Connected to Snowflake.")
        st.rerun()
    except Exception as exc:
        st.error(f"Connection failed: {exc}")


def auto_connect_inside_spcs() -> None:
    if st.session_state.get("conn") is not None:
        return
    if st.session_state.get("spcs_connect_attempted"):
        return
    if not SERVICE_TOKEN_PATH.exists():
        return

    st.session_state["spcs_connect_attempted"] = True
    try:
        conn, mode = connect_inside_spcs()
        st.session_state["conn"] = conn
        st.session_state["auth_mode"] = mode
        st.session_state["session_df"] = build_session_df(conn)
    except Exception as exc:
        st.session_state["spcs_connect_error"] = str(exc)


def show_runtime_panel():
    st.subheader("SPCS Runtime")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("UTC Time", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))
    c2.metric("Python", platform.python_version())
    c3.metric("Container Hostname", os.getenv("HOSTNAME", "n/a"))
    c4.metric("Platform", platform.system())


def show_api_panel():
    st.subheader("Public API Snapshot")
    market = fetch_market_snapshot()
    if not market["ok"]:
        st.warning(f"API unavailable: {market['error']}")
        st.caption(
            "On trial accounts this is expected if external access integration is unavailable."
        )
        return

    c1, c2, c3 = st.columns(3)
    c1.metric("BTC (USD)", f"{market['btc_usd']:,}", f"{market['btc_change']:.2f}%")
    c2.metric("ETH (USD)", f"{market['eth_usd']:,}", f"{market['eth_change']:.2f}%")
    c3.metric("SOL (USD)", f"{market['sol_usd']:,}", f"{market['sol_change']:.2f}%")
    st.caption(f"Source: {market['source']}")


def show_session_panel(conn):
    st.subheader("Snowflake Session")
    session_df = st.session_state.get("session_df")
    if session_df is not None:
        st.dataframe(session_df, use_container_width=True)

    try:
        df = run_df(
            conn,
            "SELECT CURRENT_ACCOUNT() AS ACCOUNT_NAME, CURRENT_REGION() AS REGION_NAME, "
            "CURRENT_VERSION() AS SNOWFLAKE_VERSION, CURRENT_USER() AS USER_NAME, "
            "CURRENT_ROLE() AS ROLE_NAME",
        )
        st.dataframe(df, use_container_width=True)
    except Exception as exc:
        st.warning(f"Could not load session metadata: {exc}")


def show_rls_panel(conn):
    st.subheader("Row-Level Security Demo")
    st.caption(
        "Rows are filtered by Row Access Policy using CURRENT_USER(). "
        "Use different Snowflake users to see different regions."
    )
    try:
        detail_sql = (
            f"SELECT ORDER_ID, REGION, AMOUNT, EVENT_TS "
            f"FROM {APP_CONFIG['orders_table']} "
            f"ORDER BY EVENT_TS DESC"
        )
        detail_df = run_df(conn, detail_sql)
        if detail_df.empty:
            st.info("No rows visible for this user.")
            return

        summary_df = detail_df.groupby("REGION", as_index=False)["AMOUNT"].sum()
        c1, c2 = st.columns([1, 1])
        c1.dataframe(summary_df, use_container_width=True, hide_index=True)
        c2.bar_chart(summary_df.set_index("REGION"))
        st.dataframe(detail_df, use_container_width=True, hide_index=True)
    except Exception as exc:
        st.warning(f"Could not read RLS demo table: {exc}")


def show_spcs_panel(conn):
    st.subheader("SPCS Service Introspection")
    st.caption("Shows service endpoints and container status from Snowflake metadata.")
    try:
        run_df(conn, f'USE DATABASE "{APP_CONFIG["service_database"]}"')
        run_df(conn, f'USE SCHEMA "{APP_CONFIG["service_schema"]}"')
        endpoints = run_df(
            conn, f'SHOW ENDPOINTS IN SERVICE "{APP_CONFIG["service_name"]}"'
        )
        containers = run_df(
            conn, f'SHOW SERVICE CONTAINERS IN SERVICE "{APP_CONFIG["service_name"]}"'
        )
        st.write("Endpoints")
        st.dataframe(endpoints, use_container_width=True)
        st.write("Containers")
        st.dataframe(containers, use_container_width=True)
    except Exception as exc:
        st.info(f"Service metadata not available for this user/role: {exc}")


def show_connect_error():
    err = st.session_state.get("spcs_connect_error")
    if not err:
        return
    st.error(f"Automatic SPCS connection failed: {err}")
    st.info(
        "Trial note: external access integration is not required for this auth mode. "
        "Check that caller grants are configured and the service has executeAsCaller enabled."
    )


def main():
    show_header()
    show_sidebar()

    auto_connect_inside_spcs()
    conn = get_state("conn", None)

    if conn is None:
        show_connect_error()
        if SERVICE_TOKEN_PATH.exists():
            if st.button("Retry SPCS Connection", use_container_width=True):
                st.session_state.pop("spcs_connect_attempted", None)
                st.session_state.pop("spcs_connect_error", None)
                st.rerun()
        elif APP_CONFIG["allow_password_login"]:
            show_local_login()
        else:
            st.warning(
                "No SPCS service token detected. Run this inside SPCS or set "
                "ALLOW_PASSWORD_LOGIN=true for local password login."
            )
        return

    top_left, top_right = st.columns([4, 1])
    with top_left:
        st.success("Connected. Explore SPCS features below.")
    with top_right:
        if st.button("Logout", use_container_width=True):
            try:
                conn.close()
            except Exception:
                pass
            for key in ("conn", "session_df", "auth_mode"):
                st.session_state.pop(key, None)
            st.rerun()

    show_runtime_panel()
    show_api_panel()
    show_session_panel(conn)
    show_rls_panel(conn)
    show_spcs_panel(conn)


if __name__ == "__main__":
    main()
