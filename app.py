"""
Real-Time Retail Analytics: Cloud Data Engineering Pipeline
Interactive Web Dashboard & Analytics Workbench
Built with Streamlit, DuckDB, Plotly, and Apache Beam
"""

import os
import sys
import time
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import CONFIG
from src.warehouse.duckdb_warehouse import DuckDBWarehouse
from src.pipeline.batch_pipeline import RetailBatchPipeline
from src.loaders.bigquery_loader import BigQueryLoader

# Page Configuration
st.set_page_config(
    page_title="Retail Analytics | Cloud Data Engineering Pipeline",
    page_icon="🛍️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.1rem;
        font-weight: 700;
        color: #1a73e8;
        margin-bottom: 0.2rem;
    }
    .sub-header {
        font-size: 1.05rem;
        color: #5f6368;
        margin-bottom: 1.5rem;
    }
    .kpi-card {
        background-color: #f8f9fa;
        border: 1px solid #dadce0;
        border-radius: 8px;
        padding: 16px;
        text-align: center;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }
    .kpi-value {
        font-size: 1.8rem;
        font-weight: 700;
        color: #202124;
    }
    .kpi-label {
        font-size: 0.85rem;
        font-weight: 600;
        color: #5f6368;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    .badge-local {
        background-color: #e6f4ea;
        color: #137333;
        padding: 4px 10px;
        border-radius: 12px;
        font-size: 0.85rem;
        font-weight: 600;
    }
    .badge-gcp {
        background-color: #e8f0fe;
        color: #1a73e8;
        padding: 4px 10px;
        border-radius: 12px;
        font-size: 0.85rem;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)

# Initialize Warehouse and Pipeline
@st.cache_resource
def get_warehouse():
    return DuckDBWarehouse()

warehouse = get_warehouse()
pipeline_runner = RetailBatchPipeline(warehouse=warehouse)

# Sidebar Configuration
with st.sidebar:
    st.image("https://www.gstatic.com/devrel-devsite/prod/v2.12.0/gcp/images/gcp-logo.png", width=180)
    st.markdown("### **Pipeline Controls**")
    
    # Execution Mode Indicator
    is_gcp = CONFIG.is_gcp_configured()
    mode_label = "GCP Cloud Mode" if is_gcp else "Local Mode (DuckDB + DirectRunner)"
    badge_class = "badge-gcp" if is_gcp else "badge-local"
    st.markdown(f"**Runtime Environment:** <span class='{badge_class}'>{mode_label}</span>", unsafe_allow_html=True)
    
    if not is_gcp:
        st.info("Operating in standalone local mode. All storage & queries run on local DuckDB columnar warehouse.")
    
    st.markdown("---")
    st.markdown("### **Data Ingestion Source**")
    source_choice = st.radio(
        "Select Input Source",
        ["Built-in Sample Data (1,200 records)", "Upload Custom CSV/JSON"],
        index=0
    )

    uploaded_file = None
    file_type = "csv"
    if source_choice == "Upload Custom CSV/JSON":
        uploaded_file = st.file_uploader("Upload Transaction File", type=["csv", "json"])
        if uploaded_file:
            file_type = "json" if uploaded_file.name.endswith(".json") else "csv"

    st.markdown("---")
    st.markdown("### **Run Pipeline**")
    run_btn = st.button("🚀 Trigger Data Pipeline", type="primary", use_container_width=True)

    st.markdown("---")
    st.markdown("**Author Portfolio:** [GitHub @2005swathi2000](https://github.com/2005swathi2000)")
    st.markdown("**Role Focus:** Google Data & AI Engineer")

# Pipeline Execution Handler
if run_btn:
    with st.spinner("Processing retail ingestion batch, running data quality validations, and loading warehouse..."):
        try:
            if source_choice == "Upload Custom CSV/JSON" and uploaded_file is not None:
                report = pipeline_runner.run(source=uploaded_file, file_type=file_type)
            else:
                report = pipeline_runner.run(source=CONFIG.SAMPLE_DATA_CSV, file_type="csv")
            st.session_state["last_run_report"] = report
            st.success(f"Pipeline Run `{report['run_id']}` completed in {report['duration_seconds']}s!")
        except Exception as e:
            st.error(f"Pipeline execution error: {str(e)}")

# Header
st.markdown("<div class='main-header'>Real-Time Retail Analytics: Cloud Data Engineering Pipeline</div>", unsafe_allow_html=True)
st.markdown("<div class='sub-header'>End-to-end ELT architecture featuring Schema Validation, Data Quality Quarantine, DuckDB OLAP Warehouse, and Apache Beam / BigQuery integration.</div>", unsafe_allow_html=True)

# Main Navigation Tabs
tab_analytics, tab_workbench, tab_quality, tab_lineage, tab_architecture, tab_export = st.tabs([
    "📊 Retail Analytics KPIs",
    "💻 SQL Workbench",
    "🛡️ Data Quality & Quarantine",
    "🧬 Lineage & Governance",
    "🏗️ Cloud Architecture",
    "💾 Downloads & Exports"
])

# ==========================================
# TAB 1: RETAIL ANALYTICS KPIS
# ==========================================
with tab_analytics:
    # Query summary from warehouse
    try:
        summary_df = warehouse.execute_query("""
            SELECT 
                COUNT(*) AS total_txns,
                COUNT(DISTINCT customer_id) AS unique_custs,
                ROUND(SUM(total_amount), 2) AS total_revenue,
                ROUND(AVG(total_amount), 2) AS avg_order_val,
                SUM(quantity) AS total_units,
                ROUND(SUM(discount_amount), 2) AS total_discounts
            FROM retail_transactions;
        """)
        
        has_data = not summary_df.empty and summary_df.iloc[0]["total_txns"] > 0
    except Exception:
        has_data = False

    if not has_data:
        st.warning("Warehouse is currently empty. Click '**Trigger Data Pipeline**' in the sidebar to ingest and process the dataset.")
    else:
        row = summary_df.iloc[0]
        c1, c2, c3, c4, c5 = st.columns(5)
        with c1:
            st.metric("Total Revenue", f"${row['total_revenue']:,.2f}")
        with c2:
            st.metric("Transactions", f"{int(row['total_txns']):,}")
        with c3:
            st.metric("Unique Customers", f"{int(row['unique_custs']):,}")
        with c4:
            st.metric("Average Order Value", f"${row['avg_order_val']:.2f}")
        with c5:
            st.metric("Units Sold", f"{int(row['total_units']):,}")

        st.markdown("---")
        col_left, col_right = st.columns([3, 2])

        with col_left:
            st.subheader("Daily Revenue Trend")
            daily_df = warehouse.execute_query("""
                SELECT transaction_date, SUM(total_amount) AS revenue, COUNT(*) AS orders
                FROM retail_transactions
                GROUP BY transaction_date
                ORDER BY transaction_date ASC;
            """)
            fig_trend = px.line(
                daily_df, x="transaction_date", y="revenue",
                title="Revenue Over Time (USD)",
                labels={"transaction_date": "Date", "revenue": "Daily Revenue ($)"},
                markers=True,
                color_discrete_sequence=["#1a73e8"]
            )
            fig_trend.update_layout(height=380, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig_trend, use_container_width=True)

        with col_right:
            st.subheader("Revenue by Product Category")
            cat_df = warehouse.execute_query("""
                SELECT product_category, SUM(total_amount) AS revenue
                FROM retail_transactions
                GROUP BY product_category
                ORDER BY revenue DESC;
            """)
            fig_cat = px.pie(
                cat_df, names="product_category", values="revenue",
                title="Category Sales Share",
                hole=0.45,
                color_discrete_sequence=px.colors.qualitative.Safe
            )
            fig_cat.update_layout(height=380, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig_cat, use_container_width=True)

        col_reg, col_pay = st.columns(2)
        with col_reg:
            st.subheader("Regional Performance")
            reg_df = warehouse.execute_query("""
                SELECT store_region, SUM(total_amount) AS revenue, COUNT(*) AS orders
                FROM retail_transactions
                GROUP BY store_region
                ORDER BY revenue DESC;
            """)
            fig_reg = px.bar(
                reg_df, x="store_region", y="revenue", color="store_region",
                title="Billed Revenue by Region",
                labels={"store_region": "Market Region", "revenue": "Total Revenue ($)"},
                text_auto=".2s"
            )
            fig_reg.update_layout(showlegend=False, height=350)
            st.plotly_chart(fig_reg, use_container_width=True)

        with col_pay:
            st.subheader("Payment Method Distribution")
            pay_df = warehouse.execute_query("""
                SELECT payment_method, COUNT(*) AS transaction_count, SUM(total_amount) AS revenue
                FROM retail_transactions
                GROUP BY payment_method
                ORDER BY transaction_count DESC;
            """)
            fig_pay = px.bar(
                pay_df, x="payment_method", y="transaction_count",
                title="Transactions by Payment Method",
                color_discrete_sequence=["#34a853"],
                labels={"payment_method": "Method", "transaction_count": "Transactions"},
                text_auto=True
            )
            fig_pay.update_layout(height=350)
            st.plotly_chart(fig_pay, use_container_width=True)

# ==========================================
# TAB 2: SQL WORKBENCH
# ==========================================
with tab_workbench:
    st.subheader("Analytical SQL Workbench")
    st.caption("Live execution against DuckDB Columnar Warehouse. Choose a pre-built analytical query or write your own SQL.")

    sql_dir = PROJECT_ROOT / "sql"
    sql_files = sorted([f.name for f in sql_dir.glob("*.sql")]) if sql_dir.exists() else []

    selected_sql_file = st.selectbox("Select Pre-Built Query", sql_files, index=0)
    
    default_sql = ""
    if selected_sql_file:
        with open(sql_dir / selected_sql_file, "r", encoding="utf-8") as f:
            default_sql = f.read()

    sql_input = st.text_area("SQL Editor", value=default_sql, height=220)
    
    col_run_sql, col_clear = st.columns([1, 4])
    with col_run_sql:
        execute_sql = st.button("▶ Run Query", type="primary")

    if execute_sql or default_sql:
        t0 = time.time()
        try:
            # Strip comments for clean execution
            clean_query = "\n".join([line for line in sql_input.splitlines() if not line.strip().startswith("--")])
            if clean_query.strip():
                query_result = warehouse.execute_query(clean_query)
                elapsed = round((time.time() - t0) * 1000, 2)
                st.success(f"Query returned {len(query_result)} rows in {elapsed} ms.")
                st.dataframe(query_result, use_container_width=True, hide_index=True)
        except Exception as e:
            st.error(f"SQL Execution Error: {str(e)}")

# ==========================================
# TAB 3: DATA QUALITY & QUARANTINE
# ==========================================
with tab_quality:
    st.subheader("Data Quality Governance & Quarantine Audit")
    
    # Check latest audit run or pipeline report
    audit_df = warehouse.execute_query("""
        SELECT * FROM pipeline_audit_runs ORDER BY execution_timestamp DESC LIMIT 1;
    """)

    if audit_df.empty:
        st.info("No pipeline audit runs recorded yet. Trigger the pipeline to generate quality metrics.")
    else:
        latest = audit_df.iloc[0]
        
        q1, q2, q3, q4 = st.columns(4)
        with q1:
            st.metric("Data Quality Score", f"{latest['data_quality_score']:.1f}%")
        with q2:
            st.metric("Accepted Clean Rows", f"{int(latest['accepted_records']):,}")
        with q3:
            st.metric("Quarantined Rows", f"{int(latest['rejected_records']):,}")
        with q4:
            st.metric("Duplicates Handled", f"{int(latest['duplicate_records']):,}")

        st.caption(
            "⚠️ Disclaimer: The Data Quality Score is a mathematical metric of structural and business-rule "
            "conformance against defined constraints. It does not guarantee external ground-truth accuracy."
        )

        st.markdown("---")
        st.subheader("Quarantine Reason Distribution")
        
        quarantine_summary = warehouse.execute_query("""
            SELECT rejection_reason, COUNT(*) AS count
            FROM rejected_transactions
            GROUP BY rejection_reason
            ORDER BY count DESC;
        """)

        if not quarantine_summary.empty:
            col_chart, col_tbl = st.columns([1, 1])
            with col_chart:
                fig_q = px.bar(
                    quarantine_summary, x="count", y="rejection_reason",
                    orientation="h",
                    title="Failures by Reason Code",
                    labels={"count": "Failed Rows", "rejection_reason": "Reason Code"},
                    color_discrete_sequence=["#ea4335"]
                )
                fig_q.update_layout(height=350, margin=dict(l=10, r=10, t=40, b=10))
                st.plotly_chart(fig_q, use_container_width=True)

            with col_tbl:
                st.markdown("**Quarantined Records Sample**")
                rej_sample = warehouse.execute_query("""
                    SELECT transaction_id, customer_id, rejection_reason, rejected_at
                    FROM rejected_transactions
                    ORDER BY rejected_at DESC
                    LIMIT 10;
                """)
                st.dataframe(rej_sample, use_container_width=True, hide_index=True)
        else:
            st.success("No quarantined records found in the warehouse.")

# ==========================================
# TAB 4: LINEAGE & GOVERNANCE
# ==========================================
with tab_lineage:
    st.subheader("Data Lineage & Governance Controls")
    
    st.markdown("""
    ```mermaid
    flowchart LR
        subgraph Ingestion
            A[CSV / JSON Source Files] -->|DataIngestionReader| B[Raw Ingestion Buffer]
        end
        subgraph Validation & Quarantine
            B --> C{DataValidator}
            C -->|Fails Checks| D[(Quarantine Table: rejected_transactions)]
            C -->|Passes Checks| E[Valid Stream]
        end
        subgraph Transformation
            E -->|Financial Arithmetic| F[Gross / Net / Tax / Total]
            F -->|Calendar Derivation| G[Date & Hour Normalization]
            G -->|PII Minimization| H[Email Masking: j***e@domain]
        end
        subgraph Warehouse Storage
            H -->|Idempotent Upsert| I[(DuckDB: retail_transactions)]
            H -.->|Optional Cloud Sync| J[(BigQuery Table: Partitioned & Clustered)]
        end
    ```
    """)

    st.markdown("---")
    st.subheader("PII Protection Verification")
    st.markdown("Customer emails are pseudonymized during transformation to minimize PII exposure in the analytical warehouse.")
    
    pii_sample = warehouse.execute_query("""
        SELECT customer_id, customer_masked_email, transaction_id, total_amount
        FROM retail_transactions
        LIMIT 6;
    """)
    if not pii_sample.empty:
        st.dataframe(pii_sample, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.subheader("Pipeline Audit Run History")
    audit_history = warehouse.execute_query("""
        SELECT run_id, execution_timestamp, execution_mode, total_records, accepted_records, rejected_records, data_quality_score, duration_seconds, status
        FROM pipeline_audit_runs
        ORDER BY execution_timestamp DESC
        LIMIT 10;
    """)
    if not audit_history.empty:
        st.dataframe(audit_history, use_container_width=True, hide_index=True)

# ==========================================
# TAB 5: CLOUD ARCHITECTURE
# ==========================================
with tab_architecture:
    st.subheader("Cloud Architecture & Google Data Engineer Competencies")
    
    st.markdown("""
    ### **Dual-Mode Architecture (Local DuckDB + Google Cloud Dataflow/BigQuery)**
    This project is engineered to run seamlessly locally with zero cloud costs while maintaining a 1:1 architectural parity with enterprise Google Cloud Platform (GCP) patterns.
    """)

    col_g1, col_g2 = st.columns(2)
    with col_g1:
        st.markdown("""
        #### **1. Local Execution Path (Default)**
        - **Processing Runner:** DirectRunner / High-Performance Batch Engine
        - **Warehouse Engine:** DuckDB Columnar OLAP
        - **Quarantine Storage:** Local Parquet / CSV Quarantine Table
        - **Hardware Profile:** Optimized for 8 GB RAM CPU laptops
        - **Cloud Account:** None required
        """)
    with col_g2:
        st.markdown("""
        #### **2. Google Cloud Production Path (Optional)**
        - **Managed Processing:** Cloud Dataflow (Apache Beam autoscaling runner)
        - **Raw Object Storage:** Google Cloud Storage (`gs://retail-transactions/`)
        - **Cloud Warehouse:** Google BigQuery (Partitioned by `transaction_date`, Clustered by `store_region, product_category`)
        - **Access Control:** GCP IAM Least-Privilege service accounts
        """)

    st.markdown("---")
    st.subheader("BigQuery Production DDL & Partitioning Strategy")
    st.code(BigQueryLoader.get_bigquery_ddl(), language="sql")

# ==========================================
# TAB 6: DOWNLOADS & EXPORTS
# ==========================================
with tab_export:
    st.subheader("Export Processed Datasets & Reports")
    st.markdown("Download verified outputs generated directly from warehouse execution.")

    c_dl1, c_dl2 = st.columns(2)
    with c_dl1:
        st.markdown("#### **Transformed Clean Dataset**")
        st.caption("Clean, enriched, and PII-masked transactions ready for downstream BI / ML.")
        if CONFIG.CLEAN_PARQUET_PATH.exists():
            clean_export_df = pd.read_parquet(CONFIG.CLEAN_PARQUET_PATH)
            csv_clean_bytes = clean_export_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="📥 Download Clean Transactions (CSV)",
                data=csv_clean_bytes,
                file_name="clean_retail_transactions.csv",
                mime="text/csv",
                use_container_width=True
            )
        else:
            st.info("Clean dataset not yet generated. Trigger the pipeline first.")

    with c_dl2:
        st.markdown("#### **Quarantined Rejected Records**")
        st.caption("Malformed, negative-value, and duplicate records with failure reason codes.")
        if CONFIG.REJECTED_DATA_PATH.exists():
            with open(CONFIG.REJECTED_DATA_PATH, "rb") as f:
                rej_bytes = f.read()
            st.download_button(
                label="📥 Download Quarantined Records (CSV)",
                data=rej_bytes,
                file_name="quarantined_rejected_records.csv",
                mime="text/csv",
                use_container_width=True
            )
        else:
            st.info("Quarantine file not yet generated. Trigger the pipeline first.")

# Footer
st.markdown("---")
st.markdown(
    "<div style='text-align: center; color: #70757a; font-size: 0.85rem;'>"
    "Real-Time Retail Analytics: Cloud Data Engineering Pipeline | "
    "Designed for Google Data & AI Engineer Portfolio | "
    "<a href='https://github.com/2005swathi2000' target='_blank'>GitHub: @2005swathi2000</a>"
    "</div>",
    unsafe_allow_html=True
)

