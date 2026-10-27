import matplotlib.pyplot as plt
import matplotlib.patches as patches
import os

def base_canvas(title_stage, title_main, title_sub, badge_color='#38bdf8'):
    fig, ax = plt.subplots(figsize=(20, 11.25), dpi=150)
    fig.patch.set_facecolor('#0b1120')
    ax.set_facecolor('#0b1120')
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.axis('off')

    # Top Pill Badge
    pill = patches.FancyBboxPatch((4, 91.5), 28, 4.2, boxstyle="round,pad=0.5", 
                                  facecolor='#1e293b', edgecolor=badge_color, linewidth=1.5)
    ax.add_patch(pill)
    ax.text(18, 93.6, title_stage, color=badge_color, fontsize=11.5, fontweight='bold', ha='center', va='center')

    # Main Header
    ax.text(4, 86.5, title_main, color='#ffffff', fontsize=25, fontweight='bold', ha='left', va='center')
    ax.text(4, 82.5, title_sub, color='#94a3b8', fontsize=12.5, ha='left', va='center')

    # Divider
    ax.plot([4, 96], [79.5, 79.5], color='#334155', linewidth=1.2)
    return fig, ax

def save_and_close(fig, output_path):
    plt.subplots_adjust(left=0, right=1, top=1, bottom=0)
    fig.savefig(output_path, facecolor=fig.get_facecolor(), edgecolor='none', bbox_inches='tight', pad_inches=0.15)
    plt.close(fig)
    print("Saved:", output_path)

# ==================== STEP 1: ARCHITECTURE BLUEPRINT ====================
def generate_step_1(output_path):
    fig, ax = base_canvas("DATA PIPELINE • STEP 01",
                          "Step 1: End-to-End Medallion Architecture & Pipeline Topology",
                          "High-Level Architecture: Decoupling raw ingestion from analytics-ready Kimball data marts across 10 disparate sources",
                          badge_color='#0284c7')

    # 4 Main Columns representing Stages
    stages = [
        ("DATA SOURCES (10 Files)", "150k+ Raw Records", [
            "• SRC01 Sales Trans (CSV - 119k rows)",
            "• SRC02 Sales Targets (XLSX - Multi-ver)",
            "• SRC03 Customer Master (CSV - 2k rows)",
            "• SRC04 Product Master (XLSX - 100 SKUs)",
            "• SRC05 Distributor Orders (XLSX - 36k rows)",
            "• SRC06 Distributor Master (CSV - 138 dist)",
            "• SRC07 Employee Master (XLSX - Transfers)",
            "• SRC08 Territory Mapping (XLSX)",
            "• SRC09 Return Transactions (CSV - 3.6k rows)",
            "• SRC10 Promotion Program (XLSX)"
        ], '#38bdf8', 4),

        ("BRONZE LAYER (Raw)", "schema_bronze", [
            "• Modular Python Loaders (10 modules)",
            "• Chunked Streaming for large CSVs",
            "• Append-only storage (No overwrite)",
            "• Non-destructive TEXT data types",
            "• Ingestion metadata injection:",
            "  - _batch_id, _ingested_at, _source_file",
            "• Audit log: schema_bronze.ingest_log",
            "• Automated Data Profiling Engine:",
            "  - Null % & Cardinality in profiling_summary",
            "  - Telemetry exported to Excel report"
        ], '#f59e0b', 27),

        ("SILVER LAYER (Staging)", "schema_silver", [
            "• Automated SQL Cleansing Pipeline",
            "• Composite key deduplication:",
            "  - ROW_NUMBER() over (order_id, product_id)",
            "• Phone & string regex sanitization",
            "• Standardized ISO-8601 Date casting",
            "• Strict type casting (INT, NUMERIC)",
            "• Computed business rules:",
            "  - gross_amount = qty * unit_price",
            "  - net_amount validation check",
            "• Referential integrity validation"
        ], '#94a3b8', 50),

        ("GOLD LAYER (Curated Marts)", "schema_gold", [
            "• Kimball Dimensional Star Schema",
            "• Conformed dimensions with Surrogate Keys",
            "• Slowly Changing Dimensions (SCD Type 2):",
            "  - dim_employees & dim_distributors",
            "• Point-in-Time (AS-OF) Historical Joins",
            "• Multi-version Target Ranking (v1, v2, v3)",
            "• Pre-aggregated Business Data Marts:",
            "  - mart_sales_vs_target (KPI %)",
            "  - mart_distributor_performance (Fill Rate)",
            "• Analytics-Ready for Downstream BI"
        ], '#10b981', 73)
    ]

    for title, subtitle, bullets, col, x in stages:
        box = patches.FancyBboxPatch((x, 23), 23, 53, boxstyle="round,pad=0.6", facecolor='#131e36', edgecolor='#1e293b', linewidth=1.5)
        ax.add_patch(box)
        hdr = patches.FancyBboxPatch((x + 0.5, 68.5), 22, 6.8, boxstyle="round,pad=0.3", facecolor='#1e293b', edgecolor=col, linewidth=1.2)
        ax.add_patch(hdr)
        ax.text(x + 11.5, 73.0, title, color=col, fontsize=11.0, fontweight='bold', ha='center', va='center')
        ax.text(x + 11.5, 70.2, subtitle, color='#94a3b8', fontsize=9.0, ha='center', va='center')

        by = 64.5
        for b in bullets:
            ax.text(x + 1.2, by, b, color='#cbd5e1', fontsize=8.6, ha='left', va='top')
            by -= 3.9

    # Arrows between stages
    for ax_pos in [26.0, 49.0, 72.0]:
        ax.annotate('', xy=(ax_pos + 1.0, 50), xytext=(ax_pos - 1.0, 50),
                    arrowprops=dict(arrowstyle="->", color='#38bdf8', lw=2.5))

    # Bottom Strip
    f = patches.FancyBboxPatch((4, 4), 92, 14.5, boxstyle="round,pad=0.5", facecolor='#1e293b', edgecolor='#0284c7', linewidth=1.2)
    ax.add_patch(f)
    ax.text(6, 15.0, "CORE ARCHITECTURAL PRINCIPLE — ISOLATION & TRACEABILITY:", color='#38bdf8', fontsize=11.5, fontweight='bold', ha='left')
    ax.text(6, 10.5, "1. Schema Isolation: Dedicated PostgreSQL schemas (schema_bronze, schema_silver, schema_gold) cleanly separate responsibilities.\n2. Idempotency: All stages can be re-run safely without corrupting data or generating orphaned duplicates.\n3. Complete Auditability: Every row loaded retains its lineage (_batch_id, _source_file, _ingested_at) back to origin files.", 
            color='#f1f5f9', fontsize=9.8, ha='left', va='center')

    save_and_close(fig, output_path)

# ==================== STEP 2: BRONZE INGESTION & PROFILING ====================
def generate_step_2(output_path):
    fig, ax = base_canvas("DATA PIPELINE • STEP 02",
                          "Step 2: Bronze Layer — Ingestion Engine, Schema-on-Read & Automated Profiling",
                          "Raw Data Lakehouse: Modular Python loaders, streaming chunking, metadata injection, and automated data quality audits",
                          badge_color='#d97706')

    # Left: Modular Python Ingestion Engine
    b1 = patches.FancyBboxPatch((4, 23), 44, 53, boxstyle="round,pad=0.6", facecolor='#131e36', edgecolor='#1e293b', linewidth=1.5)
    ax.add_patch(b1)
    h1 = patches.FancyBboxPatch((4.5, 68.5), 43, 6.8, boxstyle="round,pad=0.3", facecolor='#d97706', edgecolor='#fbbf24', linewidth=1.2)
    ax.add_patch(h1)
    ax.text(26.0, 71.9, "MODULAR PYTHON INGESTION ENGINE (LOADER/*.PY)", color='#ffffff', fontsize=12.5, fontweight='bold', ha='center', va='center')

    loader_features = [
        ("Chunked Streaming for Large Datasets", "• SRC01 Sales Transactions (119k+ rows, 20.6 MB) streamed in chunks.\n• Prevents memory spikes and guarantees lightweight execution in low-resource venvs.\n• Uses pandas.to_sql(chunksize=10000, method='multi') for rapid batch inserts."),
        ("Schema-on-Read & Type Persistence", "• Raw ingestion uses TEXT columns without premature type casting.\n• Prevents silent data truncation, invalid date crashes, and scientific notation loss.\n• Preserves exact source byte representations for full forensic auditability."),
        ("Multi-Version Planning Parser (SRC02)", "• Dynamically iterates all Excel sheets (Plan_v1_Original, Plan_v2_MidYear, etc.).\n• Unpivots wide budget matrices into unified long format in sales_targets_raw.\n• Logs sheet-level metadata and version tags into sales_target_files."),
        ("Decoupled Fault Tolerance & Logging", "• Each loader traps exceptions independently and logs to schema_bronze.ingest_log.\n• A network glitch on 1 source will NOT crash the remaining 9 data loaders.\n• Tracks rows_loaded, duration_sec, status (SUCCESS/FAILED), and stack traces.")
    ]
    ly = 65.0
    for title, desc in loader_features:
        card = patches.FancyBboxPatch((5.5, ly - 8.8), 41, 8.4, boxstyle="round,pad=0.3", facecolor='#1e293b', edgecolor='#334155', linewidth=1.0)
        ax.add_patch(card)
        ax.text(7.0, ly - 2.5, title, color='#fbbf24', fontsize=10.2, fontweight='bold', ha='left')
        ax.text(7.0, ly - 5.8, desc, color='#cbd5e1', fontsize=8.4, ha='left', va='center')
        ly -= 9.8

    # Right: Automated Data Profiling Engine
    b2 = patches.FancyBboxPatch((52, 23), 44, 53, boxstyle="round,pad=0.6", facecolor='#131e36', edgecolor='#1e293b', linewidth=1.5)
    ax.add_patch(b2)
    h2 = patches.FancyBboxPatch((52.5, 68.5), 43, 6.8, boxstyle="round,pad=0.3", facecolor='#b45309', edgecolor='#f59e0b', linewidth=1.2)
    ax.add_patch(h2)
    ax.text(74.0, 71.9, "AUTOMATED DATA PROFILING ENGINE (PROFILLING.PY)", color='#ffffff', fontsize=12.5, fontweight='bold', ha='center', va='center')

    profiling_cards = [
        ("Column-Level Health Telemetry", [
            "• Scans every ingested raw table immediately after loading.",
            "• Computes total rows, null counts, null %, and distinct cardinality.",
            "• Identifies dirty columns (e.g., phone numbers with letters, corrupt dates).",
            "• Records audit telemetry to schema_bronze.profiling_summary."
        ], 53.5, 47.0),
        ("Prioritizing Downstream Cleansing", [
            "• Automatically flags high-null columns (>30% missing) for Silver imputation.",
            "• Spots cardinality anomalies (e.g., duplicate customer_ids or order codes).",
            "• Informs foreign key join validity prior to Gold layer modeling.",
            "• Provides transparent data readiness benchmarks for analytics engineering."
        ], 74.5, 47.0),
        ("Exportable Stakeholder Audit Report", [
            "• Generates formatted Excel report: Profiling_Report.xlsx.",
            "• Outlines data health across all 10 sources with summary KPI cards.",
            "• Bridges communication with upstream source owners regarding data quality.",
            "• Serves as permanent evidence for enterprise data governance audits."
        ], 53.5, 25.0),
        ("Fail-Early Preventive Guardrails", [
            "• Re-profiling on fresh loads automatically cleans prior audit rows.",
            "• Prevents schema drift and unexpected missing columns from poisoning Silver.",
            "• Enables SQL-based alerting: SELECT * FROM profiling_summary WHERE null_pct > 0.",
            "• Validates raw row counts against source manifest benchmarks."
        ], 74.5, 25.0)
    ]
    for q_title, bullets, qx, qy in profiling_cards:
        q_box = patches.FancyBboxPatch((qx - 0.5, qy), 20, 19.5, boxstyle="round,pad=0.4", facecolor='#1e293b', edgecolor='#f59e0b', linewidth=1.1)
        ax.add_patch(q_box)
        ax.text(qx + 0.5, qy + 17.5, q_title, color='#fbbf24', fontsize=9.6, fontweight='bold', ha='left')
        by = qy + 14.0
        for b in bullets:
            ax.text(qx + 0.5, by, b, color='#cbd5e1', fontsize=8.2, ha='left', va='top')
            by -= 3.8

    # Bottom Strip
    f = patches.FancyBboxPatch((4, 4), 92, 14.5, boxstyle="round,pad=0.5", facecolor='#1e293b', edgecolor='#d97706', linewidth=1.2)
    ax.add_patch(f)
    ax.text(6, 15.0, "BRONZE LAYER DESIGN MANDATE: ZERO DATA LOSS & STRICT OBSERVABILITY", color='#fbbf24', fontsize=11.5, fontweight='bold', ha='left')
    ax.text(6, 10.5, "• Raw Storage: Tables are created append-only with _source_file, _source_platform, _ingested_at, and _batch_id metadata columns.\n• Audit Observability: ingest_log provides real-time duration and row count tracking per source; profiling_summary maps data quality.\n• Fault Isolation: If a single file format shifts or network fails, 90% of the data lakehouse still populates successfully.", 
            color='#f1f5f9', fontsize=9.8, ha='left', va='center')

    save_and_close(fig, output_path)

# ==================== STEP 3: SILVER CLEANSING ====================
def generate_step_3(output_path):
    fig, ax = base_canvas("DATA PIPELINE • STEP 03",
                          "Step 3: Silver Layer — Conformance, Deduplication & Data Cleansing",
                          "Clean Staging Warehouse: Composite key deduplication, phone regex formatting, ISO date standardization, and domain constraints",
                          badge_color='#64748b')

    cleansing_blocks = [
        ("COMPOSITE KEY DEDUPLICATION", "Target: sales_transactions & distributor_orders",
         "• Challenge: 119,101 raw sales rows contain 50k unique order_ids (multi-item orders).\n  Naive deduplication on order_id alone drops legitimate product lines.\n• SQL Pattern: Window function partitioning by composite key:\n  ROW_NUMBER() OVER (PARTITION BY TRIM(order_id), TRIM(product_id) ORDER BY _ingested_at DESC) AS rn\n• Filter: WHERE rn = 1 retains the latest ingested version and purges dirty re-transmissions.",
         '#38bdf8', 4, 47),

        ("PHONE & STRING REGEX SANITIZATION", "Target: customers & employees master",
         "• Challenge: Raw phone numbers contain mixed formats: dots, spaces, dashes, country codes\n  (e.g., '+84.903 123 456', '0903-123-456', '903123456').\n• SQL Pattern: REGEXP_REPLACE to strip non-digit characters and standardize prefix:\n  CASE WHEN phone ~ '^0' THEN phone WHEN phone ~ '^84' THEN '0' || SUBSTRING(phone FROM 3) END\n• Enforces uppercase on codes (region, channel) and INITCAP on human names.",
         '#fbbf24', 52, 47),

        ("STRICT TYPE CASTING & ISO DATES", "Target: All 10 staging tables",
         "• Challenge: Raw dates arrive in mixed formats ('DD/MM/YYYY', 'YYYY-MM-DD', blanks).\n  Numbers stored as text contain trailing spaces causing SQL math failures.\n• SQL Pattern: TO_DATE(NULLIF(TRIM(date_str), ''), 'YYYY-MM-DD')\n• Casts numerical amounts with NULLIF: NULLIF(TRIM(gross_amount), '')::NUMERIC.\n• Eliminates empty string casting exceptions across all numerical columns.",
         '#34d399', 4, 18),

        ("BUSINESS RULES & CONSTRAINTS", "Target: Financial measures & Order SLA",
         "• Financial Validation: Enforces mathematical consistency between gross and net amounts:\n  gross_amount = quantity * unit_price; net_amount = gross_amount - discount_amount.\n• Status Normalization: Standardizes delivery_status and payment_status into controlled vocabularies.\n• Referential Guards: Replaces blank provinces/territories with 'UNKNOWN' to prevent inner join dropouts.",
         '#c084fc', 52, 18)
    ]

    for title, target, desc, color, vx, vy in cleansing_blocks:
        card = patches.FancyBboxPatch((vx, vy), 44, 27, boxstyle="round,pad=0.5", facecolor='#131e36', edgecolor='#1e293b', linewidth=1.5)
        ax.add_patch(card)
        hdr = patches.FancyBboxPatch((vx + 0.5, vy + 21.0), 43, 5.2, boxstyle="round,pad=0.3", facecolor='#1e293b', edgecolor=color, linewidth=1.2)
        ax.add_patch(hdr)
        ax.text(vx + 2.0, vy + 23.6, title, color=color, fontsize=11.2, fontweight='bold', ha='left', va='center')
        ax.text(vx + 41.5, vy + 23.6, target, color='#94a3b8', fontsize=8.8, fontstyle='italic', ha='right', va='center')
        ax.text(vx + 2.0, vy + 10.5, desc, color='#cbd5e1', fontsize=8.8, ha='left', va='center')

    # Bottom Strip
    f = patches.FancyBboxPatch((4, 3.5), 92, 11.5, boxstyle="round,pad=0.5", facecolor='#1e293b', edgecolor='#64748b', linewidth=1.2)
    ax.add_patch(f)
    ax.text(6, 11.8, "SILVER STAGING CHECKPOINTS & VALIDATION GATES:", color='#94a3b8', fontsize=11.5, fontweight='bold', ha='left')
    ax.text(6, 7.5, "1. Deduplication Gate: SELECT order_id, product_id, COUNT(*) FROM schema_silver.sales_transactions GROUP BY 1,2 HAVING COUNT(*) > 1 -> MUST BE 0.\n2. Financial Gate: SELECT COUNT(*) FROM schema_silver.sales_transactions WHERE net_amount > gross_amount -> MUST BE 0.\n3. Fail-Fast Signal: Any failure in silver_*.sql immediately terminates the master orchestrator, preventing corrupted data from entering Gold.", 
            color='#f1f5f9', fontsize=9.6, ha='left', va='center')

    save_and_close(fig, output_path)

# ==================== STEP 4: GOLD KIMBALL & SCD2 ====================
def generate_step_4(output_path):
    fig, ax = base_canvas("DATA PIPELINE • STEP 04",
                          "Step 4: Gold Layer — Kimball Star Schema, SCD Type 2 & Analytical Marts",
                          "Analytics Engineering: Surrogate Keys, historical point-in-time joins, multi-version planning, and curated data marts",
                          badge_color='#eab308')

    # Left: SCD Type 2 & Point-in-Time Join
    b1 = patches.FancyBboxPatch((4, 23), 44, 53, boxstyle="round,pad=0.6", facecolor='#131e36', edgecolor='#1e293b', linewidth=1.5)
    ax.add_patch(b1)
    h1 = patches.FancyBboxPatch((4.5, 68.5), 43, 6.8, boxstyle="round,pad=0.3", facecolor='#ca8a04', edgecolor='#fde047', linewidth=1.2)
    ax.add_patch(h1)
    ax.text(26.0, 71.9, "SLOWLY CHANGING DIMENSIONS (SCD TYPE 2) & AS-OF JOINS", color='#ffffff', fontsize=12.0, fontweight='bold', ha='center', va='center')

    scd_points = [
        ("The Business Problem: Mid-Year Sales Transfers", 
         "• Rep EMP042 transferred from Region North to South on June 1st.\n• Overwriting employee records (SCD Type 1) retroactively alters historical reports,\n  misattributing Q1 North sales to the South and corrupting commission audits."),
        ("The SCD2 Mechanism: dim_employees & dim_distributors", 
         "• Tracks region, team, position, status changes with effective_date & expiry_date.\n• UPDATE closes existing record: SET expiry_date = new_date - 1, is_current = FALSE.\n• INSERT creates new record: with effective_date = new_date, is_current = TRUE."),
        ("Point-in-Time (AS-OF) Historical Join in fact_sales", 
         "• Fact tables DO NOT join on is_current = TRUE (which only reflects today's status).\n• Instead, join on order_date falling between employee's effective and expiry dates:\n  LEFT JOIN dim_employees e ON s.employee_id = e.employee_id\n       AND s.order_date >= e.effective_date AND (s.order_date <= e.expiry_date OR e.expiry_date IS NULL)"),
        ("Result: 100% Historical Integrity & True Attribution", 
         "• Past transactions remain forever linked to the exact territory active at transaction time.\n• Future transactions naturally bind to the new territory without retroactive distortion.")
    ]
    sy = 65.0
    for title, desc in scd_points:
        scard = patches.FancyBboxPatch((5.5, sy - 8.8), 41, 8.4, boxstyle="round,pad=0.3", facecolor='#1e293b', edgecolor='#334155', linewidth=1.0)
        ax.add_patch(scard)
        ax.text(7.0, sy - 2.5, title, color='#fde047', fontsize=10.2, fontweight='bold', ha='left')
        ax.text(7.0, sy - 5.8, desc, color='#cbd5e1', fontsize=8.4, ha='left', va='center')
        sy -= 9.8

    # Right: Curated Analytical Data Marts
    b2 = patches.FancyBboxPatch((52, 23), 44, 53, boxstyle="round,pad=0.6", facecolor='#131e36', edgecolor='#1e293b', linewidth=1.5)
    ax.add_patch(b2)
    h2 = patches.FancyBboxPatch((52.5, 68.5), 43, 6.8, boxstyle="round,pad=0.3", facecolor='#059669', edgecolor='#34d399', linewidth=1.2)
    ax.add_patch(h2)
    ax.text(74.0, 71.9, "CURATED BUSINESS DATA MARTS (BI CONSUMPTION READY)", color='#ffffff', fontsize=12.0, fontweight='bold', ha='center', va='center')

    marts = [
        ("mart_sales_vs_target (Executive KPI Attainment)", [
            "• Purpose: Monthly sales rep & team performance tracking.",
            "• Multi-Version Logic: Window ranking dynamically pulls latest target plan:",
            "  ROW_NUMBER() OVER (PARTITION BY employee_sk, year, month",
            "  ORDER BY version_date DESC NULLS LAST, plan_version DESC) AS rn",
            "• Key Metrics: Target Revenue, Actual Revenue, Variance, Achievement Rate %."
        ], 53.5, 47.0),
        ("mart_distributor_performance (Logistics SLA)", [
            "• Purpose: Evaluates tier-based distributor fulfillment operations.",
            "• Grain: 1 record per distributor per calendar month.",
            "• Fill Rate % = (Total Delivered Qty / Total Ordered Qty) * 100.",
            "• On-Time Delivery Rate % = (On-Time Shipments / Total Shipments) * 100.",
            "• Highlights underperforming distributors requiring supply chain intervention."
        ], 74.5, 47.0),
        ("Conformed Date Dimension (dim_date)", [
            "• Generated via PostgreSQL generate_series('2023-01-01', '2025-12-31').",
            "• Provides integer surrogate key date_id (YYYYMMDD).",
            "• Enforces standardized calendar attributes: year, quarter, month, week,",
            "  day_of_week, is_weekend, and month_name for BI slicing."
        ], 53.5, 25.0),
        ("Fact Return Lineage (fact_returns)", [
            "• Tracks product return transactions and financial credit notes.",
            "• Connects return reasons (Damaged, Expired, Wrong Item) to products & reps.",
            "• Calculates Net Revenue = Gross Sales - Returns - Discounts.",
            "• Identifies packaging defect trends across specific manufacturing batches."
        ], 74.5, 25.0)
    ]
    for m_title, bullets, mx, my in marts:
        m_box = patches.FancyBboxPatch((mx - 0.5, my), 20, 19.5, boxstyle="round,pad=0.4", facecolor='#1e293b', edgecolor='#34d399', linewidth=1.1)
        ax.add_patch(m_box)
        ax.text(mx + 0.5, my + 17.5, m_title, color='#34d399', fontsize=9.6, fontweight='bold', ha='left')
        by = my + 14.0
        for b in bullets:
            ax.text(mx + 0.5, by, b, color='#cbd5e1', fontsize=8.2, ha='left', va='top')
            by -= 3.8

    # Bottom Strip
    f = patches.FancyBboxPatch((4, 4), 92, 14.5, boxstyle="round,pad=0.5", facecolor='#1e293b', edgecolor='#eab308', linewidth=1.2)
    ax.add_patch(f)
    ax.text(6, 15.0, "GOLD LAYER KIMBALL STAR SCHEMA ARCHITECTURE:", color='#fde047', fontsize=11.5, fontweight='bold', ha='left')
    ax.text(6, 10.5, "• Conformed Dimensions: dim_date, dim_customers, dim_products, dim_employees (SCD2), dim_distributors (SCD2).\n• Central Fact Tables: fact_sales (grain: order line item), fact_targets (grain: employee-month-version), fact_returns (grain: return item).\n• Pre-Aggregated Marts: mart_sales_vs_target and mart_distributor_performance deliver sub-second query performance for executive BI tools.", 
            color='#f1f5f9', fontsize=9.8, ha='left', va='center')

    save_and_close(fig, output_path)

# ==================== STEP 5: ORCHESTRATION & ANALYTICS ====================
def generate_step_5(output_path):
    fig, ax = base_canvas("DATA PIPELINE • STEP 05",
                          "Step 5: Orchestration Engine, Observability & Analytics Delivery",
                          "Production Reliability: Fail-Fast inter-stage gates, UUID batch logging, automated tests, and BI-ready data marts",
                          badge_color='#7c3aed')

    # Left: Orchestrator Architecture & Logging
    b1 = patches.FancyBboxPatch((4, 23), 44, 53, boxstyle="round,pad=0.6", facecolor='#131e36', edgecolor='#1e293b', linewidth=1.5)
    ax.add_patch(b1)
    h1 = patches.FancyBboxPatch((4.5, 68.5), 43, 6.8, boxstyle="round,pad=0.3", facecolor='#7c3aed', edgecolor='#c084fc', linewidth=1.2)
    ax.add_patch(h1)
    ax.text(26.0, 71.9, "MASTER ORCHESTRATION ENGINE (MAIN.PY)", color='#ffffff', fontsize=12.5, fontweight='bold', ha='center', va='center')

    orch_items = [
        ("Fail-Fast Inter-Stage Dependency Gates", 
         "• Strict execution order: Bronze -> Silver -> Gold.\n• If Silver staging fails, pipeline HALTS immediately: Gold is NEVER executed.\n• Prevents dirty or unvalidated data from poisoning production business marts."),
        ("Resilient Bronze Stage Execution", 
         "• Each of the 10 data sources loads independently inside run_bronze().\n• Errors on a single source are logged to schema_bronze.ingest_log without\n  aborting the remaining 9 sources, ensuring maximal data availability."),
        ("Unified UUID Batch Telemetry (pipeline_logs)", 
         "• Every execution run generates a unique UUID batch_id.\n• Records stage name, status (SUCCESS/FAILED), execution duration_sec,\n  error stack trace, and timestamp in schema_gold.pipeline_logs."),
        ("Automated SQL Quality Checkpoints", 
         "• Validates row count preservation between Bronze and Silver.\n• Verifies zero duplicate composite keys and zero negative revenue values.\n• Validates foreign key referential integrity across all fact-dimension joins.")
    ]
    oy = 65.0
    for title, desc in orch_items:
        ocard = patches.FancyBboxPatch((5.5, oy - 8.8), 41, 8.4, boxstyle="round,pad=0.3", facecolor='#1e293b', edgecolor='#334155', linewidth=1.0)
        ax.add_patch(ocard)
        ax.text(7.0, oy - 2.5, title, color='#c084fc', fontsize=10.2, fontweight='bold', ha='left')
        ax.text(7.0, oy - 5.8, desc, color='#cbd5e1', fontsize=8.4, ha='left', va='center')
        oy -= 9.8

    # Right: Analytics Delivery & BI-Ready Data Marts
    b2 = patches.FancyBboxPatch((52, 23), 44, 53, boxstyle="round,pad=0.6", facecolor='#131e36', edgecolor='#1e293b', linewidth=1.5)
    ax.add_patch(b2)
    h2 = patches.FancyBboxPatch((52.5, 68.5), 43, 6.8, boxstyle="round,pad=0.3", facecolor='#4f46e5', edgecolor='#818cf8', linewidth=1.2)
    ax.add_patch(h2)
    ax.text(74.0, 71.9, "EXECUTIVE ANALYTICS DATA MARTS & BI READINESS", color='#ffffff', fontsize=12.0, fontweight='bold', ha='center', va='center')

    bi_cards = [
        ("Curated PostgreSQL Data Marts", [
            "• Direct analytical access via pre-aggregated schema_gold data marts.",
            "• Minimizes visual query latency for downstream BI tools (Power BI, Tableau).",
            "• Enforces single source of truth across all commercial KPIs.",
            "• Data model strictly mirrors conformed Kimball Star Schema."
        ], 53.5, 47.0),
        ("Sales vs Target Attainment Mart", [
            "• Aggregates actual net revenue, target quota, gap, and achievement %.",
            "• Evaluates 1,332 monthly records across 111 sales representatives.",
            "• Tracks variance deltas between v1 Original budget and v2 Mid-Year updates.",
            "• Seamlessly joins fact_sales, fact_targets, and fact_returns."
        ], 74.5, 47.0),
        ("Distributor Logistics SLA Mart", [
            "• Tracks 1,480 monthly records across 125 active distributors.",
            "• Computes Fill Rate % = (Delivered Qty / Ordered Qty) * 100.",
            "• Computes On-Time Delivery Rate % = (On-Time / Total Shipments) * 100.",
            "• Exposes regional logistics bottlenecks and delivery delay clusters."
        ], 53.5, 25.0),
        ("Product Return & Net Margin Audit", [
            "• Evaluates gross vs net sales margin erosion from returns and discounts.",
            "• Connects return reasons (Damaged in transit vs Expired batch) to SKUs.",
            "• Verified total product returns: 1.31 Billion VND across 3,665 lines.",
            "• Delivers audited financial foundations for executive reporting."
        ], 74.5, 25.0)
    ]
    for b_title, bullets, bx, by in bi_cards:
        b_box = patches.FancyBboxPatch((bx - 0.5, by), 20, 19.5, boxstyle="round,pad=0.4", facecolor='#1e293b', edgecolor='#818cf8', linewidth=1.1)
        ax.add_patch(b_box)
        ax.text(bx + 0.5, by + 17.5, b_title, color='#818cf8', fontsize=9.6, fontweight='bold', ha='left')
        by_b = by + 14.0
        for b in bullets:
            ax.text(bx + 0.5, by_b, b, color='#cbd5e1', fontsize=8.2, ha='left', va='top')
            by_b -= 3.8

    # Bottom Strip
    f = patches.FancyBboxPatch((4, 4), 92, 14.5, boxstyle="round,pad=0.5", facecolor='#1e293b', edgecolor='#7c3aed', linewidth=1.2)
    ax.add_patch(f)
    ax.text(6, 15.0, "ENTERPRISE ANALYTICS ENGINEERING MATURITY & BI READINESS:", color='#c084fc', fontsize=11.5, fontweight='bold', ha='left')
    ax.text(6, 10.5, "• BI Consumption Readiness: Curated Gold data marts eliminate heavy joins, enabling instant connection for downstream BI tools.\n• Operational Resiliency: Fail-fast gates and independent source logging guarantee that data pipeline failures never corrupt the data warehouse.\n• End-to-End Lineage: Any business metric in Gold can be traced back through Silver SQL transformations and Bronze raw logs to the exact source row.", 
            color='#f1f5f9', fontsize=9.8, ha='left', va='center')

    save_and_close(fig, output_path)

if __name__ == '__main__':
    base_dir = r'C:\Users\admin\Desktop\Xuân Thu\Da_Portfolio\03_Python\medallion-datapipeline\assets\pipeline'
    generate_step_1(os.path.join(base_dir, 'step_1_architecture_blueprint.png'))
    generate_step_2(os.path.join(base_dir, 'step_2_bronze_ingestion_profiling.png'))
    generate_step_3(os.path.join(base_dir, 'step_3_silver_cleansing_normalization.png'))
    generate_step_4(os.path.join(base_dir, 'step_4_gold_kimball_scd2.png'))
    generate_step_5(os.path.join(base_dir, 'step_5_orchestration_analytics.png'))
    print("All 5 Data Pipeline stage infographics generated successfully!")
