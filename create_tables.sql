-- ════════════════════════════════════════════════════════════════════
--  BigQuery Chart Engine  —  Table DDL  (v4)
--  File : create_tables.sql
--
--  Tables:
--    (A) email_body    — source view / table  (html with {{PLACEHOLDERS}})
--    (B) chart_config  — chart definitions    (one row per placeholder)
--    (C) email_output  — final output         (rendered HTML written back)
--
--  Before running:
--    Replace all occurrences of:
--      your_project  →  your GCP project ID
--      your_dataset  →  your BigQuery dataset name
--
--  Run in BigQuery console:
--    Paste into the Query editor and click Run
--  Or via bq CLI:
--    bq query --use_legacy_sql=false --project_id=your_project \
--             < create_tables.sql
-- ════════════════════════════════════════════════════════════════════


-- ════════════════════════════════════════════════════════════════════
-- (A)  email_body
--      If you already have this as a VIEW, skip this block.
--      Only run if you want a base TABLE to INSERT rows into directly.
-- ════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.email_body`
(
  -- Unique ID for each email. Referenced in email_output.
  email_id          STRING      NOT NULL,

  -- Address shown in the From: field.
  sender_email      STRING      NOT NULL,

  -- Email subject line.
  subject           STRING,

  -- Recipient address. Comma-separate multiple addresses.
  recipient_email   STRING      NOT NULL,

  -- Full HTML body with {{PLACEHOLDER}} tokens where charts go.
  -- Tokens must be UPPERCASE, underscores only, e.g. {{SALES_CHART}}.
  -- Each token must have a matching variable_name row in chart_config.
  html_body         STRING      NOT NULL,

  -- Auto-set to current UTC time on insert.
  created_at        TIMESTAMP   DEFAULT (CURRENT_TIMESTAMP())
)
OPTIONS (
  description =
    'Source email table. html_body contains {{PLACEHOLDER}} tokens '
    'that the chart engine replaces with embedded base64 chart images.'
);


-- ════════════════════════════════════════════════════════════════════
-- (B)  chart_config
--      One row per chart placeholder.
--      variable_name must exactly match a {{TOKEN}} in html_body.
--      Engine builds:  SELECT x_column, y_columns
--                      FROM bq_table  WHERE filters
-- ════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.chart_config`
(
  -- Must match the {{PLACEHOLDER}} in email_body.html_body exactly.
  -- e.g.  SALES_CHART  matches  {{SALES_CHART}}
  variable_name   STRING    NOT NULL,

  -- line   - line chart (supports multiple y series)
  -- bar    - grouped bar chart (supports multiple y series)
  -- pie    - pie chart (single y column)
  -- donut  - donut chart with centre total (single y column)
  chart_type      STRING    NOT NULL,

  -- Full BigQuery path:  project.dataset.table_or_view
  bq_table        STRING    NOT NULL,

  -- Optional WHERE clause, write conditions only, no WHERE keyword.
  -- Examples:
  --   year = 2026
  --   region = 'APAC' AND status = 'Active'
  --   report_date = '2026-04-01'
  -- Use empty string or NULL for no filter.
  filters         STRING,

  -- X-axis label column (line/bar) or slice label column (pie/donut).
  x_column        STRING    NOT NULL,

  -- Comma-separated numeric column(s) to plot.
  -- Single:   Revenue
  -- Multiple: Revenue,Target,Budget   (pie/donut uses only the first)
  y_columns       STRING    NOT NULL,

  -- yes | no
  legend          STRING    DEFAULT 'yes',

  -- Title text shown above the chart.
  title           STRING,

  -- Colour palette: default | blue | green | orange | red | purple | teal | rainbow
  color_theme     STRING    DEFAULT 'default',

  -- Show data value labels on chart:  yes | no
  show_values     STRING    DEFAULT 'no',

  -- Sort rows before rendering:  none | asc | desc
  sort_order      STRING    DEFAULT 'none',

  -- Output image size in pixels. Keep width_px at or below 600 for email.
  width_px        INT64     DEFAULT 580,
  height_px       INT64     DEFAULT 300,

  updated_at      TIMESTAMP DEFAULT (CURRENT_TIMESTAMP())
)
OPTIONS (
  description =
    'Chart configuration for chart engine. Each row defines one chart. '
    'variable_name links to {{PLACEHOLDER}} tokens in email_body.html_body.'
);


-- ════════════════════════════════════════════════════════════════════
-- (C)  email_output
--      Written to by the Python engine after charts are rendered.
--      Stores the final HTML with embedded base64 charts plus run
--      metadata for auditing and downstream use (e.g. email sender).
-- ════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.email_output`
(
  -- Copied from email_body.email_id.
  email_id          STRING      NOT NULL,

  -- Copied from email_body.sender_email.
  sender_email      STRING,

  -- Copied from email_body.recipient_email.
  recipient_email   STRING,

  -- Copied from email_body.subject.
  subject           STRING,

  -- Complete Outlook-compatible HTML. All {{PLACEHOLDERS}} have been
  -- replaced with <img src="data:image/png;base64,..."> blocks.
  -- Feed this directly into your email sender (SMTP, SendGrid, etc.).
  final_html        STRING      NOT NULL,

  -- Number of {{PLACEHOLDER}} tokens successfully replaced with charts.
  charts_injected   INT64,

  -- Run result:
  --   SUCCESS  all placeholders replaced with charts
  --   PARTIAL  some replaced, some skipped (missing config or empty data)
  --   FAILED   exception occurred; see error_message
  status            STRING,

  -- Empty on SUCCESS.
  -- On PARTIAL: describes which placeholders were skipped and why.
  -- On FAILED:  the exception message.
  error_message     STRING,

  -- UTC timestamp when the Python engine wrote this row.
  processed_at      TIMESTAMP
)

-- Partition by day: fast date-range queries, no full-table scans
PARTITION BY DATE(processed_at)

-- Cluster for fast lookups by email and status
CLUSTER BY email_id, status

OPTIONS (
  description =
    'Final rendered emails produced by the chart engine. '
    'One row per email per run. final_html is the complete Outlook-safe '
    'HTML with all charts embedded as base64 PNG images. '
    'Partitioned by processed_at for cost-efficient querying.',
  require_partition_filter = false
);


-- ════════════════════════════════════════════════════════════════════
-- SAMPLE DATA  —  email_body
-- ════════════════════════════════════════════════════════════════════

INSERT INTO `your_project.your_dataset.email_body`
  (email_id, sender_email, subject, recipient_email, html_body)
VALUES
(
  'E001',
  'reports@company.com',
  'Monthly Performance Report — April 2026',
  'leadership@company.com',
  CONCAT(
    '<h2 style="font-family:Arial;color:#1A2332;">Monthly Performance Report</h2>',
    '<p style="font-family:Arial;">Dear Team,</p>',
    '<p style="font-family:Arial;">Key metrics for this month are below.</p>',
    '<p style="font-family:Arial;font-weight:bold;">Revenue Trend</p>',
    '{{MONTHLY_REVENUE_CHART}}',
    '<p style="font-family:Arial;font-weight:bold;">Regional Split</p>',
    '{{REGION_PIE_CHART}}',
    '<p style="font-family:Arial;font-weight:bold;">Category Performance</p>',
    '{{CATEGORY_BAR_CHART}}',
    '<p style="font-family:Arial;font-weight:bold;">Market Share</p>',
    '{{MARKET_DONUT_CHART}}',
    '<p style="font-family:Arial;">Regards,<br/>Analytics Team</p>'
  )
);


-- ════════════════════════════════════════════════════════════════════
-- SAMPLE DATA  —  chart_config
-- ════════════════════════════════════════════════════════════════════

INSERT INTO `your_project.your_dataset.chart_config`
  (variable_name, chart_type, bq_table, filters,
   x_column, y_columns, legend, title, color_theme,
   show_values, sort_order, width_px, height_px)
VALUES

('MONTHLY_REVENUE_CHART', 'line',
  'your_project.your_dataset.monthly_sales', 'year = 2026',
  'Month', 'Revenue,Target',
  'yes', 'Monthly Revenue vs Target', 'blue', 'no', 'none', 600, 310),

('REGION_PIE_CHART', 'pie',
  'your_project.your_dataset.region_revenue', '',
  'Region', 'Revenue',
  'yes', 'Revenue by Region', 'rainbow', 'yes', 'desc', 500, 350),

('CATEGORY_BAR_CHART', 'bar',
  'your_project.your_dataset.category_quarterly', 'fiscal_year = 2026',
  'Category', 'Q1,Q2,Q3,Q4',
  'yes', 'Quarterly Sales by Category', 'teal', 'yes', 'none', 600, 330),

('MARKET_DONUT_CHART', 'donut',
  'your_project.your_dataset.market_share', 'report_date = ''2026-04-01''',
  'Segment', 'Share',
  'yes', 'Market Share by Segment', 'purple', 'yes', 'desc', 500, 350);


-- ════════════════════════════════════════════════════════════════════
-- VERIFICATION QUERIES
-- Run after the Python engine has written rows to email_output.
-- ════════════════════════════════════════════════════════════════════

-- 1. Today's run summary
SELECT
  email_id,
  sender_email,
  recipient_email,
  subject,
  charts_injected,
  status,
  error_message,
  processed_at
FROM `your_project.your_dataset.email_output`
WHERE DATE(processed_at) = CURRENT_DATE()
ORDER BY processed_at DESC;


-- 2. Run history: count by status per day
SELECT
  DATE(processed_at)   AS run_date,
  status,
  COUNT(*)             AS email_count,
  SUM(charts_injected) AS total_charts_injected
FROM `your_project.your_dataset.email_output`
GROUP BY 1, 2
ORDER BY 1 DESC, 2;


-- 3. Cross-check: find placeholders in html_body with no chart_config row
WITH ph AS (
  SELECT
    email_id,
    REGEXP_EXTRACT_ALL(html_body, r'\{\{([A-Z0-9_]+)\}\}') AS tokens
  FROM `your_project.your_dataset.email_body`
),
expanded AS (
  SELECT email_id, tok
  FROM ph, UNNEST(tokens) AS tok
)
SELECT
  e.email_id,
  e.tok                 AS placeholder,
  c.variable_name       AS config_found,
  IF(c.variable_name IS NULL, 'MISSING', 'OK') AS status
FROM expanded e
LEFT JOIN `your_project.your_dataset.chart_config` c
       ON c.variable_name = e.tok
ORDER BY 1, 2;


-- 4. Latest rendered HTML preview for a specific email
SELECT
  email_id,
  status,
  charts_injected,
  error_message,
  processed_at,
  SUBSTR(final_html, 1, 500) AS html_preview
FROM `your_project.your_dataset.email_output`
WHERE email_id = 'E001'
ORDER BY processed_at DESC
LIMIT 1;
