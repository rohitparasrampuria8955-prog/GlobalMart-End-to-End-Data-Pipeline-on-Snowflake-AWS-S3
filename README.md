# GlobalMart Data Platform (Snowflake)

This is a small end-to-end data pipeline I built on Snowflake to practice the whole medallion
architecture thing — raw files landing from S3, all the way to a proper star schema you can
actually build a dashboard on top of. I used a fictional retailer called **GlobalMart** as the
theme, pulling in three very different kinds of data (POS sales, supplier orders, IoT store
sensors) just so I'd have to deal with CSV, Parquet and JSON in the same project instead of
picking the easy one.

Nothing here is production-grade (single warehouse, no CI/CD, ARN is a placeholder), but the
data flow itself is fully automated once it's running — drop a file in S3 and it walks itself all
the way to the gold layer without me touching anything.

---

## Why I built it this way

I went through a couple of iterations on the gold layer before landing on this design. My first
pass just had flat "report" tables that I `INSERT`ed into manually, which is fine until you
re-run the script and get duplicate rows everywhere. Second pass I over-engineered it — five
dimension tables including a full `dim_date` and hashed customer dimension, which was honestly
more than this dataset needs. What's in here now is the version I actually kept: two shared
dimensions (store, product) and three fact tables, because sales, orders, and sensor readings
are three genuinely different business processes with three different grains — jamming them
into one fact table just means a lot of NULL columns and joins that don't mean anything.

If you're newer to data warehousing and wondering "wait, isn't a star schema supposed to have
one fact table?" — no, that's a common misread. One fact table *per business process* is the
actual rule (this is straight out of Kimball). Multiple fact tables sharing the same dimensions
is called a fact constellation / galaxy schema, and it's the normal shape for anything beyond a
single-process warehouse.

---

## Architecture

```
S3 (pos/*.csv, iot/*.json, parquet/*.parquet)
        │  Snowpipe (auto_ingest)
        ▼
┌─────────────┐   stream    ┌───────────┐   stream    ┌──────────────────┐
│  RAW (bronze)│ ─────────▶ │ STAGING(silver)│ ────▶ │  GOLD (star schema) │
└─────────────┘   + task    └───────────┘   + task    └──────────────────┘
                merge into stg_*             merge into dim_* / fact_*
```

- **raw** — one table per source file type, no transformation, just typed columns +
  `source_file` / `load_ts` for traceability.
- **staging** — cleaned and deduped. This is where I fix things like normalizing payment method
  values, clamping negative quantities to zero, and computing `line_total`.
- **golden_mart** — the actual star schema:
  - `dim_store`, `dim_product` — shared across all three facts
  - `fact_sales` — grain: one row per POS transaction line
  - `fact_orders` — grain: one row per supplier order line
  - `fact_iot_reading` — grain: one row per sensor reading per event
  - a handful of reporting views (`daily_sales_fact`, `gross_margin_fact`,
    `json_iot_pivot_table`, `fact_iot_sensor`) sit on top of the facts — these used to be
    physical tables I `INSERT`ed into, now they're views so they're always current and can't
    duplicate on a re-run.

Everything from raw → staging → gold is wired together with Snowflake **streams + tasks**, so
the whole thing is event-driven off `SYSTEM$STREAM_HAS_DATA()`. No cron-babysitting required.

---

## Data sources

| Source | Format | Landing path | Raw table |
|---|---|---|---|
| POS transactions | CSV | `s3://.../pos/` | `raw.pos_batch` |
| Store IoT sensors (footfall, temp, weight, etc.) | JSON | `s3://.../iot/` | `raw.iot_events_raw` |
| Supplier orders | Parquet | `s3://.../parquet/` | `raw.orders_prq` |

## Prerequisites

- A Snowflake account with `SYSADMIN` (or equivalent) rights to create databases, storage
  integrations, and warehouses
- An S3 bucket you control, with an IAM role Snowflake can assume
- The `COMPUTE_WH` warehouse (or swap the name in the task/pipe DDL for whatever you use)

## Setup

1. Run the storage integration block first, then `DESC STORAGE INTEGRATION s3_integration2` —
   you need the `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` it spits out to update
   the trust policy on your IAM role before Snowflake can actually read the bucket. This trips
   people up the first time, myself included.
2. Swap `storage_aws_role_arn` and `storage_allowed_locations` for your own values.
3. Run the rest of the script top to bottom — it creates schemas, stages, file formats, raw
   tables, pipes, streams, staging tables/tasks, and the gold star schema/tasks, then resumes
   every task at the end.
4. Set up an S3 event notification pointing at the Snowpipe queue (or just drop files and run
   `ALTER PIPE ... REFRESH` manually while testing — that's what I do locally).
5. Drop a file in `pos/`, `iot/`, or `parquet/` and watch it show up in `golden_mart` a few
   minutes later.

## Known limitations / things I'd fix with more time

- Surrogate keys are `HASH(natural_key)` instead of a proper sequence — fine at this scale, but
  it means no real SCD2 history tracking if a store's name or region changes.
- `gross_margin_fact` joins sales to orders on `product_key` only, which is a many-to-many join
  if a product has more than one supplier order in the window — it'll overstate cost/revenue in
  that case. Works fine on the sample data I tested with, just flagging it.
- No dead-letter table for rows that fail to load — a malformed CSV row currently just fails the
  whole `COPY INTO` for that file.
- Single warehouse for everything. If this ever handled real volume I'd split raw ingestion from
  the gold-layer merges so a slow day doesn't queue everything behind itself.

## Tech

Snowflake (storage integrations, Snowpipe, streams, tasks, streams-on-streams for the two-hop
automation), S3.
