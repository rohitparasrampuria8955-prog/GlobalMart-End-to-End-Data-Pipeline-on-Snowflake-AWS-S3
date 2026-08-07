-- first i am creating a database
-- ----------------- database create -------------------------------------------------------------------
create database if not exists Global_mart_db
comment ='globalmart retail data platfrom';

-- ---------------------------------------SCHEMA create ------------------------------------------------
-- schema for the storage interation
create schema if not exists global_mart_db.storage_integrations
comment ='storage interation, file formates,external stage';

-- describe databse
describe database global_mart_db;

-- --------------------------storage integation--------------------------------------------------
create or replace storage integration s3_integration2
type = External_Stage
storage_provider = 's3'
enabled = True
storage_aws_role_arn='arn:aws:iam::153722385756:role/snowflake_user_role'  
storage_allowed_locations=('s3://global-mart-data-lake-practic/');

-- describe storage integration for i am use id and password
desc storage integration s3_integration2;


-- ------------------stage creation --------------------------------------------
-- create stage for data loading
create or replace stage global_mart_db.storage_integrations.s3_stage
url = 's3://global-mart-data-lake-practic/'
storage_integration = s3_integration2;

-- list stage use for check files in s3 bucket
list @global_mart_db.storage_integrations.s3_stage;


-- ----------------------create raw schema ------------------------------------------------
create schema if not exists global_mart_db.raw; --raw for raw data given by company
use schema global_mart_db.storage_integrations;


-- --------------------file formate ------------------------------------------------
-- create file formate for json file
create file format format_json
type='JSON'
STRIP_OUTER_ARRAY=TRUE --it remove outer array bracket
COMMENT='json format for iot event bacth files';


-- create file formate for csv_file
CREATE OR REPLACE FILE FORMAT global_mart_db.storage_integrations.my_csv_format
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER = 1;


-- create parguet format for parquet files
CREATE OR REPLACE FILE FORMAT parquet_format
type = parquet
compression = auto
binary_as_text = false
trim_space = true
replace_invalid_characters = true;

--for storing raw files --
use schema global_mart_db.raw;


-- ---------------------row table creation-----------------------------------------
-- creating table for raw data to be stored json file
create or replace table global_mart_db.raw.iot_events_raw(
    event_id string,
    event_type string,
    store_id string,
    store_name string,
    event_ts timestamp,
    device_id string,
    raw_payload variant,
    source_file string,
    load_at timestamp

);


-- create csv file
create or replace table global_mart_db.raw.pos_batch(
transaction_id varchar(50),
store_id varchar(50),
store_name varchar(50),
store_city varchar(50),
store_region varchar(50),
cashier_id varchar(50),
customer_id varchar(50),
transaction_date date,
transaction_time time,
product_sku varchar(50),
product_name varchar(50),
category varchar(50),
subcategory varchar(50),
quantity int, 
unit_price float,
discount_pct int,
total_amount float,
payment_method varchar(50),
loyalty_points int,
source_file STRING,
load_ts TIMESTAMP_NTZ
);


create or replace table global_mart_db.raw.orders_prq(
    order_id varchar(50),
    order_date datetime,
    store_id varchar(50),
    supplier_id varchar(50),
    store_city varchar(50),
    supplier_name varchar(50),
    supplier_city varchar(50),
    product_sku varchar(50),
    category varchar(50),
    unit_cost float,
    quantity_ordered int,
    quantity_received int,
    order_status varchar(50),
    expected_delivery date,
    actual_delivery date,
    warehouse_id varchar(50),
    lead_time_days int,
    is_late varchar(50),
    file_load_time timestamp,
    source_file varchar
);


-- ------------------pipe creation-------------------------------------------------

-- create a pipline for csv file to automatic loading data
create or replace pipe global_mart_db.storage_integrations.csv_pipe_raw
auto_ingest = TRUE
as
copy into global_mart_db.raw.pos_batch
from(
     select
        t.$1::varchar(50)  as transaction_id,
        t.$2::varchar(50)  as store_id,
        t.$3::varchar(50)  as store_name,
        t.$4::varchar(50)  as store_city,
        t.$5::varchar(50)  as store_region,
        t.$6::varchar(50)  as cashier_id,
        t.$7::varchar(50)  as customer_id,
        t.$8::date         as transaction_date,
        t.$9::time         as transaction_time,
        t.$10::varchar(50) as product_sku,
        t.$11::varchar(50) as product_name,
        t.$12::varchar(50) as category,
        t.$13::varchar(50) as subcategory,
        t.$14::int         as quantity,
        t.$15::float       as unit_price,
        t.$16::int         as discount_pct,
        t.$17::float       as total_amount,
        t.$18::varchar(50) as payment_method,
        t.$19::int         as loyalty_points,

        metadata$filename  as source_file,
        current_timestamp() as load_ts
    from @global_mart_db.storage_integrations.s3_stage/pos/ t
)
file_format=(format_name='global_mart_db.storage_integrations.my_csv_format');


-- create json pipe for automatic data load from s3 to json table
create or replace pipe global_mart_db.storage_integrations.json_pipe_raw
auto_ingest = TRUE
as
copy into global_mart_db.raw.iot_events_raw
from (
    select
        $1:event_id::string,
        $1:event_type::string,
        $1:store_id::string,
        $1:store_name::string,
        $1:timestamp::timestamp,
        $1:device_id::string,
        $1,
        METADATA$FILENAME,
        current_timestamp()
    from @global_mart_db.storage_integrations.s3_stage/iot/)
file_format=(format_name='global_mart_db.storage_integrations.format_json');


create or replace pipe global_mart_db.storage_integrations.parquet_pipe_raw
auto_ingest = TRUE
as
copy into global_mart_db.raw.orders_prq
from (
    select
        $1:order_id::STRING,
        $1:order_date::DATETIME,
        $1:store_id::STRING,
        $1:supplier_id::STRING,  
        $1:store_city::STRING,
        $1:supplier_name::STRING,
        $1:supplier_city::STRING,
        $1:product_sku::STRING,
        $1:category::STRING,
        $1:unit_cost::FLOAT,
        $1:quantity_ordered::INT,
        $1:quantity_received::INT,
        $1:order_status::STRING,
        $1:expected_delivery::DATE,
        $1:actual_delivery::DATE,
        $1:warehouse_id::STRING,
        $1:lead_time_days::INT,
        $1:is_late::STRING,
        CURRENT_TIMESTAMP(),
        METADATA$FILENAME
    from @global_mart_db.storage_integrations.s3_stage/parquet/)
file_format=(format_name='global_mart_db.storage_integrations.parquet_format');
-- -------------------------------- raw streams -------------------------------------------------
create or replace stream global_mart_db.storage_integrations.iot_event_raw_filter_stram on table
global_mart_db.raw.iot_events_raw
append_only= TRUE ;


create or replace stream global_mart_db.storage_integrations.pos_batch_stream on table
global_mart_db.raw.pos_batch
append_only= TRUE;


create or replace stream global_mart_db.storage_integrations.orders_filter_stram on table
global_mart_db.raw.orders_prq;


-- ---------------------silver schema---------------------------------------------

create or replace schema global_mart_db.staging;

-- -----------------------silver_table's-------------------------------------------
create or replace table global_mart_db.staging.stg_csv_transaction(
    transaction_id varchar(50),
    store_id varchar(50),
    store_name varchar(50),
    store_city varchar(50),
    store_region varchar(50),
    cashier_id varchar(50),
    customer_id varchar(50),

    -- transaction timestamp
    transaction_ts timestamp_ntz,

    -- product details
    product_sku varchar(50),
    product_name varchar(50),
    category varchar(50),
    subcategory varchar(50),

    -- sales details
    quantity int,
    unit_price float,
    discount_pct int,
    line_total float,

    -- payment & loyalty
    payment_method varchar(10),
    loyalty_points int,

    -- audit columns
    source_file string,
    load_ts timestamp_ntz
);


 create or replace table global_mart_db.staging.stg_json_sensors (
    event_id string,
    event_type string,
    store_id string,
    store_name string,
    event_ts timestamp,
    device_id string,
    firmware string,
    battery_pct number,
    store_floor number,
    sensor_name string,
    sensor_value float,
    sensor_unit string,
    source_file string,
    load_at timestamp
);


create or replace table global_mart_db.staging.stg_orders_parquet(
    order_id varchar(50),
    order_date datetime,
    store_id varchar(50),
    supplier_id varchar(50),
    store_city varchar(50),
    supplier_name varchar(50),
    supplier_city varchar(50),
    product_sku varchar(50),
    category varchar(50),
    unit_cost float,
    quantity_ordered int,
    quantity_received int,
    order_status varchar(50),
    expected_delivery date,
    actual_delivery date,
    warehouse_id varchar(50),
    lead_time_days int,
    is_late varchar(50),
    file_load_time timestamp,
    source_file varchar
    
);


-- ------------------- (merge)----------------------------------------

CREATE OR REPLACE TASK orders_task
WAREHOUSE = COMPUTE_WH
SCHEDULE = '5 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('global_mart_db.storage_integrations.pos_batch_stream')
as
merge into global_mart_db.staging.stg_csv_transaction as t
using (
    select
        -- transaction details
        transaction_id::varchar(50) as transaction_id,
        store_id::varchar(50) as store_id,
        store_name::varchar(50) as store_name,
        store_city::varchar(50) as store_city,
        store_region::varchar(50) as store_region,
        cashier_id::varchar(50) as cashier_id,
        customer_id::varchar(50) as customer_id,
        to_timestamp_ntz(
            transaction_date || ' ' || transaction_time
        )::timestamp_ntz as transaction_ts,
        product_sku::varchar(50) as product_sku,
        product_name::varchar(50) as product_name,
        category::varchar(50) as category,
        subcategory::varchar(50) as subcategory,   

        GREATEST(COALESCE(quantity,0), 0)::INT as quantity,
        GREATEST(COALESCE(unit_price,0), 0)::FLOAT as unit_price,
        GREATEST(COALESCE(discount_pct,0), 0)::INT as discount_pct,
        
        (GREATEST(COALESCE(QUANTITY,0),0)*GREATEST(COALESCE(UNIT_PRICE,0),0)*
        (1 - GREATEST(COALESCE(DISCOUNT_PCT,0),0)/100.0))::FLOAT AS line_total,

        (case when lower(payment_method) = 'credit card' then 'cc' 
        when lower(payment_method) = 'debit card' then 'dc'
        else lower(payment_method) end)::varchar(10) as payment_method,
        
        loyalty_points::int as loyalty_points,
        source_file::string as source_file,
        load_ts::timestamp_ntz as load_ts,
        metadata$action,
        metadata$isupdate
    from global_mart_db.storage_integrations.pos_batch_stream
) as s
on t.transaction_id = s.transaction_id
WHEN MATCHED
    AND s.metadata$action = 'INSERT'
    AND s.metadata$isupdate = TRUE

THEN UPDATE SET

    t.store_id = s.store_id,
    t.store_name = s.store_name,
    t.store_city = s.store_city,
    t.store_region = s.store_region,
    t.cashier_id = s.cashier_id,
    t.customer_id = s.customer_id,
    t.transaction_ts = s.transaction_ts,
    t.product_sku = s.product_sku,
    t.product_name = s.product_name,
    t.category = s.category,
    t.subcategory = s.subcategory,
    t.quantity = s.quantity,
    t.unit_price = s.unit_price,
    t.discount_pct = s.discount_pct,
    t.line_total = s.line_total,
    t.payment_method = s.payment_method,
    t.loyalty_points = s.loyalty_points,
    t.source_file = s.source_file,
    t.load_ts = s.load_ts

WHEN MATCHED
    AND s.metadata$action = 'DELETE'
    AND s.metadata$isupdate = FALSE

THEN DELETE

WHEN NOT MATCHED
    AND s.metadata$action = 'INSERT'

THEN INSERT (

    transaction_id,
    store_id,
    store_name,
    store_city,
    store_region,
    cashier_id,
    customer_id,
    transaction_ts,
    product_sku,
    product_name,
    category,
    subcategory,
    quantity,
    unit_price,
    discount_pct,
    line_total,
    payment_method,
    loyalty_points,
    source_file,
    load_ts

)

VALUES (

    s.transaction_id,
    s.store_id,
    s.store_name,
    s.store_city,
    s.store_region,
    s.cashier_id,
    s.customer_id,
    s.transaction_ts,
    s.product_sku,
    s.product_name,
    s.category,
    s.subcategory,
    s.quantity,
    s.unit_price,
    s.discount_pct,
    s.line_total,
    s.payment_method,
    s.loyalty_points,
    s.source_file,
    s.load_ts

);


CREATE OR REPLACE TASK json_sensor_task
WAREHOUSE = COMPUTE_WH
SCHEDULE = '5 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('global_mart_db.storage_integrations.iot_event_raw_filter_stram')
as
merge into global_mart_db.staging.stg_json_sensors as t 
using(
    SELECT
        event_id,
        event_type,
        store_id,
        store_name,
        event_ts,
        device_id,
        raw_payload:metadata.firmware::STRING  as firmware,
        raw_payload:metadata.battery_pct::INT  as battery_pct,
        raw_payload:metadata.store_floor::INT   as store_floor,
        f.value:sensor::STRING  as sensor_name,
        f.value:value::FLOAT  as sensor_value,
        f.value:unit::STRING   as sensor_unit,
        source_file,
        load_at,
        CURRENT_TIMESTAMP()  as processed_ts
    FROM global_mart_db.storage_integrations.iot_event_raw_filter_stram,
         LATERAL FLATTEN(input => raw_payload:readings) as f
) AS src
ON t.event_id = src.event_id
AND t.sensor_name = src.sensor_name
WHEN NOT MATCHED THEN
INSERT (
    event_id, event_type, store_id, store_name, event_ts, device_id, firmware, battery_pct,
    store_floor, sensor_name, sensor_value, sensor_unit,  source_file, load_at
)
VALUES (
    src.event_id,  src.event_type, src.store_id, src.store_name, src.event_ts, src.device_id, src.firmware,
    src.battery_pct, src.store_floor, src.sensor_name, src.sensor_value, src.sensor_unit, src.source_file,
    src.load_at
);


CREATE OR REPLACE TASK parquet_orders_task
WAREHOUSE = COMPUTE_WH
SCHEDULE = '5 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('global_mart_db.storage_integrations.orders_filter_stram')
as
merge into global_mart_db.staging.stg_orders_parquet as s
using global_mart_db.storage_integrations.orders_filter_stram as b
ON s.order_id = b.order_id
WHEN MATCHED THEN
    UPDATE SET
        s.order_date = b.order_date,
        s.store_id = b.store_id,
        s.supplier_id = b.supplier_id,
        s.store_city = b.store_city,
        s.supplier_name = b.supplier_name,
        s.supplier_city = b.supplier_city,
        s.product_sku = b.product_sku,
        s.category = b.category,
        s.unit_cost = b.unit_cost,
        s.quantity_ordered = b.quantity_ordered,
        s.quantity_received = b.quantity_received,
        s.order_status = b.order_status,
        s.expected_delivery = b.expected_delivery,
        s.actual_delivery = b.actual_delivery,
        s.warehouse_id = b.warehouse_id,
        s.lead_time_days = b.lead_time_days,
        s.is_late = b.is_late,
        s.file_load_time = b.file_load_time,
        s.source_file = b.source_file

WHEN NOT MATCHED THEN
INSERT (
    order_id,
    order_date,
    store_id,
    supplier_id,
    store_city,
    supplier_name,
    supplier_city,
    product_sku,
    category,
    unit_cost,
    quantity_ordered,
    quantity_received,
    order_status,
    expected_delivery,
    actual_delivery,
    warehouse_id,
    lead_time_days,
    is_late,
    file_load_time,
    source_file
)
VALUES (
    b.order_id,
    b.order_date,
    b.store_id,
    b.supplier_id,
    b.store_city,
    b.supplier_name,
    b.supplier_city,
    b.product_sku,
    b.category,
    b.unit_cost,
    b.quantity_ordered,
    b.quantity_received,
    b.order_status,
    b.expected_delivery,
    b.actual_delivery,
    b.warehouse_id,
    b.lead_time_days,
    b.is_late,
    b.file_load_time,
    b.source_file
);


-- ---------------------------------- silver_streams ----------------------------------
create or replace stream global_mart_db.storage_integrations.stg_csv_transaction_stream
on table global_mart_db.staging.stg_csv_transaction;

create or replace stream global_mart_db.storage_integrations.stg_json_sensors_stream
on table global_mart_db.staging.stg_json_sensors
append_only = TRUE;

create or replace stream global_mart_db.storage_integrations.stg_orders_parquet_stream
on table global_mart_db.staging.stg_orders_parquet;


-- --------------------------golden schema ----------------------------------------
create or replace schema global_mart_db.golden_mart;

-- --------------------------golden - star schema -----------------------------------------
-- dimensions (store aur product dono facts me common hai isliye alag nikala)
create or replace table global_mart_db.golden_mart.dim_store(
    store_key   number,
    store_id    varchar(50),
    store_name  varchar(50),
    store_city  varchar(50),
    store_region varchar(50),
    updated_at  timestamp_ntz,
    primary key(store_key)
);

create or replace table global_mart_db.golden_mart.dim_product(
    product_key  number,
    product_sku  varchar(50),
    product_name varchar(50),
    category     varchar(50),
    subcategory  varchar(50),
    updated_at   timestamp_ntz,
    primary key(product_key)
);

-- fact table 
create or replace table global_mart_db.golden_mart.fact_sales(
    transaction_id varchar(50),
    store_key      number,
    product_key    number,
    cashier_id     varchar(50),
    customer_id    varchar(50),
    transaction_ts timestamp_ntz,
    quantity       int,
    unit_price     float,
    discount_pct   int,
    line_total     float,
    payment_method varchar(10),
    loyalty_points int,
    source_file    string,
    load_ts        timestamp_ntz,
    primary key(transaction_id)
);

create or replace table global_mart_db.golden_mart.fact_orders(
    order_id           varchar(50),
    store_key          number,
    product_key        number,
    supplier_id        varchar(50),
    supplier_name      varchar(50),
    supplier_city      varchar(50),
    order_date         datetime,
    warehouse_id       varchar(50),
    unit_cost          float,
    quantity_ordered   int,
    quantity_received  int,
    order_status       varchar(50),
    expected_delivery  date,
    actual_delivery    date,
    lead_time_days     int,
    is_late            varchar(50),
    source_file        varchar,
    load_ts            timestamp,
    primary key(order_id)
);

create or replace table global_mart_db.golden_mart.fact_iot_reading(
    event_id     string,
    sensor_name  string,
    store_key    number,
    event_ts     timestamp,
    device_id    string,
    firmware     string,
    battery_pct  number,
    store_floor  number,
    sensor_value float,
    sensor_unit  string,
    source_file  string,
    load_at      timestamp,
    primary key(event_id, sensor_name)
);


CREATE OR REPLACE TASK task_load_dim_store
WAREHOUSE = COMPUTE_WH
AFTER orders_task
WHEN SYSTEM$STREAM_HAS_DATA('global_mart_db.storage_integrations.stg_csv_transaction_stream')
AS
merge into global_mart_db.golden_mart.dim_store as t
using (
    select distinct
        hash(store_id) as store_key, store_id, store_name, store_city, store_region
    from global_mart_db.storage_integrations.stg_csv_transaction_stream
) as s
on t.store_key = s.store_key
when matched then update set
    t.store_name = s.store_name, t.store_city = s.store_city, t.store_region = s.store_region,
    t.updated_at = current_timestamp()
when not matched then insert(store_key, store_id, store_name, store_city, store_region, updated_at)
values(s.store_key, s.store_id, s.store_name, s.store_city, s.store_region, current_timestamp());


CREATE OR REPLACE TASK task_load_dim_product
WAREHOUSE = COMPUTE_WH
AFTER orders_task
WHEN SYSTEM$STREAM_HAS_DATA('global_mart_db.storage_integrations.stg_csv_transaction_stream')
AS
merge into global_mart_db.golden_mart.dim_product as t
using (
    select distinct
        hash(product_sku) as product_key, product_sku, product_name, category, subcategory
    from global_mart_db.storage_integrations.stg_csv_transaction_stream
) as s
on t.product_key = s.product_key
when matched then update set
    t.product_name = s.product_name, t.category = s.category, t.subcategory = s.subcategory,
    t.updated_at = current_timestamp()
when not matched then insert(product_key, product_sku, product_name, category, subcategory, updated_at)
values(s.product_key, s.product_sku, s.product_name, s.category, s.subcategory, current_timestamp());


CREATE OR REPLACE TASK task_load_fact_sales
WAREHOUSE = COMPUTE_WH
AFTER task_load_dim_store, task_load_dim_product
AS
merge into global_mart_db.golden_mart.fact_sales as t
using (
    select
        transaction_id, hash(store_id) as store_key, hash(product_sku) as product_key,
        cashier_id, customer_id, transaction_ts, quantity, unit_price, discount_pct,
        line_total, payment_method, loyalty_points, source_file, load_ts
    from global_mart_db.staging.stg_csv_transaction
) as s
on t.transaction_id = s.transaction_id
when matched then update set
    t.store_key = s.store_key, t.product_key = s.product_key, t.cashier_id = s.cashier_id,
    t.customer_id = s.customer_id, t.transaction_ts = s.transaction_ts, t.quantity = s.quantity,
    t.unit_price = s.unit_price, t.discount_pct = s.discount_pct, t.line_total = s.line_total,
    t.payment_method = s.payment_method, t.loyalty_points = s.loyalty_points,
    t.source_file = s.source_file, t.load_ts = s.load_ts
when not matched then insert(
    transaction_id, store_key, product_key, cashier_id, customer_id, transaction_ts, quantity,
    unit_price, discount_pct, line_total, payment_method, loyalty_points, source_file, load_ts
) values (
    s.transaction_id, s.store_key, s.product_key, s.cashier_id, s.customer_id, s.transaction_ts,
    s.quantity, s.unit_price, s.discount_pct, s.line_total, s.payment_method, s.loyalty_points,
    s.source_file, s.load_ts
);


CREATE OR REPLACE TASK task_load_fact_orders
WAREHOUSE = COMPUTE_WH
AFTER parquet_orders_task
WHEN SYSTEM$STREAM_HAS_DATA('global_mart_db.storage_integrations.stg_orders_parquet_stream')
AS
merge into global_mart_db.golden_mart.fact_orders as t
using (
    select
        order_id, hash(store_id) as store_key, hash(product_sku) as product_key,
        supplier_id, supplier_name, supplier_city, order_date, warehouse_id, unit_cost,
        quantity_ordered, quantity_received, order_status, expected_delivery, actual_delivery,
        lead_time_days, is_late, source_file, file_load_time
    from global_mart_db.storage_integrations.stg_orders_parquet_stream
) as s
on t.order_id = s.order_id
when matched then update set
    t.store_key = s.store_key, t.product_key = s.product_key, t.supplier_id = s.supplier_id,
    t.supplier_name = s.supplier_name, t.supplier_city = s.supplier_city, t.order_date = s.order_date,
    t.warehouse_id = s.warehouse_id, t.unit_cost = s.unit_cost, t.quantity_ordered = s.quantity_ordered,
    t.quantity_received = s.quantity_received, t.order_status = s.order_status,
    t.expected_delivery = s.expected_delivery, t.actual_delivery = s.actual_delivery,
    t.lead_time_days = s.lead_time_days, t.is_late = s.is_late, t.source_file = s.source_file,
    t.load_ts = s.file_load_time
when not matched then insert(
    order_id, store_key, product_key, supplier_id, supplier_name, supplier_city, order_date,
    warehouse_id, unit_cost, quantity_ordered, quantity_received, order_status, expected_delivery,
    actual_delivery, lead_time_days, is_late, source_file, load_ts
) values (
    s.order_id, s.store_key, s.product_key, s.supplier_id, s.supplier_name, s.supplier_city,
    s.order_date, s.warehouse_id, s.unit_cost, s.quantity_ordered, s.quantity_received,
    s.order_status, s.expected_delivery, s.actual_delivery, s.lead_time_days, s.is_late,
    s.source_file, s.file_load_time
);


CREATE OR REPLACE TASK task_load_fact_iot
WAREHOUSE = COMPUTE_WH
AFTER json_sensor_task
WHEN SYSTEM$STREAM_HAS_DATA('global_mart_db.storage_integrations.stg_json_sensors_stream')
AS
merge into global_mart_db.golden_mart.fact_iot_reading as t
using (
    select
        event_id, sensor_name, hash(store_id) as store_key, event_ts, device_id, firmware,
        battery_pct, store_floor, sensor_value, sensor_unit, source_file, load_at
    from global_mart_db.storage_integrations.stg_json_sensors_stream
) as s
on t.event_id = s.event_id and t.sensor_name = s.sensor_name
when not matched then insert(
    event_id, sensor_name, store_key, event_ts, device_id, firmware, battery_pct, store_floor,
    sensor_value, sensor_unit, source_file, load_at
) values (
    s.event_id, s.sensor_name, s.store_key, s.event_ts, s.device_id, s.firmware, s.battery_pct,
    s.store_floor, s.sensor_value, s.sensor_unit, s.source_file, s.load_at
);


-- ---------------------------------- resume all task ----------------------------------
alter task global_mart_db.golden_mart.task_load_fact_sales resume;
alter task global_mart_db.golden_mart.task_load_fact_orders resume;
alter task global_mart_db.golden_mart.task_load_fact_iot resume;
alter task global_mart_db.golden_mart.task_load_dim_store resume;
alter task global_mart_db.golden_mart.task_load_dim_product resume;
alter task global_mart_db.storage_integrations.orders_task resume;
alter task global_mart_db.storage_integrations.json_sensor_task resume;
alter task global_mart_db.storage_integrations.parquet_orders_task resume;

show tasks in database global_mart_db;

-- businees ready data 

create or replace view global_mart_db.golden_mart.daily_sales_fact as
select
    date(f.transaction_ts) as report_date,
    st.store_id,
    st.store_city,
    st.store_name,
    p.category,
    sum(f.line_total) as total_revenue_generated,
    sum(f.quantity)/count(distinct f.transaction_id) as avg_cart_size,
    count(distinct f.customer_id) as total_unique_customer,
    current_date() as date_updated
from global_mart_db.golden_mart.fact_sales f
join global_mart_db.golden_mart.dim_store st on f.store_key = st.store_key
join global_mart_db.golden_mart.dim_product p on f.product_key = p.product_key
group by date(f.transaction_ts), st.store_id, st.store_city, st.store_name, p.category;


create or replace view global_mart_db.golden_mart.gross_margin_fact as
select 
    st.store_name,
    p.category,
    sum(f.line_total) as total_revenue_generated,
    sum(o.unit_cost*f.quantity) as total_cost_generated,
    sum(f.line_total) - sum(o.unit_cost*f.quantity) as gross_profit,
    DIV0NULL((sum(f.line_total) - sum(o.unit_cost*f.quantity)), sum(f.line_total))*100 as gross_margin_precentage,
    sum(f.quantity) as no_of_units_sold,
    count(distinct f.transaction_id) as total_unit_orders,
    current_date() as date_updated
from global_mart_db.golden_mart.fact_sales as f
join global_mart_db.golden_mart.dim_store st on f.store_key = st.store_key
join global_mart_db.golden_mart.dim_product p on f.product_key = p.product_key
join global_mart_db.golden_mart.fact_orders as o on o.product_key = f.product_key
group by st.store_name, p.category;


create or replace view global_mart_db.golden_mart.json_iot_pivot_table as
select 
 st.store_id, st.store_name,
 avg( case when r.sensor_name='footfall' then r.sensor_value end) as average_football, 
 avg( case when r.sensor_name='weight_kg' then r.sensor_value end) as average_weight,
 avg( case when r.sensor_name='temp_c' then r.sensor_value end) as avg_temp 
from global_mart_db.golden_mart.fact_iot_reading r
join global_mart_db.golden_mart.dim_store st on r.store_key = st.store_key
group by st.store_id, st.store_name;


create or replace view global_mart_db.golden_mart.fact_iot_sensor as 
select c.*, j.average_football, j.average_weight, j.avg_temp
from global_mart_db.golden_mart.daily_sales_fact as c
join global_mart_db.golden_mart.json_iot_pivot_table as j
on c.store_id=j.store_id and c.store_name=j.store_name;
