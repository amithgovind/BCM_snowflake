-- Create multiple databases representing different business domains
CREATE DATABASE IF NOT EXISTS sales_db COMMENT = 'Sales and revenue data';
CREATE DATABASE IF NOT EXISTS operations_db COMMENT = 'Operational metrics and processes';
CREATE DATABASE IF NOT EXISTS finance_db COMMENT = 'Financial data and reporting';
CREATE DATABASE IF NOT EXISTS marketing_db COMMENT = 'Marketing and customer analytics';
CREATE DATABASE IF NOT EXISTS snowpipe_db COMMENT = 'snowpipe through s3';

-- Create schemas within each database
CREATE SCHEMA IF NOT EXISTS sales_db.raw COMMENT = 'Raw sales data';
CREATE SCHEMA IF NOT EXISTS sales_db.staging COMMENT = 'Staged sales transformations';
CREATE SCHEMA IF NOT EXISTS sales_db.analytics COMMENT = 'Sales analytics layer';

CREATE SCHEMA IF NOT EXISTS operations_db.logs COMMENT = 'Operational logs';
CREATE SCHEMA IF NOT EXISTS operations_db.metrics COMMENT = 'Operational metrics';

CREATE SCHEMA IF NOT EXISTS finance_db.general_ledger COMMENT = 'GL data';
CREATE SCHEMA IF NOT EXISTS finance_db.reporting COMMENT = 'Financial reports';

CREATE SCHEMA IF NOT EXISTS marketing_db.campaigns COMMENT = 'Campaign data';
CREATE SCHEMA IF NOT EXISTS marketing_db.customer_360 COMMENT = 'Customer analytics';




-- 1. STANDARD TABLES with business data
CREATE TABLE IF NOT EXISTS sales_db.raw.customers2 (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100),
    country VARCHAR(50),
    segment VARCHAR(20),
    created_date DATE,
    created_timestamp TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS sales_db.raw.transactions (
    transaction_id INT PRIMARY KEY,
    customer_id INT,
    amount DECIMAL(10,2),
    transaction_date DATE,
    product_category VARCHAR(50),
    FOREIGN KEY (customer_id) REFERENCES sales_db.raw.customers(customer_id)
);


-- Create storage integration with the NEW IAM role ARN
CREATE or replace STORAGE INTEGRATION s3_integration_new_account
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'xxx'
    STORAGE_ALLOWED_LOCATIONS = ('s3://snowflake-testing-ag-bucket/');
--for security reasons, if you create a new storage integration (or recreate an existing storage integration using the CREATE OR REPLACE STORAGE INTEGRATION syntax) without specifying an external ID, the new integration has a different external ID and can’t resolve the trust relationship unless you update the trust policy.

SHOW INTEGRATIONS;
DESC STORAGE INTEGRATION s3_integration_new_account;



CREATE or replace STAGE snowpipe_db.public.s3_stage
    URL = 's3://snowflake-testing-ag-bucket/snowpipe_s3_test/'
    STORAGE_INTEGRATION = s3_integration_new_account;

    



CREATE or replace PIPE snowpipe_db.public.pipe_s3_test
  AUTO_INGEST = TRUE
  AWS_SNS_TOPIC='yyy'
  AS
    COPY INTO snowpipe_db.public.pipe_s3_test_table
      FROM @snowpipe_db.public.s3_stage
      FILE_FORMAT = (type = 'JSON')
      MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

SELECT SYSTEM$PIPE_STATUS('snowpipe_db.public.pipe_s3_test');

DESC STAGE snowpipe_db.public.s3_stage;
LIST @snowpipe_db.public.s3_stage;


DESC TABLE snowpipe_db.public.pipe_s3_test_table;


SELECT * FROM snowpipe_db.public.pipe_s3_test_table;


DESC PIPE snowpipe_db.public.pipe_s3_test;


SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.PIPES
WHERE PIPE_NAME = 'PIPE_S3_TEST';



SELECT 
  FILE_NAME,
  STATUS,
  ROW_COUNT,
  ERROR_COUNT,
  LAST_LOAD_TIME
FROM TABLE INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME=>'pipe_s3_test_table',
  LIMIT=>100
)
ORDER BY LAST_LOAD_TIME DESC;





















CREATE FILE FORMAT IF NOT EXISTS sales_db.raw.csv_format
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1;

CREATE EXTERNAL TABLE IF NOT EXISTS sales_db.raw.external_orders (
    order_id INT AS (VALUE:order_id::INT),
    customer_id INT AS (VALUE:customer_id::INT),
    order_date DATE AS (VALUE:order_date::DATE),
    order_amount DECIMAL(10,2) AS (VALUE:order_amount::DECIMAL(10,2))
)
LOCATION = @sales_db.raw.s3_stage/orders/
FILE_FORMAT = (FORMAT_NAME = sales_db.raw.csv_format);

-- 3. DYNAMIC TABLES (auto-refreshing, creates dangling references)
CREATE DYNAMIC TABLE sales_db.analytics.customer_summary
TARGET_LAG = '1 day'
WAREHOUSE = compute_wh
AS
SELECT 
    c.customer_id,
    c.customer_name,
    c.country,
    COUNT(t.transaction_id) as total_transactions,
    SUM(t.amount) as total_spent,
    MAX(t.transaction_date) as last_transaction_date
FROM sales_db.raw.customers c
LEFT JOIN sales_db.raw.transactions t ON c.customer_id = t.customer_id
GROUP BY c.customer_id, c.customer_name, c.country;

-- 4. VIEWS (simple views)
CREATE VIEW IF NOT EXISTS sales_db.analytics.high_value_customers AS
SELECT 
    customer_id,
    customer_name,
    total_transactions,
    total_spent
FROM sales_db.analytics.customer_summary
WHERE total_spent > 10000;

-- 5. MATERIALIZED VIEW (creates dependencies)
CREATE MATERIALIZED VIEW IF NOT EXISTS sales_db.analytics.monthly_sales_summary AS
SELECT 
    DATE_TRUNC('MONTH', transaction_date) as month,
    product_category,
    COUNT(*) as transaction_count,
    SUM(amount) as total_sales
FROM sales_db.raw.transactions
GROUP BY DATE_TRUNC('MONTH', transaction_date), product_category;


-- 6. ICEBERG TABLES (modern table format - good for testing) Note: did not work
CREATE ICEBERG TABLE IF NOT EXISTS operations_db.metrics.performance_metrics (
    metric_id INT PRIMARY KEY,
    metric_name STRING,
    metric_value FLOAT,
    measurement_date DATE,
    measurement_time TIMESTAMP_NTZ
)
PARTITION BY (measurement_date)
STORAGE_INTEGRATION = 's3_integration_new_account'
BASE_LOCATION = 's3://snowflake-testing-ag-bucket/iceberg_data/';

-- 7. TEMP AND TRANSIENT TABLES (for testing edge cases)
CREATE TEMPORARY TABLE temp_calculations AS
SELECT customer_id, COUNT(*) as freq FROM sales_db.raw.transactions GROUP BY customer_id;

CREATE TRANSIENT TABLE operations_db.logs.session_logs (
    session_id VARCHAR(50),
    user_name VARCHAR(100),
    login_time TIMESTAMP_NTZ,
    logout_time TIMESTAMP_NTZ
);




CREATE OR REPLACE PROCEDURE sales_db.analytics.refresh_customer_metrics()
RETURNS TABLE (status VARCHAR)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests')
HANDLER = 'refresh_metrics'
AS
$$
def refresh_metrics(session):
    try:
        session.sql("""
            INSERT INTO operations_db.logs.session_logs 
            VALUES (UUID_STRING(), 'ADMIN', CURRENT_TIMESTAMP, NULL)
        """).collect()
        return [("Metrics refreshed successfully",)]
    except Exception as e:
        return [("Error: " + str(e),)]
$$;



CREATE OR REPLACE FUNCTION sales_db.analytics.calculate_customer_tier(spent FLOAT)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Categorizes customers into tiers based on lifetime spending'
AS
$$
    CASE 
        WHEN spent > 50000 THEN 'Platinum'
        WHEN spent > 20000 THEN 'Gold'
        WHEN spent > 5000 THEN 'Silver'
        ELSE 'Bronze'
    END
$$;




CREATE OR REPLACE FUNCTION get_customer_transactions(cust_id INT)
RETURNS TABLE (transaction_id INT, amount DECIMAL, transaction_date DATE)
LANGUAGE SQL
AS
$$
  SELECT transaction_id, amount, transaction_date 
  FROM sales_db.raw.transactions 
  WHERE customer_id = cust_id
  ORDER BY transaction_date DESC
$$;





-- 2. NOTIFICATION INTEGRATION (for event-driven pipelines)
  CREATE NOTIFICATION INTEGRATION sns_integration_test
  ENABLED = TRUE
  DIRECTION = OUTBOUND
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AWS_SNS
  AWS_SNS_TOPIC_ARN = 'xxx'
  AWS_SNS_ROLE_ARN = 'yyy';

  -- Create a test task that will fail to test notification integration
CREATE OR REPLACE TASK test_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 0 * * * UTC'
  ERROR_INTEGRATION = sns_integration_test
AS
  SELECT * FROM nonexistent_table;  -- This table doesn't exist, so it will fail
  EXECUTE TASK test_task;

  DESC NOTIFICATION INTEGRATION sns_integration_test;

















USE ROLE SECURITYADMIN;

-- Create a database and schema for your network rules
CREATE DATABASE IF NOT EXISTS security_db;
CREATE SCHEMA IF NOT EXISTS security_db.network_rules;

-- Create a network rule to allow your current IP (replace with your actual IP)
CREATE NETWORK RULE security_db.network_rules.allow_my_ip
  TYPE = IPV4
  MODE = INGRESS
  VALUE_LIST = ('89.0.49.0/16')  -- Replace with YOUR IP address
  COMMENT = 'Allow my current IP address';



-- Create a network rule to block specific IPs if needed
CREATE NETWORK RULE security_db.network_rules.block_suspicious_ips
  TYPE = IPV4
  MODE = INGRESS
  VALUE_LIST = ('198.51.100.50/32')  -- Example blocked IP
  COMMENT = 'Block suspicious IP addresses';



-- Create network policy using network rules
CREATE NETWORK POLICY my_network_policy
  ALLOWED_NETWORK_RULE_LIST = ('security_db.network_rules.allow_my_ip')
  BLOCKED_NETWORK_RULE_LIST = ('security_db.network_rules.block_suspicious_ips')
  COMMENT = 'Production network policy for account access';


SHOW NETWORK RULES;


DESC NETWORK RULE security_db.network_rules.allow_my_ip;
SHOW NETWORK POLICIES;














-- Recreate as a standard share (for within organization)
CREATE SHARE sales_analytics_share
  COMMENT = 'Share for analytics partners';

-- Grant permissions
GRANT USAGE ON DATABASE sales_db TO SHARE sales_analytics_share;
GRANT USAGE ON SCHEMA sales_db.analytics TO SHARE sales_analytics_share;
GRANT SELECT ON DYNAMIC TABLE SALES_DB.ANALYTICS.CUSTOMER_SUMMARY 
  TO SHARE sales_analytics_share;

-- Now add accounts from your organization
ALTER SHARE sales_analytics_share 
  ADD ACCOUNTS = LGOPGQY.TEST_ACCOUNT_FRA;














-- Production warehouse
CREATE WAREHOUSE IF NOT EXISTS prod_wh
  WAREHOUSE_SIZE = SMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = FALSE;

-- Compute warehouse
CREATE WAREHOUSE IF NOT EXISTS compute_wh
  WAREHOUSE_SIZE = MEDIUM
  AUTO_SUSPEND = 300;

-- Analytics warehouse
CREATE WAREHOUSE IF NOT EXISTS analytics_wh
  WAREHOUSE_SIZE = LARGE
  AUTO_SUSPEND = 600;






  -- Create custom roles
CREATE ROLE IF NOT EXISTS sales_analyst;
CREATE ROLE IF NOT EXISTS finance_admin;
CREATE ROLE IF NOT EXISTS operations_viewer;

-- Grant database-level access
GRANT USAGE ON DATABASE sales_db TO ROLE sales_analyst;
GRANT USAGE ON DATABASE finance_db TO ROLE finance_admin;
GRANT USAGE ON DATABASE operations_db TO ROLE operations_viewer;

-- Grant schema-level access
GRANT USAGE ON SCHEMA sales_db.analytics TO ROLE sales_analyst;
GRANT USAGE ON SCHEMA finance_db.reporting TO ROLE finance_admin;

-- Grant object-level access
GRANT SELECT ON ALL TABLES IN SCHEMA sales_db.analytics TO ROLE sales_analyst;
GRANT SELECT, UPDATE ON ALL TABLES IN SCHEMA finance_db.reporting TO ROLE finance_admin;

-- Create users and assign roles

GRANT ROLE sales_analyst TO USER JOHN_DOE;
GRANT ROLE sales_analyst TO USER PRIMARY_ACCOUNT;

show users;


-- Populate customers
INSERT INTO sales_db.raw.customers VALUES
(1, 'Acme Corp', 'USA', 'Enterprise', '2023-01-15', '2023-01-15 10:00:00'),
(2, 'TechStart Inc', 'Germany', 'Mid-Market', '2023-02-20', '2023-02-20 14:30:00'),
(3, 'Global Solutions', 'UK', 'Enterprise', '2023-03-10', '2023-03-10 09:15:00');

-- Populate transactions
INSERT INTO sales_db.raw.transactions VALUES
(101, 1, 15000.00, '2024-01-10', 'Software', 1),
(102, 1, 8500.00, '2024-02-15', 'Services', 1),
(103, 2, 5000.00, '2024-01-20', 'Software', 1),
(104, 3, 25000.00, '2024-02-28', 'Enterprise', 1);



-- Create a task that runs procedures
CREATE TASK IF NOT EXISTS sales_db.analytics.refresh_customer_data
  WAREHOUSE = compute_wh
  SCHEDULE = 'USING CRON 0 2 * * * America/New_York'
AS
CALL sales_db.analytics.refresh_customer_metrics();

-- Enable the task
ALTER TASK sales_db.analytics.refresh_customer_data RESUME;




-- Check all databases
SHOW DATABASES;

-- Check all tables including types
SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE 
FROM INFORMATION_SCHEMA.TABLES 
ORDER BY TABLE_SCHEMA, TABLE_NAME;

-- Check views and dependencies
SHOW VIEWS;

-- Check external tables
SHOW EXTERNAL TABLES;

-- Check stored procedures
SHOW PROCEDURES;

-- Check warehouses
SHOW WAREHOUSES;



-- Simple and correct version
CREATE AUTHENTICATION POLICY mfa_required_policy
  MFA_ENROLLMENT = REQUIRED
  COMMENT = 'Policy requiring MFA for all users';


ALTER AUTHENTICATION POLICY mfa_required_policy
  SET CLIENT_TYPES = ('SNOWFLAKE_UI');  -- add others if needed



  
