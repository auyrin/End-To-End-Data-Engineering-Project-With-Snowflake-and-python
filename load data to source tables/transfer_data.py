import sys
import logging

from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.types import StructType, StringType, StructField, StringType,LongType,DecimalType,DateType,TimestampType
from snowflake.snowpark.functions import col,lit,row_number, rank
from snowflake.snowpark import Window

# initiate logging at info level
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%I:%M:%S')

# snowpark session
def get_snowpark_session() -> Session:
    connection_parameters = {
       "ACCOUNT":"fttphrp-evb74680",
        "USER":"SNOWPARK_USER",
        "PASSWORD":"baby123",
        "ROLE":"SYSADMIN",
        "DATABASE":"sales_dwh",
        "SCHEMA":"source",
        "WAREHOUSE":"SNOWPARK_ETL_WH"
    }
    # creating snowflake session object
    return Session.builder.configs(connection_parameters).create()   


def ingest_in_sales(session) -> None:
    session.sql("""
        copy INTO sales_dwh.source.in_sales_order
        from (
            SELECT
                sales_dwh.source.in_sales_order_seq.nextval, 
                t.$1::text as order_id, 
                t.$2::text as customer_name,
                t.$3::text as mobile_key,
                t.$4::number as order_quantity,
                t.$5::number as unit_price,
                t.$6::number as order_valaue,
                t.$7::text as promotion_code, 
                t.$8::number(10,2)  as final_order_amount,
                t.$9::number(10,2) as tax_amount,
                t.$10::date as order_dt,
                t.$11::text as payment_status,
                t.$12::text as shipping_status,
                t.$13::text as payment_method,
                t.$14::text as payment_provider,
                t.$15::text as mobile,
                t.$16::text as shipping_address,
                metadata$filename as stg_file_name,
                metadata$file_row_number as stg_row_numer,
                metadata$file_last_modified as stg_last_modified
            FROM
                @sales_dwh.source.my_internal_stg/csv/
                (FILE_FORMAT => sales_dwh.common.my_csv_format) t )
                on_error = continue
    """).collect()

def ingest_us_sales(session) -> None:
    session.sql("""
        copy INTO sales_dwh.source.us_sales_order
        from (
            SELECT
                sales_dwh.source.us_sales_order_seq.nextval,
                t.$1:"Order ID"::TEXT AS order_id,
                t.$1:"Customer Name"::TEXT AS customer_name,
                t.$1:"Mobile Model"::TEXT AS mobile_key,
                TO_NUMBER(t.$1:"Quantity") AS quantity,
                TO_NUMBER(t.$1:"Price per Unit") AS unit_price,
                TO_DECIMAL(t.$1:"Total Price") AS total_price,
                t.$1:"Promotion Code"::TEXT AS promotion_code,
                t.$1:"Order Amount"::NUMBER(10,2) AS order_amount,
                TO_DECIMAL(t.$1:"Tax") AS tax,
                t.$1:"Order Date"::DATE AS order_dt,
                t.$1:"Payment Status"::TEXT AS payment_status,
                t.$1:"Shipping Status"::TEXT AS shipping_status,
                t.$1:"Payment Method"::TEXT AS payment_method,
                t.$1:"Payment Provider"::TEXT AS payment_provider,
                t.$1:"Phone"::TEXT AS phone,
                t.$1:"Delivery Address"::TEXT AS shipping_address,
                METADATA$FILENAME AS stg_file_name,
                METADATA$FILE_ROW_NUMBER AS stg_row_number,
                METADATA$FILE_LAST_MODIFIED AS stg_last_modified
            FROM
                @sales_dwh.source.my_internal_stg/parquet/
                (FILE_FORMAT => sales_dwh.common.my_parquet_format) t )
                on_error = continue
    """).collect()
    
def ingest_fr_sales(session)-> None:
    session.sql("""
        copy into sales_dwh.source.fr_sales_order
        from                                                    
        ( 
            select  
            sales_dwh.source.fr_sales_order_seq.nextval, 
            t.$1:"Order ID"::text as order_id,
            t.$1:"Customer Name"::text as customer_name,
            t.$1:"Mobile Model"::text as mobile_key,
            to_number(t.$1:"Quantity") as quantity,
            to_number(t.$1:"Price per Unit") as unit_price,
            to_decimal(t.$1:"Total Price") as total_price,
            t.$1:"Promotion Code"::text as promotion_code,  
            t.$1:"Order Amount"::number(10,2) as order_amount,
            to_decimal(t.$1:"Tax") as tax, 
            t.$1:"Order Date"::date as order_dt,
            t.$1:"Payment Status"::text as payment_status,
            t.$1:"Shipping Status"::text as shipping_status,
            t.$1:"Payment Method"::text as payment_method,
            t.$1:"Payment Provider"::text as payment_provider,
            t.$1:"Phone"::text as phone,
            t.$1:"Delivery Address"::text as shipping_address,
            metadata$filename as stg_file_name,
            metadata$file_row_number as stg_row_numer,
            metadata$file_last_modified as stg_last_modified
            from
            @sales_dwh.source.my_internal_stg/json/
            (file_format => sales_dwh.common.my_json_format) t )
            on_error=continue
        """).collect()

def main():

    #get the session object and get dataframe
    session = get_snowpark_session()

    #ingest in sales data
    print("<indian sales order> before copy:")
    session.sql("select count(*) from sales_dwh.source.in_sales_order").show()
    ingest_in_sales(session)
    print("<indian sales order> after copy:")
    session.sql("select count(*) from sales_dwh.source.in_sales_order").show()

    #ingest in sales data
    print("<US sales order> before copy:")
    session.sql("select count(*) from sales_dwh.source.us_sales_order").show()
    ingest_us_sales(session) 
    print("<US sales order> after copy:")
    session.sql("select count(*) from sales_dwh.source.us_sales_order").show()

    #ingest in sales data
    print("<FR sales order> before copy:")
    session.sql("select count(*) from sales_dwh.source.fr_sales_order").show()
    ingest_fr_sales(session)   
    print("<FR sales order> after copy:")
    session.sql("select count(*) from sales_dwh.source.fr_sales_order").show()

if __name__ == '__main__':
    main()