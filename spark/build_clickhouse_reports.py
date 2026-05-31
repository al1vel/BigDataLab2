import os
import urllib.parse
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    concat_ws,
    corr,
    count,
    countDistinct,
    dense_rank,
    lag,
    lit,
    lpad,
    sum as spark_sum,
    to_date,
)
from pyspark.sql.window import Window


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "12345")
POSTGRES_DRIVER = "org.postgresql.Driver"

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_HTTP_PORT = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
CLICKHOUSE_JDBC_PORT = os.getenv("CLICKHOUSE_JDBC_PORT", CLICKHOUSE_HTTP_PORT)
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "reports")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"

POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
CLICKHOUSE_JDBC_URL = f"jdbc:ch:http://{CLICKHOUSE_HOST}:{CLICKHOUSE_JDBC_PORT}/{CLICKHOUSE_DB}"

POSTGRES_OPTIONS = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": POSTGRES_DRIVER,
}

CLICKHOUSE_OPTIONS = {
    "user": CLICKHOUSE_USER,
    "password": CLICKHOUSE_PASSWORD,
    "driver": CLICKHOUSE_DRIVER,
}


REPORT_TABLES = {
    "report_product_sales": """
        CREATE TABLE IF NOT EXISTS reports.report_product_sales (
            product_key String,
            source_product_id Nullable(Int64),
            product_name Nullable(String),
            product_category Nullable(String),
            pet_category Nullable(String),
            product_brand Nullable(String),
            total_quantity_sold Int64,
            total_revenue Float64,
            avg_rating Nullable(Float64),
            reviews_count Nullable(Int64),
            product_sales_rank UInt32,
            category_total_revenue Float64
        )
        ENGINE = MergeTree
        ORDER BY (product_sales_rank, product_key)
    """,
    "report_customer_sales": """
        CREATE TABLE IF NOT EXISTS reports.report_customer_sales (
            customer_key String,
            source_customer_id Nullable(Int64),
            customer_full_name String,
            customer_country Nullable(String),
            total_purchase_amount Float64,
            orders_count Int64,
            average_order_value Float64,
            customer_sales_rank UInt32,
            country_customer_count Int64
        )
        ENGINE = MergeTree
        ORDER BY (customer_sales_rank, customer_key)
    """,
    "report_time_sales": """
        CREATE TABLE IF NOT EXISTS reports.report_time_sales (
            period_start Date,
            year Int32,
            month Int32,
            total_revenue Float64,
            total_quantity_sold Int64,
            orders_count Int64,
            average_order_value Float64,
            previous_month_revenue Nullable(Float64),
            revenue_delta Nullable(Float64),
            yearly_total_revenue Float64
        )
        ENGINE = MergeTree
        ORDER BY (year, month)
    """,
    "report_store_sales": """
        CREATE TABLE IF NOT EXISTS reports.report_store_sales (
            store_key String,
            store_name Nullable(String),
            store_city Nullable(String),
            store_state Nullable(String),
            store_country Nullable(String),
            total_revenue Float64,
            orders_count Int64,
            average_order_value Float64,
            store_sales_rank UInt32,
            city_revenue Float64,
            country_revenue Float64
        )
        ENGINE = MergeTree
        ORDER BY (store_sales_rank, store_key)
    """,
    "report_supplier_sales": """
        CREATE TABLE IF NOT EXISTS reports.report_supplier_sales (
            supplier_key String,
            supplier_name Nullable(String),
            supplier_city Nullable(String),
            supplier_country Nullable(String),
            total_revenue Float64,
            total_quantity_sold Int64,
            average_product_price Nullable(Float64),
            supplier_sales_rank UInt32,
            country_revenue Float64
        )
        ENGINE = MergeTree
        ORDER BY (supplier_sales_rank, supplier_key)
    """,
    "report_product_quality": """
        CREATE TABLE IF NOT EXISTS reports.report_product_quality (
            product_key String,
            source_product_id Nullable(Int64),
            product_name Nullable(String),
            product_category Nullable(String),
            rating Nullable(Float64),
            reviews_count Nullable(Int64),
            total_quantity_sold Int64,
            total_revenue Float64,
            highest_rating_rank UInt32,
            lowest_rating_rank UInt32,
            reviews_rank UInt32,
            rating_sales_correlation Nullable(Float64)
        )
        ENGINE = MergeTree
        ORDER BY (highest_rating_rank, product_key)
    """,
}


def execute_clickhouse(sql):
    query = {
        "user": CLICKHOUSE_USER,
    }
    if CLICKHOUSE_PASSWORD:
        query["password"] = CLICKHOUSE_PASSWORD

    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}/?{urllib.parse.urlencode(query)}"
    request = urllib.request.Request(
        url,
        data=sql.encode("utf-8"),
        method="POST",
    )

    with urllib.request.urlopen(request, timeout=60) as response:
        response.read()


def prepare_clickhouse_tables():
    execute_clickhouse(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")
    for table_name, create_sql in REPORT_TABLES.items():
        execute_clickhouse(f"DROP TABLE IF EXISTS {CLICKHOUSE_DB}.{table_name}")
        execute_clickhouse(create_sql.replace("reports.", f"{CLICKHOUSE_DB}."))


def read_postgres_table(spark, table_name):
    return (
        spark.read.format("jdbc")
        .option("url", POSTGRES_JDBC_URL)
        .option("dbtable", table_name)
        .options(**POSTGRES_OPTIONS)
        .load()
    )


def write_clickhouse_table(df, table_name):
    (
        df.write.format("jdbc")
        .option("url", CLICKHOUSE_JDBC_URL)
        .option("dbtable", f"{CLICKHOUSE_DB}.{table_name}")
        .option("batchsize", "1000")
        .option("isolationLevel", "NONE")
        .options(**CLICKHOUSE_OPTIONS)
        .mode("append")
        .save()
    )


def revenue_column():
    return col("sale_total_price").cast("double")


def quantity_column():
    return col("sale_quantity").cast("long")


def build_product_sales(fact_sales, dim_products):
    product_metrics = fact_sales.groupBy("product_key").agg(
        spark_sum(quantity_column()).cast("long").alias("total_quantity_sold"),
        spark_sum(revenue_column()).cast("double").alias("total_revenue"),
    )

    product_report = product_metrics.join(dim_products, "product_key", "left").select(
        "product_key",
        col("source_product_id").cast("long").alias("source_product_id"),
        col("name").alias("product_name"),
        col("category").alias("product_category"),
        "pet_category",
        col("brand").alias("product_brand"),
        "total_quantity_sold",
        "total_revenue",
        col("rating").cast("double").alias("avg_rating"),
        col("reviews").cast("long").alias("reviews_count"),
    )

    category_revenue = product_report.groupBy("product_category").agg(
        spark_sum("total_revenue").cast("double").alias("category_total_revenue")
    )

    return (
        product_report.join(category_revenue, "product_category", "left")
        .withColumn(
            "product_sales_rank",
            dense_rank().over(Window.orderBy(col("total_quantity_sold").desc(), col("total_revenue").desc())),
        )
        .fillna({"category_total_revenue": 0.0})
        .select(
            "product_key",
            "source_product_id",
            "product_name",
            "product_category",
            "pet_category",
            "product_brand",
            "total_quantity_sold",
            "total_revenue",
            "avg_rating",
            "reviews_count",
            "product_sales_rank",
            "category_total_revenue",
        )
    )


def build_customer_sales(fact_sales, dim_customers):
    customer_metrics = fact_sales.groupBy("customer_key").agg(
        spark_sum(revenue_column()).cast("double").alias("total_purchase_amount"),
        count("*").cast("long").alias("orders_count"),
        avg(revenue_column()).cast("double").alias("average_order_value"),
    )

    customer_report = customer_metrics.join(dim_customers, "customer_key", "left").select(
        "customer_key",
        col("source_customer_id").cast("long").alias("source_customer_id"),
        concat_ws(" ", col("first_name"), col("last_name")).alias("customer_full_name"),
        col("country").alias("customer_country"),
        "total_purchase_amount",
        "orders_count",
        "average_order_value",
    )

    country_counts = dim_customers.groupBy(col("country").alias("customer_country")).agg(
        countDistinct("customer_key").cast("long").alias("country_customer_count")
    )

    return (
        customer_report.join(country_counts, "customer_country", "left")
        .withColumn(
            "customer_sales_rank",
            dense_rank().over(Window.orderBy(col("total_purchase_amount").desc())),
        )
        .fillna({"country_customer_count": 0})
        .select(
            "customer_key",
            "source_customer_id",
            "customer_full_name",
            "customer_country",
            "total_purchase_amount",
            "orders_count",
            "average_order_value",
            "customer_sales_rank",
            "country_customer_count",
        )
    )


def build_time_sales(fact_sales, dim_dates):
    date_sales = fact_sales.join(dim_dates, "date_key", "left").where(
        col("year").isNotNull() & col("month").isNotNull()
    )
    month_metrics = date_sales.groupBy("year", "month").agg(
        spark_sum(revenue_column()).cast("double").alias("total_revenue"),
        spark_sum(quantity_column()).cast("long").alias("total_quantity_sold"),
        count("*").cast("long").alias("orders_count"),
        avg(revenue_column()).cast("double").alias("average_order_value"),
    )

    month_window = Window.orderBy("year", "month")
    year_window = Window.partitionBy("year")

    return (
        month_metrics.withColumn(
            "period_start",
            to_date(concat_ws("-", col("year"), lpad(col("month").cast("string"), 2, "0"), lit("01"))),
        )
        .withColumn("previous_month_revenue", lag("total_revenue").over(month_window))
        .withColumn("revenue_delta", col("total_revenue") - col("previous_month_revenue"))
        .withColumn("yearly_total_revenue", spark_sum("total_revenue").over(year_window).cast("double"))
        .select(
            "period_start",
            col("year").cast("int").alias("year"),
            col("month").cast("int").alias("month"),
            "total_revenue",
            "total_quantity_sold",
            "orders_count",
            "average_order_value",
            "previous_month_revenue",
            "revenue_delta",
            "yearly_total_revenue",
        )
    )


def build_store_sales(fact_sales, dim_stores):
    store_metrics = fact_sales.groupBy("store_key").agg(
        spark_sum(revenue_column()).cast("double").alias("total_revenue"),
        count("*").cast("long").alias("orders_count"),
        avg(revenue_column()).cast("double").alias("average_order_value"),
    )

    store_report = store_metrics.join(dim_stores, "store_key", "left").select(
        "store_key",
        col("name").alias("store_name"),
        col("city").alias("store_city"),
        col("state").alias("store_state"),
        col("country").alias("store_country"),
        "total_revenue",
        "orders_count",
        "average_order_value",
    )

    city_revenue = store_report.groupBy("store_city", "store_country").agg(
        spark_sum("total_revenue").cast("double").alias("city_revenue")
    )
    country_revenue = store_report.groupBy("store_country").agg(
        spark_sum("total_revenue").cast("double").alias("country_revenue")
    )

    return (
        store_report.join(city_revenue, ["store_city", "store_country"], "left")
        .join(country_revenue, "store_country", "left")
        .withColumn(
            "store_sales_rank",
            dense_rank().over(Window.orderBy(col("total_revenue").desc())),
        )
        .fillna({"city_revenue": 0.0, "country_revenue": 0.0})
        .select(
            "store_key",
            "store_name",
            "store_city",
            "store_state",
            "store_country",
            "total_revenue",
            "orders_count",
            "average_order_value",
            "store_sales_rank",
            "city_revenue",
            "country_revenue",
        )
    )


def build_supplier_sales(fact_sales, dim_suppliers):
    supplier_metrics = fact_sales.groupBy("supplier_key").agg(
        spark_sum(revenue_column()).cast("double").alias("total_revenue"),
        spark_sum(quantity_column()).cast("long").alias("total_quantity_sold"),
        avg(col("product_price").cast("double")).cast("double").alias("average_product_price"),
    )

    supplier_report = supplier_metrics.join(dim_suppliers, "supplier_key", "left").select(
        "supplier_key",
        col("name").alias("supplier_name"),
        col("city").alias("supplier_city"),
        col("country").alias("supplier_country"),
        "total_revenue",
        "total_quantity_sold",
        "average_product_price",
    )

    country_revenue = supplier_report.groupBy("supplier_country").agg(
        spark_sum("total_revenue").cast("double").alias("country_revenue")
    )

    return (
        supplier_report.join(country_revenue, "supplier_country", "left")
        .withColumn(
            "supplier_sales_rank",
            dense_rank().over(Window.orderBy(col("total_revenue").desc())),
        )
        .fillna({"country_revenue": 0.0})
        .select(
            "supplier_key",
            "supplier_name",
            "supplier_city",
            "supplier_country",
            "total_revenue",
            "total_quantity_sold",
            "average_product_price",
            "supplier_sales_rank",
            "country_revenue",
        )
    )


def build_product_quality(fact_sales, dim_products):
    product_metrics = fact_sales.groupBy("product_key").agg(
        spark_sum(quantity_column()).cast("long").alias("total_quantity_sold"),
        spark_sum(revenue_column()).cast("double").alias("total_revenue"),
    )

    quality_report = product_metrics.join(dim_products, "product_key", "left").select(
        "product_key",
        col("source_product_id").cast("long").alias("source_product_id"),
        col("name").alias("product_name"),
        col("category").alias("product_category"),
        col("rating").cast("double").alias("rating"),
        col("reviews").cast("long").alias("reviews_count"),
        "total_quantity_sold",
        "total_revenue",
    )

    correlation_row = quality_report.select(corr("rating", "total_quantity_sold")).first()
    rating_sales_correlation = correlation_row[0] if correlation_row else None

    return (
        quality_report.withColumn(
            "highest_rating_rank",
            dense_rank().over(Window.orderBy(col("rating").desc_nulls_last())),
        )
        .withColumn(
            "lowest_rating_rank",
            dense_rank().over(Window.orderBy(col("rating").asc_nulls_last())),
        )
        .withColumn(
            "reviews_rank",
            dense_rank().over(Window.orderBy(col("reviews_count").desc_nulls_last())),
        )
        .withColumn(
            "rating_sales_correlation",
            lit(rating_sales_correlation).cast("double"),
        )
        .select(
            "product_key",
            "source_product_id",
            "product_name",
            "product_category",
            "rating",
            "reviews_count",
            "total_quantity_sold",
            "total_revenue",
            "highest_rating_rank",
            "lowest_rating_rank",
            "reviews_rank",
            "rating_sales_correlation",
        )
    )


def main():
    spark = (
        SparkSession.builder.appName("BuildClickHouseReports")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    prepare_clickhouse_tables()

    fact_sales = read_postgres_table(spark, "star.fact_sales")
    dim_products = read_postgres_table(spark, "star.dim_products")
    dim_customers = read_postgres_table(spark, "star.dim_customers")
    dim_dates = read_postgres_table(spark, "star.dim_dates")
    dim_stores = read_postgres_table(spark, "star.dim_stores")
    dim_suppliers = read_postgres_table(spark, "star.dim_suppliers")

    reports = {
        "report_product_sales": build_product_sales(fact_sales, dim_products),
        "report_customer_sales": build_customer_sales(fact_sales, dim_customers),
        "report_time_sales": build_time_sales(fact_sales, dim_dates),
        "report_store_sales": build_store_sales(fact_sales, dim_stores),
        "report_supplier_sales": build_supplier_sales(fact_sales, dim_suppliers),
        "report_product_quality": build_product_quality(fact_sales, dim_products),
    }

    for table_name, dataframe in reports.items():
        write_clickhouse_table(dataframe, table_name)
        print(f"Wrote {dataframe.count()} rows to ClickHouse table {CLICKHOUSE_DB}.{table_name}")

    spark.stop()


if __name__ == "__main__":
    main()
