import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    coalesce,
    date_format,
    dayofmonth,
    length,
    lit,
    month,
    quarter,
    row_number,
    sha2,
    to_date,
    trim,
    when,
    year,
)
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "12345")
POSTGRES_DRIVER = "org.postgresql.Driver"

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
JDBC_OPTIONS = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": POSTGRES_DRIVER,
}


def clean_text_columns(df):
    for field in df.schema.fields:
        if field.dataType.simpleString() == "string":
            df = df.withColumn(
                field.name,
                when(length(trim(col(field.name))) == 0, None).otherwise(trim(col(field.name))),
            )
    return df


def hash_key(*column_names):
    values = [coalesce(col(name).cast("string"), lit("")) for name in column_names]
    return sha2(concat_ws("||", *values), 256)


def execute_sql(spark, sql):
    props = spark._sc._gateway.jvm.java.util.Properties()
    props.setProperty("user", POSTGRES_USER)
    props.setProperty("password", POSTGRES_PASSWORD)
    props.setProperty("driver", POSTGRES_DRIVER)

    connection = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(JDBC_URL, props)
    try:
        statement = connection.createStatement()
        try:
            statement.execute(sql)
        finally:
            statement.close()
    finally:
        connection.close()


def read_postgres_table(spark, table_name):
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table_name)
        .options(**JDBC_OPTIONS)
        .load()
    )


def write_postgres_table(df, table_name):
    (
        df.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table_name)
        .option("batchsize", "1000")
        .options(**JDBC_OPTIONS)
        .mode("overwrite")
        .save()
    )


def main():
    spark = (
        SparkSession.builder.appName("BuildPostgresStarSchema")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    execute_sql(spark, "CREATE SCHEMA IF NOT EXISTS star")

    raw = clean_text_columns(read_postgres_table(spark, "public.mock_data"))
    raw_count = raw.count()
    if raw_count == 0:
        raise RuntimeError("public.mock_data is empty. Load CSV files into PostgreSQL first.")

    sales = (
        raw.withColumn("customer_key", hash_key(
            "sale_customer_id",
            "customer_first_name",
            "customer_last_name",
            "customer_email",
            "customer_country",
            "customer_postal_code",
            "customer_pet_type",
            "customer_pet_name",
            "customer_pet_breed",
            "pet_category",
        ))
        .withColumn("seller_key", hash_key(
            "sale_seller_id",
            "seller_first_name",
            "seller_last_name",
            "seller_email",
            "seller_country",
            "seller_postal_code",
        ))
        .withColumn("product_key", hash_key(
            "sale_product_id",
            "product_name",
            "product_category",
            "product_brand",
            "product_material",
            "product_color",
            "product_size",
            "pet_category",
        ))
        .withColumn("store_key", hash_key(
            "store_name",
            "store_location",
            "store_city",
            "store_state",
            "store_country",
            "store_phone",
            "store_email",
        ))
        .withColumn("supplier_key", hash_key(
            "supplier_name",
            "supplier_contact",
            "supplier_email",
            "supplier_phone",
            "supplier_address",
            "supplier_city",
            "supplier_country",
        ))
        .withColumn("sale_date_value", coalesce(col("sale_date").cast("date"), to_date(col("sale_date").cast("string"), "M/d/yyyy")))
        .withColumn("date_key", date_format(col("sale_date_value"), "yyyyMMdd").cast("int"))
        .withColumn("customer_age", col("customer_age").cast("int"))
        .withColumn("product_price", col("product_price").cast(DecimalType(12, 2)))
        .withColumn("product_quantity", col("product_quantity").cast("int"))
        .withColumn("sale_customer_id", col("sale_customer_id").cast("long"))
        .withColumn("sale_seller_id", col("sale_seller_id").cast("long"))
        .withColumn("sale_product_id", col("sale_product_id").cast("long"))
        .withColumn("sale_quantity", col("sale_quantity").cast("int"))
        .withColumn("sale_total_price", col("sale_total_price").cast(DecimalType(12, 2)))
        .withColumn("product_weight", col("product_weight").cast(DecimalType(12, 2)))
        .withColumn("product_rating", col("product_rating").cast(DecimalType(3, 2)))
        .withColumn("product_reviews", col("product_reviews").cast("int"))
        .withColumn("product_release_date", coalesce(col("product_release_date").cast("date"), to_date(col("product_release_date").cast("string"), "M/d/yyyy")))
        .withColumn("product_expiry_date", coalesce(col("product_expiry_date").cast("date"), to_date(col("product_expiry_date").cast("string"), "M/d/yyyy")))
    )

    dim_customers = sales.select(
        "customer_key",
        col("sale_customer_id").alias("source_customer_id"),
        col("customer_first_name").alias("first_name"),
        col("customer_last_name").alias("last_name"),
        col("customer_age").alias("age"),
        col("customer_email").alias("email"),
        col("customer_country").alias("country"),
        col("customer_postal_code").alias("postal_code"),
        col("customer_pet_type").alias("pet_type"),
        col("customer_pet_name").alias("pet_name"),
        col("customer_pet_breed").alias("pet_breed"),
        "pet_category",
    ).dropDuplicates(["customer_key"])

    dim_sellers = sales.select(
        "seller_key",
        col("sale_seller_id").alias("source_seller_id"),
        col("seller_first_name").alias("first_name"),
        col("seller_last_name").alias("last_name"),
        col("seller_email").alias("email"),
        col("seller_country").alias("country"),
        col("seller_postal_code").alias("postal_code"),
    ).dropDuplicates(["seller_key"])

    dim_products = sales.select(
        "product_key",
        col("sale_product_id").alias("source_product_id"),
        col("product_name").alias("name"),
        col("product_category").alias("category"),
        "pet_category",
        col("product_price").alias("price"),
        col("product_quantity").alias("stock_quantity"),
        col("product_weight").alias("weight"),
        col("product_color").alias("color"),
        col("product_size").alias("size"),
        col("product_brand").alias("brand"),
        col("product_material").alias("material"),
        col("product_description").alias("description"),
        col("product_rating").alias("rating"),
        col("product_reviews").alias("reviews"),
        col("product_release_date").alias("release_date"),
        col("product_expiry_date").alias("expiry_date"),
    ).dropDuplicates(["product_key"])

    dim_stores = sales.select(
        "store_key",
        col("store_name").alias("name"),
        col("store_location").alias("location"),
        col("store_city").alias("city"),
        col("store_state").alias("state"),
        col("store_country").alias("country"),
        col("store_phone").alias("phone"),
        col("store_email").alias("email"),
    ).dropDuplicates(["store_key"])

    dim_suppliers = sales.select(
        "supplier_key",
        col("supplier_name").alias("name"),
        col("supplier_contact").alias("contact"),
        col("supplier_email").alias("email"),
        col("supplier_phone").alias("phone"),
        col("supplier_address").alias("address"),
        col("supplier_city").alias("city"),
        col("supplier_country").alias("country"),
    ).dropDuplicates(["supplier_key"])

    dim_dates = (
        sales.where(col("date_key").isNotNull())
        .select(
            "date_key",
            col("sale_date_value").alias("date"),
            year("sale_date_value").alias("year"),
            quarter("sale_date_value").alias("quarter"),
            month("sale_date_value").alias("month"),
            dayofmonth("sale_date_value").alias("day"),
            date_format("sale_date_value", "E").alias("weekday"),
        )
        .dropDuplicates(["date_key"])
    )

    sale_key_window = Window.orderBy(
        col("id"),
        col("sale_date_value"),
        col("sale_customer_id"),
        col("sale_seller_id"),
        col("sale_product_id"),
        col("sale_quantity"),
        col("sale_total_price"),
    )

    fact_sales = sales.withColumn("sale_key", row_number().over(sale_key_window)).select(
        "sale_key",
        col("id").alias("source_row_id"),
        "date_key",
        "customer_key",
        "seller_key",
        "product_key",
        "store_key",
        "supplier_key",
        "sale_customer_id",
        "sale_seller_id",
        "sale_product_id",
        "sale_quantity",
        "sale_total_price",
        "product_price",
    )

    tables = {
        "star.dim_customers": dim_customers,
        "star.dim_sellers": dim_sellers,
        "star.dim_products": dim_products,
        "star.dim_stores": dim_stores,
        "star.dim_suppliers": dim_suppliers,
        "star.dim_dates": dim_dates,
        "star.fact_sales": fact_sales,
    }

    for table_name, dataframe in tables.items():
        write_postgres_table(dataframe, table_name)
        print(f"Wrote {dataframe.count()} rows to {table_name}")

    print(f"Built star schema from {raw_count} raw rows")
    spark.stop()


if __name__ == "__main__":
    main()
