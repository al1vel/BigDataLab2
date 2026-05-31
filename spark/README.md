# Spark jobs

Запустить:

```bash
docker compose up -d
docker compose logs -f data-loader spark-star-schema spark-clickhouse-reports
```

Check ClickHouse report tables:

```bash
docker compose exec clickhouse clickhouse-client \
  --query "SHOW TABLES FROM reports"
```

```bash
docker compose exec clickhouse clickhouse-client \
  --query "SELECT count() FROM reports.report_product_sales"
```
