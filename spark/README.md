# Spark jobs

Run the full pipeline:

```bash
docker compose up -d spark-star-schema
docker compose logs -f data-loader spark-star-schema
```

This starts PostgreSQL as a dependency, loads CSV files with `transform.py`, and builds the star schema with Spark.

Start PostgreSQL and an idle Spark container for manual runs:

```bash
docker compose up -d postgres spark
```

Run the job that creates the star schema in PostgreSQL:

```bash
docker compose exec spark spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.postgresql:postgresql:42.7.3 \
  /opt/spark-apps/build_star_schema.py
```

Check loaded raw rows:

```bash
docker compose exec postgres psql -U admin -d db \
  -c "select count(*) from public.mock_data;"
```

Check star schema tables:

```bash
docker compose exec postgres psql -U admin -d db \
  -c "\dt star.*"
```
