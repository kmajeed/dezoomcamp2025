# Notes and code for Week 1 Home work

## Question 1:

- Save the Dockerfile: Save the following code as a file named Dockerfile.

```
# Use the official Python 3.12.8 image as a base
FROM python:3.12.8

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies if needed (example: for building certain Python packages)
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     build-essential \
#     && rm -rf /var/lib/apt/lists/*

# Copy your application code if you have any
# COPY . /app

# Set the entrypoint to bash for interactive mode
ENTRYPOINT ["/bin/bash"]

# Add a command to check pip version inside the container
RUN pip --version

# or for more detailed output
# RUN pip show pip
```
- Build the image: Open the terminal in the directory where you saved the Dockerfile and run the following command:

```Bash
docker build -t python3128-interactive .
```

`-t pthon3128-interactive` :This tags the image with the name `pthon3128-interactive`. 

`.` :This specifies the build context (the current directory).

- Run the container in interactive mode:

```bash
docker run -it python3128-interactive
```

`-i`: This keeps `STDIN` open even if not attached.

`-t`: This allocates a `pseudo-TTY`.

`python3128-interactive`: This is the name of the image we built.

we will now be inside the container's bash shell. You can run Python, install packages with pip, and explore the file system.

- Now run either of the following commands

```bash
root@86b49387500e:/app# pip --version
pip 24.3.1 from /usr/local/lib/python3.12/site-packages/pip (python 3.12)
root@86b49387500e:/app#
root@86b49387500e:/app# python -m pip --version
pip 24.3.1 from /usr/local/lib/python3.12/site-packages/pip (python 3.12)
root@86b49387500e:/app#
root@86b49387500e:/app# pip show pip
Name: pip
Version: 24.3.1
Summary: The PyPA recommended tool for installing Python packages.
Home-page: https://pip.pypa.io/
Author: 
Author-email: The pip developers <distutils-sig@python.org>
License: MIT
Location: /usr/local/lib/python3.12/site-packages
Requires: 
Required-by: 
root@86b49387500e:/app#
```
# Docker Compose

```bash
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin  

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```
docker run -d \
    -e DB_HOST=localhost \
    -e DB_PORT=5432 \
    -e DB_USER=postgres \
    -e DB_PASSWORD=postgres \
    -e DB_NAME=ny_taxi \
    --network your_docker_network \ # Important: Connect to the database network
    ny-taxi-ingest

# Running docker for ingestion script

docker run -it \
    --network=pg-network \
  ingest_green:v002 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_gtaxi \
    --table_name=green_taxi_data \
    --zones_table_name=zones \
    --urlg="https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-10.parquet" \
    --urlz="https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

docker run -d \
    -e DB_HOST=localhost \
    -e DB_PORT=5432 \
    -e DB_USER=postgres \
    -e DB_PASSWORD=postgres \
    -e DB_NAME=ny_taxi \
    ny-taxi-ingest




ingest-data-1  | /app/ingest.py:90: MovedIn20Warning: The ``declarative_base()`` function is now available as sqlalchemy.orm.declarative_base(). (deprecated since: 2.0) (Background on SQLAlchemy 2.0 at: https://sqlalche.me/e/b8d9)


ingest-data-1  | 2025-01-26 20:08:04,207 - ERROR - Error ingesting data: (psycopg2.errors.UndefinedColumn) column "lpep_pickup_datetime" of relation "green_taxi_trips" does not exist
ingest-data-1  | LINE 1: INSERT INTO green_taxi_trips (vendor_id, lpep_pickup_datetim...
ingest-data-1  |                                                  ^
ingest-data-1  | 
ingest-data-1  | [SQL: INSERT INTO green_taxi_trips (vendor_id, lpep_pickup_datetime, lpep_dropoff_datetime, store_and_fwd_flag, ratecode_id, pickup_location_id, dropoff_location_id, passenger_count, trip_distance, fare_amount, extra, mta_tax, tip_amount, tolls_amount, eha ... 491797 characters truncated ... )s, %(total_amount__999)s, %(payment_type__999)s, %(trip_type__999)s, %(congestion_surcharge__999)s)]
ingest-data-1  | [parameters: {'payment_type__0': 2, 'trip_type__0': '1', 'tolls_amount__0': 0.0, 'total_amount__0': 19.3, 'congestion_surcharge__0': 0.0, 'lpep_pickup_datetime__0': '2019-10-01 00:26:02', 'vendor_id__0': 2, 'store_and_fwd_flag__0': 'N', 'improvement_surcharge__0': 0.3, 'lpep_dropoff_datetime__0': '2019-10-01 00:39:58', 'ehail_fee__0': None, 'pickup_location_id__0': 112, 'dropoff_location_id__0': 196, 'trip_distance__0': 5.88, 'ratecode_id__0': 1, 'tip_amount__0': 0.0, 'extra__0': 0.5, 'mta_tax__0': 0.5, 'fare_amount__0': 18.0, 'passenger_count__0': 1, 'payment_type__1': 2, 'trip_type__1': '1', 'tolls_amount__1': 0.0, 'total_amount__1': 9.05, 'congestion_surcharge__1': 0.0, 'lpep_pickup_datetime__1': '2019-10-01 00:18:11', 'vendor_id__1': 1, 'store_and_fwd_flag__1': 'N', 'improvement_surcharge__1': 0.3, 'lpep_dropoff_datetime__1': '2019-10-01 00:22:38', 'ehail_fee__1': None, 'pickup_location_id__1': 43, 'dropoff_location_id__1': 263, 'trip_distance__1': 0.8, 'ratecode_id__1': 1, 'tip_amount__1': 0.0, 'extra__1': 3.25, 'mta_tax__1': 0.5, 'fare_amount__1': 5.0, 'passenger_count__1': 1, 'payment_type__2': 2, 'trip_type__2': '1', 'tolls_amount__2': 0.0, 'total_amount__2': 22.8, 'congestion_surcharge__2': 0.0, 'lpep_pickup_datetime__2': '2019-10-01 00:09:31', 'vendor_id__2': 1, 'store_and_fwd_flag__2': 'N', 'improvement_surcharge__2': 0.3, 'lpep_dropoff_datetime__2': '2019-10-01 00:24:47' ... 19900 parameters truncated ... 'ehail_fee__997': None, 'pickup_location_id__997': 75, 'dropoff_location_id__997': 74, 'trip_distance__997': 0.63, 'ratecode_id__997': 1, 'tip_amount__997': 0.0, 'extra__997': 0.0, 'mta_tax__997': 0.5, 'fare_amount__997': 4.0, 'passenger_count__997': 1, 'payment_type__998': 1, 'trip_type__998': '1', 'tolls_amount__998': 0.0, 'total_amount__998': 11.3, 'congestion_surcharge__998': 0.0, 'lpep_pickup_datetime__998': '2019-10-01 07:57:32', 'vendor_id__998': 1, 'store_and_fwd_flag__998': 'N', 'improvement_surcharge__998': 0.3, 'lpep_dropoff_datetime__998': '2019-10-01 08:11:12', 'ehail_fee__998': None, 'pickup_location_id__998': 76, 'dropoff_location_id__998': 222, 'trip_distance__998': 1.8, 'ratecode_id__998': 1, 'tip_amount__998': 0.0, 'extra__998': 0.0, 'mta_tax__998': 0.5, 'fare_amount__998': 10.5, 'passenger_count__998': 1, 'payment_type__999': 1, 'trip_type__999': '1', 'tolls_amount__999': 0.0, 'total_amount__999': 14.76, 'congestion_surcharge__999': 0.0, 'lpep_pickup_datetime__999': '2019-10-01 07:43:02', 'vendor_id__999': 2, 'store_and_fwd_flag__999': 'N', 'improvement_surcharge__999': 0.3, 'lpep_dropoff_datetime__999': '2019-10-01 07:56:32', 'ehail_fee__999': None, 'pickup_location_id__999': 116, 'dropoff_location_id__999': 24, 'trip_distance__999': 2.48, 'ratecode_id__999': 1, 'tip_amount__999': 2.46, 'extra__999': 0.0, 'mta_tax__999': 0.5, 'fare_amount__999': 11.5, 'passenger_count__999': 1}]
ingest-data-1  | (Background on this error at: https://sqlalche.me/e/20/f405)

