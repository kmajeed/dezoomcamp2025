# Module 1 Homework: Docker & SQL

In this homework we'll prepare the environment and practice
Docker and SQL

When submitting your homework, you will also need to include
a link to your GitHub repository or other public code-hosting
site.

This repository should contain the code for solving the homework. 

When your solution has SQL or shell commands and not code
(e.g. python files) file format, include them directly in
the README file of your repository.


## Question 1. Understanding docker first run 

Run docker with the `python:3.12.8` image in an interactive mode, use the entrypoint `bash`.

What's the version of `pip` in the image?

- 24.3.1
- 24.2.1
- 23.3.1
- 23.2.1

Running the docker and checking the pip version from bash I got the following answer

```python
pip 24.3.1 from /usr/local/lib/python3.12/site-packages/pip (python 3.12)
```

## Question 2. Understanding Docker networking and docker-compose

Given the following `docker-compose.yaml`, what is the `hostname` and `port` that **pgadmin** should use to connect to the postgres database?

```yaml
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

- postgres:5433
- localhost:5432
- db:5433
- postgres:5432
- db:5432

If there are more than one answers, select only one of them

To connect to the Postgres database from pgadmin, you should use the following hostname and port:

Hostname: `db` (this is the service name of the Postgres container, which is used as the hostname within the Docker network)
Port: `5432` (this is the default port used by Postgres, which is exposed within the Docker network)
Note that the port mapping 5433:5432 in the db service only applies to external access to the Postgres container, i.e., from outside the Docker network. Within the Docker network, the Postgres container is still listening on port 5432.

So, in pgadmin, you would create a new server connection with the following settings:

Hostname: db
Port: 5432
Username: postgres
Password: postgres
Database: ny_taxi


##  Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from October 2019:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz
```

You will also need the dataset with zones:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```

Download this data and put it into Postgres.

You can use the code from the course. It's up to you whether
you want to use Jupyter or a python script.


- Code created: refer to ingest.py 
- Rows inserted into `green_taxi_trips` = 476,386 rows
- Rows added to `zones` table = 256 rows

## Question 3. Trip Segmentation Count

During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, **respectively**, happened:
1. Up to 1 mile
2. In between 1 (exclusive) and 3 miles (inclusive),
3. In between 3 (exclusive) and 7 miles (inclusive),
4. In between 7 (exclusive) and 10 miles (inclusive),
5. Over 10 miles 

Answers:

- 104,802;  197,670;  110,612;  27,831;  35,281
- 104,802;  198,924;  109,603;  27,678;  35,189
- 104,793;  201,407;  110,612;  27,831;  35,281
- 104,793;  202,661;  109,603;  27,678;  35,189
- 104,838;  199,013;  109,645;  27,688;  35,202

```sql
SELECT
    CASE
        WHEN trip_distance <= 1 THEN 'Up to 1 mile'
        WHEN trip_distance > 1 AND trip_distance <= 3 THEN 'Between 1 and 3 miles'
        WHEN trip_distance > 3 AND trip_distance <= 7 THEN 'Between 3 and 7 miles'
        WHEN trip_distance > 7 AND trip_distance <= 10 THEN 'Between 7 and 10 miles'
        WHEN trip_distance > 10 THEN 'Over 10 miles'
        ELSE 'Unknown'  -- Handle potential NULL or unexpected values
    END AS trip_segment,
    COUNT(*) AS trip_count
FROM
    green_taxi_trips
WHERE
    dropoff_datetime >= '2019-10-01' AND dropoff_datetime < '2019-11-01'
GROUP BY
    trip_segment
ORDER BY
    trip_segment;
```

Output
| Segment                    | Count  |
|----------------------------|--------|
| "Up to 1 mile"             | 104,802 |
| "Between 1 and 3 miles"    | 198,924 |
| "Between 3 and 7 miles"    | 109,603 |
| "Between 7 and 10 miles"   | 27,678  |
| "Over 10 miles"            | 35,189  |


So correct answer is - **104,802;  198,924;  109,603;  27,678;  35,189**

## Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance?
Use the pick up time for your calculations.

Tip: For every day, we only care about one single trip with the longest distance. 

- 2019-10-11
- 2019-10-24
- 2019-10-26
- 2019-10-31

```SQL
WITH DailyMaxTrips AS (
    SELECT
        DATE(pickup_datetime) AS pickup_date,
        MAX(trip_distance) AS max_trip_distance
    FROM
        green_taxi_trips
    GROUP BY
        pickup_date
),
RankedTrips AS (
    SELECT
        DATE(pickup_datetime) AS pickup_date,
        trip_distance,
        ROW_NUMBER() OVER (PARTITION BY DATE(pickup_datetime) ORDER BY trip_distance DESC) as rn
    FROM
        green_taxi_trips
    WHERE (DATE(pickup_datetime), trip_distance) IN (SELECT pickup_date, max_trip_distance from DailyMaxTrips)
)
SELECT pickup_date, trip_distance
from RankedTrips
where rn = 1
ORDER BY trip_distance DESC;
```

Output
| pickup_date                | trip_distance  |
|----------------------------|----------------|
| 2019-10-31                 |  515.89        |

## Question 5. Three biggest pickup zones

Which were the top pickup locations with over 13,000 in
`total_amount` (across all trips) for 2019-10-18?

Consider only `lpep_pickup_datetime` when filtering by date.
 
- East Harlem North, East Harlem South, Morningside Heights
- East Harlem North, Morningside Heights
- Morningside Heights, Astoria Park, East Harlem South
- Bedford, East Harlem North, Astoria Park


```sql
SELECT
    tz.zone,
    SUM(gtt.total_amount) AS total_amount_sum
FROM
    green_taxi_trips gtt
JOIN
    taxi_zones tz ON gtt.pickup_location_id = tz.location_id
WHERE
    DATE(gtt.pickup_datetime) = '2019-10-18'
GROUP BY
    tz.zone
HAVING
    SUM(gtt.total_amount) > 13000
ORDER BY
    total_amount_sum DESC;
```
**Output**
| Zone                  | total_amount_sum  |
|-----------------------|-------------------|
| East Harlem North     |18686.68 |
| East Harlem South     |16797.26 |
| Morningside Heights   |13029.79 |


So correct answer is - **East Harlem North, East Harlem South, Morningside Heights**

## Question 6. Largest tip

For the passengers picked up in October 2019 in the zone
named "East Harlem North" which was the drop off zone that had
the largest tip?

Note: it's `tip` , not `trip`

We need the name of the zone, not the ID.

- Yorkville West
- JFK Airport
- East Harlem North
- East Harlem South

```sql
SELECT
    dropoff_zone.zone,
    MAX(gtt.tip_amount) AS largest_tip_amount
FROM
    green_taxi_trips gtt
JOIN
    taxi_zones pickup_zone ON gtt.pickup_location_id = pickup_zone.location_id
JOIN
    taxi_zones dropoff_zone ON gtt.dropoff_location_id = dropoff_zone.location_id
WHERE
    pickup_zone.zone = 'East Harlem North'
    AND gtt.pickup_datetime >= '2019-10-01'
    AND gtt.pickup_datetime < '2019-11-01'
GROUP BY
    dropoff_zone.zone
ORDER BY
    largest_tip_amount DESC
LIMIT 1;
```
Output 

- JFK Airport

## Terraform

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform. 
Copy the files from the course repo
[here](../../../01-docker-terraform/1_terraform_gcp/terraform) to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.


## Question 7. Terraform Workflow

Which of the following sequences, **respectively**, describes the workflow for: 
1. Downloading the provider plugins and setting up backend,
2. Generating proposed changes and auto-executing the plan
3. Remove all resources managed by terraform`

Answers:
- terraform import, terraform apply -y, terraform destroy
- teraform init, terraform plan -auto-apply, terraform rm
- terraform init, terraform run -auto-approve, terraform destroy
- terraform init, terraform apply -auto-approve, terraform destroy
- terraform import, terraform apply -y, terraform rm

The correct answer is **d) terraform init, terraform apply -auto-approve, terraform destroy.** 

Here's why:

- **terraform init**: This command initializes the working directory. It downloads the necessary provider plugins (like AWS, Azure, GCP), sets up the backend for state storage, and initializes any modules used in your Terraform configuration. This is the crucial first step.

- **terraform apply -auto-approve:** This command applies the changes proposed in the execution plan. The `-auto-approve` flag automatically approves the plan, skipping the interactive confirmation prompt. While convenient for automation, it should be used with caution in production environments. `terraform plan` is used to generate the execution plan, and `terraform apply` is used to apply the changes. The `-auto-apply` option for `terraform plan` is deprecated.

- **terraform destroy:** This command destroys all resources managed by the current Terraform configuration. It's the standard way to remove infrastructure created by Terraform.

Let's look at why the other options are incorrect:

a) **terraform import, terraform apply -y, terraform destroy:** terraform import is used to import existing infrastructure into Terraform's state. It's not part of the standard creation workflow. -y is a shorthand for -auto-approve, but the init step is missing.

b) **terraform init, terraform plan -auto-apply, terraform rm**: As mentioned before, -auto-apply is not a valid option for terraform plan. terraform rm is not a standard Terraform command for destroying infrastructure.

c) **terraform init, terraform run -auto-approve, terraform destroy**: There is no terraform run command.

Therefore, option d correctly describes the workflow.

## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2025/homework/hw1
