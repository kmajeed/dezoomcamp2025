# The libraries required for the script]
import psycopg2
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import orm
from sqlalchemy.orm import sessionmaker
from urllib.request import urlretrieve
import gzip
import pandas as pd
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# Configure logging
# logging.basicConfig(level=logging.INFO)


# Define database connection details (use environment variables for security)
# DB_HOST = os.environ.get("DB_HOST", "db")  # Default to "db" for Docker
# DB_PORT = int(os.environ.get("DB_PORT", 5432))
# DB_USER = os.environ.get("DB_USER", "postgres")
# DB_PASSWORD = os.environ.get("DB_PASSWORD", "postgres")
# DB_NAME = os.environ.get("DB_NAME", "ny_taxi")

# For the purpose of this work I'll use locally defined variables
DB_HOST = "db"
DB_PORT = 5432
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_NAME = "ny_taxi"

# Define table names and column mappings
GREEN_TAXI_TABLE_NAME = "green_taxi_trips"
ZONE_LOOKUP_TABLE_NAME = "taxi_zones"

# cols in csv file
#,VendorID,
# lpep_pickup_datetime,lpep_dropoff_datetime,
# store_and_fwd_flag,
# RatecodeID,
# PULocationID,DOLocationID,passenger_count,trip_distance,fare_amount,extra,mta_tax,
# tip_amount,tolls_amount,ehail_fee,improvement_surcharge,total_amount,payment_type,trip_type,congestion_surcharge


GREEN_TAXI_COLUMN_MAPPINGS = {
    "VendorID": "vendor_id",
    "lpep_pickup_datetime": "pickup_datetime",
    "lpep_dropoff_datetime": "dropoff_datetime",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "RatecodeID": "ratecode_id",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "fare_amount": "fare_amount", 
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "ehail_fee": "ehail_fee",    
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "payment_type": "payment_type",
    "trip_type":"trip_type",
    "congestion_surcharge": "congestion_surcharge",
}

ZONE_LOOKUP_COLUMN_MAPPINGS = {
    "LocationID": "location_id",
    "Borough": "borough",
    "Zone": "zone",
    "service_zone": "service_zone",
}
CHUNK_SIZE = 100000

# Define data URLs and filenames
GREEN_TAXI_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz"
ZONE_LOOKUP_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
GREEN_TAXI_FILENAME = "green_tripdata_2019-10.csv.gz"
ZONE_LOOKUP_FILENAME = "taxi_zone_lookup.csv"

def download_data(url, filename):
    """Downloads data from the given URL and saves it to the specified filename."""
    logging.info(f"Downloading data from {url}...")
    urlretrieve(url, filename)
    logging.info(f"Download complete: {filename}")

def decompress_data(filename):
    """Decompresses a gzipped file."""
    with gzip.open(filename, 'rb') as f_in, open(filename[:-3], 'wb') as f_out:
        f_out.write(f_in.read())
    logging.info(f"File unzip complete: {filename}")

def create_database_engine():
    """Creates a SQLAlchemy engine."""
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    return engine

#
#    """Creates the tables for green taxi trips and zone lookup data in the database."""
Base = orm.declarative_base()

class GreenTaxiTrip(Base):
    __tablename__ = GREEN_TAXI_TABLE_NAME
    trip_id = Column(Integer, primary_key=True, autoincrement=True)  # Auto-generated primary key    
    vendor_id = Column(Integer) # , primary_key=True
    pickup_datetime = Column(DateTime)
    dropoff_datetime = Column(DateTime)
    store_and_fwd_flag = Column(String)
    ratecode_id = Column(Integer)
    pickup_location_id = Column(Integer)
    dropoff_location_id = Column(Integer)
    passenger_count = Column(Integer)
    trip_distance = Column(Float)
    fare_amount = Column(Float)
    extra = Column(Float)
    mta_tax = Column(Float)
    tip_amount = Column(Float)
    tolls_amount = Column(Float)
    ehail_fee = Column(Float) # Make it nullable
    improvement_surcharge = Column(Float)
    total_amount = Column(Float)
    payment_type = Column(Integer)
    trip_type= Column(Integer)
    congestion_surcharge = Column(Float)

class ZoneLookup(Base):
    __tablename__ = ZONE_LOOKUP_TABLE_NAME

    location_id = Column(Integer, primary_key=True)
    borough = Column(String)
    zone = Column(String)
    service_zone = Column(String)

def create_tables(engine):
    Base.metadata.create_all(engine)
    logging.info("Tables created successfully.")

def ingest_data(engine, filename, table_name, column_mappings, table_class):
    """Ingests data from a CSV file into the database."""
    logging.info(f"Ingesting data into {table_name}...")
    try:
        df_chunks = pd.read_csv(filename, chunksize=CHUNK_SIZE, dtype='str')  # Read in chunks, force string type initially
        for i, chunk in enumerate(df_chunks):
            logging.info(f"Processing chunk {i+1}...")
            # Rename columns
            chunk = chunk.rename(columns=column_mappings)

            # Convert columns to correct data types
            for col, dtype in table_class.__table__.columns.items():
                if col in chunk.columns:
                    if dtype.type.__class__.__name__ == 'DateTime':
                         chunk[col] = pd.to_datetime(chunk[col], errors='coerce') #convert to datetime
                    elif dtype.type.__class__.__name__ in ('Integer', 'Float'):
                        chunk[col] = pd.to_numeric(chunk[col], errors='coerce') #Convert to numeric

            chunk.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            logging.info(f"Chunk {i+1} ingested.")
        logging.info(f"Data ingestion into {table_name} complete.")
    except Exception as e:
        logging.error(f"Error ingesting data: {e}")


def main():
    """Main function"""
    try:
        engine = create_database_engine()
        create_tables(engine)

        download_data(GREEN_TAXI_URL, GREEN_TAXI_FILENAME)
        decompress_data(GREEN_TAXI_FILENAME)
        ingest_data(engine, GREEN_TAXI_FILENAME[:-3], GREEN_TAXI_TABLE_NAME, GREEN_TAXI_COLUMN_MAPPINGS, GreenTaxiTrip)

        download_data(ZONE_LOOKUP_URL, ZONE_LOOKUP_FILENAME)
        ingest_data(engine, ZONE_LOOKUP_FILENAME, ZONE_LOOKUP_TABLE_NAME, ZONE_LOOKUP_COLUMN_MAPPINGS, ZoneLookup)

        logging.info("All operations completed successfully.")

    except Exception as e:
        logging.error(f"A critical error occurred: {e}")

if __name__ == "__main__":
    main()