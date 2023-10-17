# Precious metals price ML model trainer

ELT pipeline to train price prediction model.

## Tech Stack

- Apache Airflow
- Docker
- Google Cloud Storage
- BigQuery

## How it works (brief)
1. Periodically (hourly) fetches data from https://metalpriceapi.com/ API.
2. Transforms and stores the data on GCS and Big Query.
3. Retrieves the data from Big Query for ML training.
4. Backs up BigQuery and everything in 'data' folder periodically to a coldline GCS bucket.

## How it works (in-detail)

1. The data fetching/ELT DAG:
    - First we check if extracted data for the days hour does not exist yet.
    This tries to save precious API call count in case of downstream task failure.
    - Depending on the condition the DAG either then calls the API endpoint to retrieve the data
    or uses the existing stored extracted data file.
    - It then transforms the data into more useful structure and does one light calculation to have
    one troy ounce price per ticker.
    - The transformed data is stored in a dataset to trigger the downstream DAG.

2. Loading data to Big Query & GCS DAG:
    - The DAG is triggered by the ELT (producing) DAG via dataset.
    - It then stores (overwriting) it in a GCS bucket.
    - Then it uses a handy operator to load the data from the GCS into Big Query table.
    - As a last step - it creates a materialized table form a predefined view which returns latest 12 records.

3. Model training DAG:
    - The DAG also runs on an hourly schedule at the moment.
    - It senses for the materialized table to be created as a trigger.
    - Once the condition is met - it simultaniously saves a CSV file in GCS bucket and
    and returns it from the Big Query materialized table.
    - It then passes the data to model training operator to produce the model and saves
    them in a timestamped folder.
    - Last step deletes the materialized tables for next cycle.

4. Retransform data DAG:
    - Dynamic DAG that runs transformation on all locally stored extracted data.
    - Only use it when full data retransformation is necessary.

5. Data Backup DAG:
    - All backup data is stored in GCS Coldline versioned bucket set for 20 versions
    and 90 day expirity. 
    - Backups the BQ 'rates' table to the GCS.
    - Compresses 'data' folder.
    - Uploads the compressed 'data' zip to the GCS.
 