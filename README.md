# Precious metals price ML model trainer

Airflow ELT pipeline to train price prediction model.


## Tech Stack

- Apache Airflow
- Docker
- Google Cloud Storage
- BigQuery


## What it does

- Produces ARIMA ML models for "XAUUSD", "XAGUSD", "XPTUSD", "XPDUSD" tickers. 


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
 

## Getting Started

To run this project locally, follow these steps:

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/TuringCollegeSubmissions/jukaral-DE2.4.git
   ```

2. **Navigate to the Project Directory:**

    ```bash
    cd jukaral-DE2.4
    ```

3. **Start the Docker Containers:**
    - Run the following command in your terminal, from the root of the project:

    ```bash
    docker compose up
    ```

4. **Variables and connections:**
    - With the container running - create the default filesystem (fs) connection with the following:
        - Conn id: ```fs_default```
        - Leave everything else blank.

    - Import the provided default 'variables.json' to your Variables for quick start.
    You'll have to edit them as needed.

5. **Set up your GCP project:**
    - This pipeline utilises GCP (Big Query and GCS).
    - It is suggested to create two GCS buckets: one for production data and one for backups.
    - Create a dataset and a table for the data.
    - Create a view with the name of ```latest_12``` with the following query:
    
    ```sql
    SELECT * FROM `your_project.your_dataset.your_data_table` ORDER BY data_datetime DESC LIMIT 12
    ```

    - You have to generate a service account key JSON file.
    - Do not forget to set up IAM principles for the service account, fastest way would be
    giving Big Query Admin and Storage Admin roles.
    - Create a GCP connection in Airflow with the following:
        - Conn id: ```google_cloud_default```
        - Keyfile path: ```path/to/service_account.json```, eg: ```/opt/airflow/config/service_account.json```.
            This would mean you have to place your file in the ```config``` folder that's at the root of project folder.
    - Do not forget to edit the Variables according to your bucket and table namings.
