# Running the Apache Beam Pipeline

To successfully run the Apache Beam pipeline, follow these steps:

## 1. Prepare Your Environment

### Copy the Dataset

Place your `yellow_tripdata_2022-01.csv` dataset inside the `apache_beam_pipeline` folder.

### Create the `.env` File

Create a `.env` file in the project root directory with the following environment variables:

```plaintext
ENDPOINT_URL=your_endpoint_here
API_KEY=your_api_key_here
```
### Install Python Dependencies
Install the required Python packages listed in requirements.txt by running:

```
pip install -r requirements.txt

```

## 2. Run the Pipeline
Execute the Apache Beam pipeline script using the following command:
```
python apache_beam_pipeline/pipeline.py
```

# Code Explanation
### 1.SingletonElasticSearch Class
The SingletonElasticSearch class is designed to ensure that only one instance of the Elasticsearch client is created.
It uses the Singleton design pattern, which provides a way to create a single instance of a class and ensure that it is reused throughout the application.
This helps in managing the Elasticsearch client efficiently, especially in a multi-threaded environment.

### 2.DocumentInsertion Class
The DocumentInsertion class is a custom beam.DoFn function used to insert documents into an Elasticsearch index.
It is executed at the end of the Apache Beam pipeline to save the processed data into Elasticsearch.
Each document is inserted into the specified index using the Elasticsearch client.index().

### 3.run_pipeline() Function
The run_pipeline() function handles all the data transformation tasks based on the provided instructions.
It performs the following steps:

#### Read the CSV File: Reads data from the CSV file specified.
#### Parse CSV Lines: Splits each line into fields.
#### Filter Trips: Filters trips based on the trip distance (<01 miles) .
#### Calculate the total fare amount for each trip.
#### Group Data: Groups data by pickup and dropoff locations
#### calculates average fare and distance.
#### Finally, the processed data is inserted into the specified Elasticsearch index using the DocumentInsertion class.

### 4.print_results_from_es Function
The print_results_from_es() function retrieves and prints the results stored in the Elasticsearch index.
It performs a search query to fetch documents from the specified index and prints each document's contents to the console.
