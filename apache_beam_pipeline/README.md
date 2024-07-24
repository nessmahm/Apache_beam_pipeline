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

