# Apache Beam Pipeline Project


## Prerequisites

Ensure you have the following installed:
- Docker
- Python 3

## Setup Instructions

## 1. Prepare Your Environment

### Copy the Dataset

Place your `yellow_tripdata_2022-01.csv` dataset inside the `apache_beam_pipeline` folder.

### Create the `.env` File

Create a `.env` file in the project root directory with the following environment variables:
You can follow these instructions to get your credentials:
`https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/getting-started-python.html`

```plaintext
ENDPOINT_URL=your_endpoint_here
API_KEY=your_api_key_here
```
### Install Python Dependencies
Install the required Python packages listed in requirements.txt by running:

```
pip install -r requirements.txt
```

## 2. Running the Project Inside Docker

## Build the Docker Image
Build the Docker image using the following command:

```
docker build -t apache_beam_pipeline -f ./docker/dockerfile .
```

This command will create a Docker image named apache_beam_pipeline using the Dockerfile located in the docker directory.

## Run the Docker Container
After building the image, run the Docker container with:

```
docker run -it apache_beam_pipeline
```
This command starts a new container from the apache_beam_pipeline image and opens an interactive terminal.

