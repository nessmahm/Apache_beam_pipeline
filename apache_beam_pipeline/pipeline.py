import statistics
import threading
import os
from dotenv import load_dotenv

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from elasticsearch import Elasticsearch
load_dotenv()


class IndexToElasticsearch(beam.DoFn):
    def __init__(self, es_client, index_name):
        self.es_client = es_client
        self.index_name = index_name

    def process(self, element):
        doc = {
            "PULocationID": element[0],
            "DOLocationID": element[1],
            "AVG_far_amount": element[2],
            "AVG_distance": element[3]
        }
        yield doc


ENDPOINT = os.getenv('ENDPOINT_URL')
API_KEY = os.getenv('API_KEY')
INDEX_NAME = "nyc_taxi_trip_data"

class SingletonElasticSearch:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, endpoint=None, api_key=None):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(SingletonElasticSearch, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self, endpoint=None, api_key=None):
        if not self._initialized:
            try:
                self.client = Elasticsearch(endpoint, api_key=api_key)
                self.endpoint = endpoint
                self.api_key = api_key
                self._initialized = True
                print("Elasticsearch clinet instance created ! ")
            except Exception as e:
                print("Error occured while creating Elasticsearch client : ",str(e))


    def get_client(self):
        return self.client

class DocumentInsertion(beam.DoFn):

    def process(self, item):
        try:
            client = SingletonElasticSearch(ENDPOINT, API_KEY)
            client = client.get_client()
            client.index(
                index=INDEX_NAME,
                document=item)
            print("Document insertion done  succefully !")
        except Exception as e:
            print("An error occured while writing into Elasticsearch: ",str(e))
        yield item
def run_pipeline():
    try :
        options = PipelineOptions()
        options.view_as(SetupOptions).save_main_session = True
        print("Starting data processing !")

        with beam.Pipeline(options=options) as p:
            # Read from the CSV file
            lines = p | 'Read' >> beam.io.ReadFromText('apache_beam_pipeline/yellow_tripdata_2022-01.csv',
                                                       skip_header_lines=1)

            # Parse CSV lines
            parsed_lines = lines | 'ParseCSV' >> beam.Map(lambda line: line.split(','))

            # 1. Filter trips with distance >= 0.1 miles
            filtered_trips = (
                    parsed_lines
                    | 'FilterDistance' >> beam.Map(lambda fields: fields)
                    | 'Filter' >> beam.Filter(lambda field: float(field[4]) < 0.1)
                # | 'Print' >> beam.Map(print)  # Debugging step to see the output

            )
            print("Step 1 : Done !")
            # sum of fare amount and tip amount
            # 2. Calculate total fare amount and retain all fields
            trips_with_fare = (
                    filtered_trips
                    | 'AddTotalFare' >> beam.Map(lambda trip: trip + [float(trip[10]) + float(trip[14])])
            )
            print("Step 2 : Done !")
            # 3. Group the data by pickup location and calculate the average total fareamount.
            grouped_by_pickup_and_dropup = (
                    trips_with_fare
                    | 'GroupByPickup' >> beam.GroupBy(lambda s: (s[7], s[8]))

            )
            print("Step 3 : Done !")

            # 4. Group the data by dropoff location and calculate the average trip distance.
            calc_avg_total_fareamout = (
                    grouped_by_pickup_and_dropup
                    | 'avg_total_far_ammout' >> beam.Map(
                lambda result: (result[0], result[1], statistics.mean(subarray[-1] for subarray in result[1])))
                # | 'Print' >> beam.Map(print)  # Debugging step to see the output
            )

            print("Step 4 : Done !")

            calc_avg_total_total_distance = (
                    calc_avg_total_fareamout
                    | 'avg_total_distance' >> beam.Map(
                lambda result: {"PULocationID": result[0][0], "DOLocationID": result[0][1], "AVG_far_amount": result[2],
                                "AVG_distance": statistics.mean(float(subarray[4]) for subarray in result[1])})
                # | 'Print' >> beam.Map(print)  # Debugging step to see the output
            )
            print("Data Processing : Done !")

            calc_avg_total_total_distance | 'IndexToElasticsearch' >> beam.ParDo(DocumentInsertion())

    except Exception as e:
        print("Error occured while running pipeline : ", str(e))
def print_results_from_es(client, index_name='my_index'):
    try:
        client.indices.refresh(index=index_name)

        resp = client.search(index=index_name, scroll='2m',
                             size=5, query={"match_all": {}})
        print("Got %d Hits:" % resp['hits']['total']['value'])
        for hit in resp['hits']['hits']:
            print(hit["_source"])
    except Exception as e:
        print("Error occured while printing results :",str(e))


if __name__ == '__main__':
    run_pipeline()
    print("Print pipeline result :")
    client = SingletonElasticSearch(ENDPOINT, API_KEY)
    client = client.get_client()
    print_results_from_es(client, INDEX_NAME)

    print('Finished !')