# -*- coding: utf-8 -*-
"""
Created on Sat Sep 12 17:49:20 2020

@author: xingy
"""

#!/usr/bin/env python

import argparse
import datetime
import json
import logging

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import pubsub
from google.cloud import bigquery

PROJECT_NAME='my-first-gcp-project-271812'
BUCKET_NAME='my-first-gcp-project-271812'

class GroupWindowsIntoBatches(beam.PTransform):
    """A composite transform that groups Pub/Sub messages based on publish
    time and outputs a list of dictionaries, where each contains one message
    and its publish timestamp.
    """
    def __init__(self, window_size):
        # Convert minutes into seconds.
        self.window_size = int(window_size * 60)

    def expand(self, pcoll):
        return (
            pcoll
            # Assigns window info to each Pub/Sub message based on its
            # publish timestamp.
            | "Window into Fixed Intervals" >> beam.WindowInto(window.FixedWindows(self.window_size))
            | "Add timestamps to messages" >> beam.ParDo(AddTimestamps())
            # Use a dummy key to group the elements in the same window.
            # Note that all the elements in one window must fit into memory
            # for this. If the windowed elements do not fit into memory,
            # please consider using `beam.util.BatchElements`.
            # https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.util.html#apache_beam.transforms.util.BatchElements
            | "Add Dummy Key" >> beam.Map(lambda elem: (None, elem))
            | "Groupby" >> beam.GroupByKey()
            | "Abandon Dummy Key" >> beam.MapTuple(lambda _, val: val)
        )
        

class AddTimestamps(beam.DoFn):
    def process(self, element, publish_time=beam.DoFn.TimestampParam):
        """Processes each incoming windowed element by extracting the Pub/Sub
        message and its publish timestamp into a dictionary. `publish_time`
        defaults to the publish timestamp returned by the Pub/Sub server. It
        is bound to each element by Beam at runtime.
        """

        yield {
            "message_body": element.decode("utf-8"),
            "publish_time": datetime.datetime.utcfromtimestamp(
                float(publish_time)
            ).strftime("%Y-%m-%d %H:%M:%S.%f"),
        }
        
def parse_json(line):
    '''Converts line from PubSub back to dictionary
    '''
    d={}
    d["publish_time"] = line["publish_time"]   
    l = line["message_body"][:-1].split(",")
    n = ["timestamp",	"latitude",	"longitude",	"highway",	"direction",	"lane",	"speed"]
    
    for i,x in enumerate(l):
        d[n[i]] = x
        
    return d  

class WriteBatchesToGCS(beam.DoFn):
    def __init__(self, output_path):
        self.output_path = output_path

    def process(self, batch, window=beam.DoFn.WindowParam):
        """Write one batch per file to a Google Cloud Storage bucket. """

        ts_format = "%H:%M"
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        filename = "-".join([self.output_path, window_start, window_end])

        with beam.io.gcp.gcsio.GcsIO().open(filename=filename, mode="w") as f:
            for element in batch:
                r = parse_json(element)
                f.write("{}\n".format(json.dumps(r)).encode("utf-8"))
                #f.write("{}\n".format(json.dumps(element)).encode("utf-8"))

class WriteBatchesToBQ(beam.DoFn):
    def __init__(self,  output_table):
       
        self.output_table = output_table
     
       
    def process(self, batch, window=beam.DoFn.WindowParam):
        client = bigquery.Client()
        table = client.get_table("my-first-gcp-project-271812.test.test6")
        rows_to_insert = []
        for element in batch:
            row = parse_json(element)
            rows_to_insert.append(row)
            
        errors = client.insert_rows( table, 
                                    rows_to_insert, 
                                    row_ids=[None] * len(rows_to_insert)
                                         )
                

def run(input_topic, output_path, output_table, window_size=1.0, pipeline_args=None):
    
    pipeline_options = PipelineOptions(pipeline_args, streaming=True, save_main_session=True)

    with beam.Pipeline(options=pipeline_options) as pipeline:      
        batches = (
                    pipeline
                    | "Read PubSub Messages" >> beam.io.ReadFromPubSub(topic="projects/my-first-gcp-project-271812/topics/sandiego")
                    | "Window into Batches" >> GroupWindowsIntoBatches(window_size)  
                   )
        
        batches | "Write to GCS" >> beam.ParDo(WriteBatchesToGCS(output_path))  
        batches | "Write to BigQuery" >> beam.ParDo(WriteBatchesToBQ(output_table)) 
        
if __name__ == "__main__":

    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input_topic",
        help="The Cloud Pub/Sub topic to read from.\n"
        '"projects/my-first-gcp-project-271812/topics/sandiago".',
    )
    
    
    parser.add_argument(
        '--output_table', 
        default='my-first-gcp-project-271812:sim_traffic.current_conditions',
        help='Output BigQuery table for results specified as: PROJECT:DATASET.TABLE'
    )
    
    parser.add_argument("--window_size", type=float, default=1.0, help="Output file's window size in number of minutes.",)
    parser.add_argument("--output_path", default="gs://my-first-gcp-project-271812/tmp/simtraffic", help="GCS Path of the output file including filename prefix.",)
    
    

    known_args, pipeline_args = parser.parse_known_args()
    run(known_args.input_topic, known_args.output_path, known_args.output_table, known_args.window_size,  pipeline_args=None)
