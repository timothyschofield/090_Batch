"""
    BatchController.py
    Author: Tim Schofield
    Date: 24 July 2024 

"""

from pathlib import Path 
import pandas as pd
import os
from helper_functions_batch import get_file_timestamp,are_keys_valid, get_headers, save_dataframe_to_csv, path_exists
import time, threading

from Batch import Batch

class BatchController:
    def __init__(self, openai_client):
        
        self.openai_client = openai_client
        
        INPUT_FOLDER = path_exists(Path(f"batch_input"))
        OUTPUT_FOLDER = path_exists(Path(f"batch_output"))
        
        self.batch_list = dict()
        
        self.input_folder = INPUT_FOLDER
        self.output_folder = OUTPUT_FOLDER
        
        self.check_status_delay = 5 # seconds
        self.start_time = None
    """
    """
    def do_batch(self, batch_data):   
        
        batch_name = batch_data["batch_name"]
        if batch_name in self.batch_list.keys():
            print(f"ERROR {batch_name}: Batch name must be unique - {batch_name} already exists.")
            print(f"{batch_name} is not uploaded.")
            return False
        
        this_batch = self.add_batch(batch_data)
        this_batch.do_batch()

    """
        Add a Batch to the BatchController
    """    
    def add_batch(self, batch_data):
        
        this_batch = Batch(openai_client=self.openai_client, input_folder=self.input_folder, output_folder=self.output_folder, batch_data=batch_data)
        
        batch_name = batch_data["batch_name"]
        self.batch_list[batch_name] = this_batch
        
        return this_batch
    
    """
        This must be called after creating all the Batches to monitor the Batches' progress
    """
    def start_batches(self):
        self.start_time = int(time.time())
        print(f"============ BATCHES STARTED: {time.ctime()} ============")
        print(f"Number of Batches: {len(self.batch_list)}")
        self.check_status()
    
    """
        Check the status of all the batches every few seconds/minutes
        When a Batch status becomes "completed" the JSONP and CSV results files is be downloaded
    """
    def check_status(self):
        print(time.ctime())
        
        processing_count = 0
        for batch_name, batch in self.batch_list.items():
            
            batch_info_response = batch.get_status()
            print(f"name: {batch_name}, status: {batch_info_response.status}, request_counts: {batch_info_response.request_counts}")

            if batch.app_batch_status == "processing":
                processing_count = processing_count + 1
                if batch_info_response.status == "completed":
                    batch.app_batch_status = "downloaded"
                    batch.download()
                        
        print("----------------------------")
        if processing_count == 0:
            print("============ ALL BATCHES COMPLETED ============")
            end_time = int(time.time())
            print(f"Processing time: {end_time - self.start_time} seconds")
            exit()
        else: threading.Timer(self.check_status_delay, self.check_status).start()
        
        
        
        
    
    """
    """
    def display_openai_batches(self):    
        active_batches = self.openai_client.batches.list()
        print("---------------------------------")
        for batch in active_batches.data:
            print(f"id: {batch.id}, status: {batch.status}, {batch.request_counts}, output_file_id: {batch.output_file_id}")
        print("---------------------------------")