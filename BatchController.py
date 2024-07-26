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
        
        self.check_status()
        
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
        Check the status of all the batches every few seconds/minutes
        When a Batch status becomes "completed" the JSONP results file can be downloaded
        
        Need to check that all batches have been up created and returned batch_id properly
        before starting this loop
    """
    def check_status(self):
        print(time.ctime())
        
        """
        if len(self.batch_list) == 0:
            print(f"NO BATCHES TO PROCESS: EXIT")
            exit()
        """
          
        for batch_name, batch in self.batch_list.items():
            
            # You have to check for batch_id != None to make sure the batch has been uploaded, created and come back with batch_id
            # because uploading and creating a batch is an asyncronouse process
            if batch.batch_id != None:
                batch_info_response = batch.get_status()
                print(f"name: {batch_name}, status: {batch_info_response.status}, request_counts: {batch_info_response.request_counts}")
                
        print("----------------------------")
        threading.Timer(self.check_status_delay, self.check_status).start()       
        