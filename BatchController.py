"""
    BatchController.py
    Author: Tim Schofield
    Date: 24 July 2024 

"""

from pathlib import Path 
import pandas as pd
import os
from helper_functions_batch import get_file_timestamp,are_keys_valid, get_headers, save_dataframe_to_csv

from Batch import Batch

class BatchController:
    def __init__(self, paths):
        
        self.paths = paths
        self.batch_list = dict()
        
    """
        Add a Batch to the BatchController
    """    
    def add_batch(self, batch_name):
        self.batch_list[batch_name] = Batch(batch_name)
        
    """
        Process the CSV and create a JSONL Batch file
    """
    def create_batch_file(self, batch_name):
        pass
        
    """
        Upload the JSONL Batch file to OpenAI Batch processing
    """    
    def upload_batch(self, batch_name):
        self.batchList[batch_name].upload()
        
    """
        Tell OpenAI to start processing the Batch
    """    
    def start_batch(self, batch_name):
        self.batchList[batch_name].start()
        
    """
        Get the Batch status - including if it is complete
    """    
    def get_batch_status(self, batch_name):
        self.batchList[batch_name].get_status()
        
    """
        Get the JSONL data resulting from a completed Batch
    """   
    def download_batch(self, batch_name):
        self.batchList[batch_name].download()
         
    """
        Delete a Batch from OpenAI
    """    
    def delete_batch(self, batch_name):
        self.batchList[batch_name].delete() 
        
        
   
    
    