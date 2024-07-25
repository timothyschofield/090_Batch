"""
    BatchController.py
    Author: Tim Schofield
    Date: 24 July 2024 

"""

from pathlib import Path 
import pandas as pd
import os
from helper_functions_batch import get_file_timestamp,are_keys_valid, get_headers, save_dataframe_to_csv, path_exists

from Batch import Batch

class BatchController:
    def __init__(self, openai_client):
        
        self.openai_client = openai_client
        
        INPUT_FOLDER = path_exists(Path(f"batch_input"))
        OUTPUT_FOLDER = path_exists(Path(f"batch_output"))
        
        self.batch_list = dict()
        
        self.input_folder = INPUT_FOLDER
        self.output_folder = OUTPUT_FOLDER
        
    """
    """
    def do_batch(self, batch_name, source_csv_path, source_csv_image_col, source_csv_unique_id_col, model, prompt, max_tokens, endpoint):   
        
        if batch_name in self.batch_list.keys():
            print(f"ERROR {batch_name}: Batch name must be unique - {batch_name} already exists.")
            print(f"{batch_name} is not uploaded.")
            return False
        
        this_batch = self.add_batch(batch_name, source_csv_path, source_csv_image_col, source_csv_unique_id_col, model, prompt, max_tokens, endpoint)
        this_batch.do_batch()
    """
        Add a Batch to the BatchController
    """    
    def add_batch(self, batch_name, source_csv_path, source_csv_image_col, source_csv_unique_id_col, model, prompt, max_tokens, endpoint):
        this_batch = Batch(openai_client=self.openai_client, 
                           input_folder=self.input_folder, 
                           output_folder=self.output_folder, 
                           batch_name=batch_name, 
                           source_csv_path=source_csv_path, 
                           source_csv_image_col=source_csv_image_col, 
                           source_csv_unique_id_col=source_csv_unique_id_col, 
                           model=model, 
                           prompt=prompt, 
                           max_tokens=max_tokens,
                           endpoint=endpoint)
        
        self.batch_list[batch_name] = this_batch
        
        return this_batch

      
   
    
    