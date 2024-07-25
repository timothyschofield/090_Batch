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
    def __init__(self):
        
        INPUT_FOLDER = path_exists(Path(f"batch_input"))
        OUTPUT_FOLDER = path_exists(Path(f"batch_output"))
        
        self.batch_list = dict()
        
        self.input_folder = INPUT_FOLDER
        self.output_folder = OUTPUT_FOLDER
        
    """
    """
    def do_batch(self, batch_name, source_csv_path, source_csv_image_col, source_csv_unique_id_col, model, prompt, max_tokens):   
        this_batch = self.add_batch(batch_name, source_csv_path, source_csv_image_col, source_csv_unique_id_col, model, prompt, max_tokens)
        this_batch.do_batch()
    """
        Add a Batch to the BatchController
    """    
    def add_batch(self, batch_name, source_csv_path, source_csv_image_col, source_csv_unique_id_col, model, prompt, max_tokens):
        this_batch = Batch(self.input_folder, self.output_folder, batch_name, source_csv_path, source_csv_image_col, source_csv_unique_id_col, model, prompt, max_tokens)
        self.batch_list[batch_name] = this_batch
        
        return this_batch

      
   
    
    