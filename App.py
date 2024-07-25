"""
    App.py
    Author: Tim Schofield
    Date: 23 July 2024 
"""
from pathlib import Path 
import pandas as pd
import os
from helper_functions_batch import get_file_timestamp,are_keys_valid, get_headers, save_dataframe_to_csv

from BatchController import BatchController

class App:
    def __init__(self):

        self.batch_controller = BatchController()
        
    """
    """
    def do_batch(self, batch_name, source_csv_path, source_csv_image_col, source_csv_unique_id_col, model, prompt, max_tokens):
        self.batch_controller.do_batch(batch_name, source_csv_path, source_csv_image_col, source_csv_unique_id_col, model, prompt, max_tokens)
        