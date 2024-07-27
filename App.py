"""
    App.py
    Author: Tim Schofield
    Date: 23 July 2024 
"""
from pathlib import Path 
import pandas as pd
import os
from helper_functions_batch import get_file_timestamp,are_keys_valid, get_headers, save_dataframe_to_csv
from dotenv import load_dotenv
from openai import OpenAI

from BatchController import BatchController

class App:
    def __init__(self):

        self.openai_client = None

        load_dotenv()
        try:
            my_api_key = os.environ['OPENAI_API_KEY']          
            self.openai_client = OpenAI(api_key=my_api_key)
        except Exception as ex:
            print("Exception:", ex)
            exit()

        self.batch_controller = BatchController(self.openai_client)
        
    """
    """
    def do_batch(self, batch_data):
        self.batch_controller.do_batch(batch_data)
   
    """
    """
    def start_batches(self):
        self.batch_controller.start_batches()
        
    """
    """
    def display_openai_batches(self):   
        self.batch_controller.display_openai_batches()