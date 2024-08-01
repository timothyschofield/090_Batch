"""
    App.py
    Author: Tim Schofield
    Date: 23 July 2024 
"""
from pathlib import Path  
import pandas as pd
import os
from dotenv import load_dotenv
from openai import OpenAI
 
from batch_package import BatchController

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

        self.batch_controller = BatchController.BatchController(self.openai_client)
        
    """
    """
    def do_batch_from_csv(self, batch_data):
        self.batch_controller.do_batch_from_csv(batch_data)
   
    """
    """
    def start_batches(self):
        self.batch_controller.start_batches()
        
    """
    """
    def display_openai_batches(self):   
        self.batch_controller.display_openai_batches()
        