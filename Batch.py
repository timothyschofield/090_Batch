"""
    Batch.py
    Author: Tim Schofield
    Date: 24 July 2024 

    Batch does not inherit from BatchController, 
    they has a parts-of relationship

"""

from pathlib import Path 
import pandas as pd
import os
from helper_functions_batch import get_file_timestamp,are_keys_valid, get_headers, save_dataframe_to_csv


class Batch():
    def __init__(self, name):
        self.name = name
       
    """
    """ 
    def upload():
        pass 
      
    """
    """ 
    def start():
        pass        
        
    """
    """ 
    def get_status():
        pass         
        
    """
    """ 
    def download():
        pass        
        
    """
    """ 
    def delete():
        pass          
        
        
        
        
        
        
        
        