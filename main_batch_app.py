"""
    File: main_batch_app.py
    Author: Tim Schofield
    Date: 23 July 2024  

    This is the main entry point to the image batch application

"""
from App import App
        
  
app1 = App()
  
source_csv_file_name = "NY_specimens_to_transcribe_test.csv"
image_col = "DarImageURL"
num_images_to_batch = 10
app1.create_source_image_jsonl(source_csv_file_name, image_col, num_images_to_batch)  
  
        