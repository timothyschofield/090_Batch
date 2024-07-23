"""
    File: main_batch_app.py
    Author: Tim Schofield
    Date: 23 July 2024  

    This is the main entry point to the image batch application.
    This uses the OpenAI Batch processing API
    
    It is a wrapper around some of the methods demonstrated in main_batch_basics.py and main_batch_image.py - but not dependant on them.
    
    This App takes a source CSV with a column of URLs, generates a JSONL input file from the CSV and uploades it for batch processing.
    The batch is then processes - it is OCRed by OpenAI API with the prompt and model provided.

    During processing the batch status is polled every few seconds or minutes and when it's status changes to "completed"
    the resultant data is downloaded and saved in it's entirety as a JSONL output file.
    A CSV file of certain useful values in the data is also created - such as the OCR output and unique identifier.

"""
from App import App
app1 = App()
  
source_csv_file_name = "NY_specimens_to_transcribe_test.csv"
image_col = "DarImageURL"

# If you don't have a unique id column to pass into unique_id_col,  pass in "custom_id"
# The JSONL lines will then be uniquely identified as custom_id-0, custom_id-1, custom_id-2, etc.
unique_id_col = "DarCatalogNumber"

app1.create_source_jsonl(source_csv_file_name=source_csv_file_name, image_col=image_col, unique_id_col=unique_id_col)  




