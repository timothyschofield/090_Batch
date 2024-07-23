"""
    File: main_batch_app.py
    Author: Tim Schofield
    Date: 23 July 2024  

    This is the main entry point to the image batch application.
    
    This program takes a source CSV with a column of URLs, generates a JSONL input file from the CSV and uploades it for batch processing.
    The batch is then processes - it is OCRed by OpenAI with the prompt and model provided.

    The batch ststus is polled every few seconds or minutes and when it's status changes to "completed"
    the resultant data is downloaded and saved in total as a JSONL output file.
    A CSV file of certain important values is also created - such as the OCR output and unique identifier.

"""
from App import App
        
  
app1 = App()
  
source_csv_file_name = "NY_specimens_to_transcribe_test.csv"
image_col = "DarImageURL"

# If you don't have a unique column to pass into unique_id_col
# pass in the batch_name and the lines will be uniquely identified as batch_name-0, batch_name-1, batch_name-2, etc.
unique_id_col = "DarCatalogNumber"
batch_name = "test_batch"
num_images_to_batch = 10

app1.create_source_image_jsonl(source_csv_file_name=source_csv_file_name, 
                               image_col=image_col, 
                               batch_name=batch_name, 
                               unique_id_col=unique_id_col, 
                               num_images_to_batch=num_images_to_batch)  
