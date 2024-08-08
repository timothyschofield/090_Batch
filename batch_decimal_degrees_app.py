"""
    File: batch_decimal_degrees_app.py
    Author: Tim Schofield
    Date: 6 August 2024
    
    Batch version of 015_Fields_Peru_DMS
    
    
"""
from batch_package import App

app1 = App() 


prompt = (
        f"Determine the decimal latitude and longitude from the location description."
        f"Return the data in JSON format with 'latitude' and 'longitude' as keys."
        f"Return the accuracy of the location in meters in the 'accuracy_meters' field."
        f"If you can not find a value for a key return the string 'None' as the value."
        )

# Not including to_line
batch1_data = {"batch_name": "peru_dms_test", 
            "batch_type": "TEXT",  
            "source_csv_path": "batch_source_csv/Peru_field_from_Frank.csv", 
            "from_line": 100, 
            "to_line": 200,
            "source_csv_image_col": "AI_verbatim", 
            "source_csv_unique_id_col": "barcode", 
            "model": "gpt-4o", 
            "prompt": prompt, 
            "max_tokens": 4096, 
            "endpoint": "/v1/chat/completions"}



app1.do_batch(batch1_data)
app1.start_batches()


















