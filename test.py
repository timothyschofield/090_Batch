"""

Starting with a 45 line NY_specimens_to_transcribe_small.csv
test_batch_all_45_lines_output.jsonl had 9 failures and is only 36 lines long

Write program which starts with these two files and
1) identifies which lines are missing
2) makes a NY_specimens_to_transcribe_small_fix_01.csv
    with just the missing lines in      

"""
from pathlib import Path 
import pandas as pd
from batch_package import batch_utils
import json

endpoint = "/v1/chat/completions"
model = "gpt-4o"
max_tokens = 4096

batch_name = "test_batch_all_45_lines"

# This is the origonal, complete, JSONL input file
input_folder = Path("batch_input") 
complete_file = Path(f"{batch_name}_input.jsonl")
complete_file_path = batch_utils.path_exists(Path(f"{input_folder}/{complete_file}"))

with open(complete_file_path, "r") as f:
    complete_jsonl_text = f.read()
complete_jsonl_list = complete_jsonl_text.splitlines()

# This is the file that has come back from Batch processing possibly with missing lines
output_folder = Path("batch_output")
incomplete_file = Path(f"{batch_name}_output.jsonl") 
incomplete_file_path = batch_utils.path_exists(Path(f"{output_folder}/{incomplete_file}"))

with open(incomplete_file_path, "r") as f:
    incomplete_jsonl_text = f.read()
incomplete_jsonl_list = incomplete_jsonl_text.splitlines()

numlines_complete = len(complete_jsonl_list)
numlines_incomplete = len(incomplete_jsonl_list)

print(f"\nNumlines complete {complete_file_path} {numlines_complete}")
print(f"Numlines incomplete {incomplete_file_path} {numlines_incomplete}\n")

if numlines_complete == numlines_incomplete:
    print(f"The output file: {incomplete_file_path} has the same number of lines as {complete_file_path} - so no fixing to do.")
else:
    print(f"The output file: {incomplete_file_path} has fewer lines than {complete_file_path}\nSo we need to do a FIXUP batch")

    complete_jsonl_json_list = []
    for jsonl_line in complete_jsonl_list:
        jsonl_dict_line = eval(jsonl_line)
        complete_jsonl_json_list.append(jsonl_dict_line)

    incomplete_jsonl_json_list = []
    for jsonl_line in incomplete_jsonl_list:
        
        jsonl_dict_line = eval(jsonl_line.replace('null', '0'))
        incomplete_jsonl_json_list.append(jsonl_dict_line["custom_id"])

    fixup_content = f""
    for row in complete_jsonl_json_list:
        if row["custom_id"] not in incomplete_jsonl_json_list:
            #print(f'Missing {row["custom_id"]}')
            row = json.dumps(row) # This is needed to turn JSON with single quotes into JSON with double quotes
            fixup_content = f"{fixup_content}{row}\n"


    #print(f"****{fixup_content}****")

fix_number = 1
fix_file_name = f"{batch_name}_input_fix_{fix_number}.jsonl"
fix_file_path = f"{input_folder}/{fix_file_name}"

print(f"WRITING: {fix_file_path}")
with open(fix_file_path, "w") as f:
    f.write(fixup_content) 
