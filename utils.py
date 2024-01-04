import datetime, json, os
import pandas as pd

def log(msg:str, log_path:str, log_file:bool=True, log_terminal:bool=False):
    if log_terminal:
        print(msg, flush=True)
    if log_file:
        with open(log_path, 'a') as file:
            file.write(f'{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}    {msg}\n')


def prep_dir(dir_list):
    for directory in dir_list:
        if not os.path.exists(directory):
            os.mkdir(directory)

def read_raw_json(path):
    with open(path) as file:
        return json.load(file)["data"]
    
def get_dir_from_file_path(path):
    return path.removesuffix(path.split("/")[-1])

def fix_df_num(df): return df.map(lambda x: pd.to_numeric(x, errors='ignore'))
