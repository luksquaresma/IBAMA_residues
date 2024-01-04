import datetime, json, os

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