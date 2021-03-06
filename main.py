import pandas as pd
from config import *
import csv

def get_dict_from_csv(path):
    result_dict = {}
    with open(path) as f:
        reader = csv.reader(f, delimiter=",")
        count = 0
        for row in reader:
            if count != 0:
                result_dict[row[0]] = row
            count += 1
    return result_dict

def get_deltas():
    old_data = get_dict_from_csv(SOURCE_PATH)
    new_data = get_dict_from_csv(TARGET_PATH)

    deltas = []

    for key,val in new_data.items():
        if old_data.get(key) is None:
            deltas += val
        else:
            diff_exists = False
            for i in range(len(val)):
                if old_data[key][i] != val[i]:
                    diff_exists = True
                    break
            if diff_exists is True:
                deltas += val

    return deltas
