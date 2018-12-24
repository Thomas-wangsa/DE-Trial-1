import pandas as pd
import time
from datetime import datetime
import json
import os
import sys, getopt
import numpy as np
import collections

interval = 0
batch = 0

demographic = pd.read_excel("Dataset.xlsx", sheet_name="demographic")
base_path = os.getcwd() + "/input_file/"


def myconverter(o):
    if isinstance(o, datetime):
        return o.__str__()

def append_raw_data_call() :
    print("WAITING FOR APPENDING THE RAW DATA CALL")
    raw_data_call_1 = pd.read_excel("Dataset.xlsx", sheet_name="raw_data_call_1")
    raw_data_call_2 = pd.read_excel("Dataset.xlsx", sheet_name="raw_data_call_2")
    raw_data_call_3 = pd.read_excel("Dataset.xlsx", sheet_name="raw_data_call_3")
    raw_data_call_4 = pd.read_excel("Dataset.xlsx", sheet_name="raw_data_call_4")
    print("APPENDING THE RAW DATA SUCESS")
    return pd.concat([raw_data_call_1, raw_data_call_2, raw_data_call_3,raw_data_call_4])

raw_data_call = append_raw_data_call()

def create_parent_path() :
    parent_path = base_path
    # define the name of the directory to be created
    if not os.path.exists(parent_path):
        try:
            os.mkdir(parent_path)
            print ("Successfully created the directory %s " % parent_path)
        except OSError:
            print ("Creation of the directory %s failed" % parent_path)
            print OSError
            exit(1)


def create_user_path(user_path) :
    path = base_path+str(user_path)
    # define the name of the directory to be created
    if not os.path.exists(path):
        try:
            os.mkdir(path)
            print ("Successfully created the directory %s " % path)
        except OSError:
            print ("Creation of the directory %s failed" % path)
            print OSError
            exit(1)


def execution(i):
    start_iterate = 0 + i;
    for y in range(batch):
        index = y + start_iterate
        if (index + 1 > len(demographic.index)): break

        data = {}
        for title in list(demographic):
            if (title == "flag_bad"):
                data[title] = 'bad' if demographic[title][index] == 1 else 'good'
                continue
            data[title] = demographic[title][index]

        demographic_json = {
            'type': "demographic",
            'data': json.dumps(data, default=myconverter)
        }

        create_user_path(index+1)

        with open(os.getcwd() + "/input_file/"+str(index+1)+"/demographic.json", 'w') as outfile:
            json.dump(demographic_json, outfile)

        idx = raw_data_call.index[raw_data_call['user_id'] == index+1]
        call_log_data = raw_data_call.loc[raw_data_call['user_id'] == index+1]
        print call_log_data

        call_log_array = []

        if(len(call_log_data) > 0) :
            for quantity in range (len(call_log_data)) :
                #print quantity
                #print call_log_data['date'][idx[quantity]]
                if( pd.isnull(call_log_data['date'][idx[quantity]]) ) :
                    convert_date = None
                else :
                    ts = int(call_log_data['date'][idx[quantity]])
                    convert_date = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

                if( pd.isnull(call_log_data['duration'][idx[quantity]]) ) :
                    duration_result = None
                else :
                    duration_result = int(call_log_data['duration'][idx[quantity]])

                if ( pd.isnull(call_log_data['type_cat'][idx[quantity]]) ):
                    category_result = None
                else :
                    category_raw = int(call_log_data['type_cat'][idx[quantity]])
                    category_result = "undefined"

                    if(category_raw == 1) :
                        category_result = "incoming"
                    elif(category_raw == 2) :
                        category_result = "missed call"
                    elif(category_raw == 3) :
                        category_result = "outgoing"
                    elif(category_raw == 4) :
                        category_result = "unknown"
                    elif(category_raw == 5) :
                        category_result = "voicemail"

                call_log_sub_array = {
                    "date"      : convert_date,
                    "duration"  : duration_result,
                    "category"  : category_result
                }
                call_log_array.append(call_log_sub_array)

        call_log_json = {
            'type': 'call_log',
            'user_id': index + 1,
            'data': call_log_array
        }

        with open(os.getcwd() + "/input_file/"+str(index+1)+"/call_log.json", 'w') as outfile:
            json.dump(call_log_json, outfile)





def execution_data() :
    iterate = len(demographic.index) / batch;
    modulus = len(demographic.index) % batch;
    if (modulus > 0):
        iterate += 1

    i = 0
    for x in range(iterate):
        execution(i)
        print datetime.today()
        time.sleep(interval)
        i += batch

try:
    opts, args = getopt.getopt(sys.argv[1:], "i:o:", ["interval=", "batch="])
except getopt.GetoptError:
    print 'simulate-event --interval=5 --batch-size=10'
    sys.exit(2)
for opt, arg in opts:
    if opt in ("-i", "--interval") or opt in ("-b", "--batch"):
        if opt in ("-i", "--interval"):
            interval = int(arg)
        elif opt in ("-b", "--batch"):
            batch = int(arg)
        else:
            print "out of scope"
    else:
        print 'simulate-event --interval=5 --batch-size=10'
        sys.exit(2)

if (interval < 1 or batch < 1):
    print "interval or batch args must defined,simulate-event --interval=5 --batch-size=10"
    sys.exit(1)

create_parent_path()
execution_data()

















# raw_data_call = append_raw_data_call()
# print(raw_data_call)
# print(raw_data_call.loc[raw_data_call['user_id'] == 1])













# for x in range(len(demographic.index)) :
#     time.sleep(interval)
# print(demographic['user_id'][0])

# print(len(demographic.index))

# dfs = pd.read_excel("Dataset.xlsx", sheet_name="raw_data_call_1")
#
# sheet6 = pd.read_excel("Dataset.xlsx", sheet_name="Sheet6")
#
#
#
# print sheet6.to_dict()["key"][0]
#
# print(dfs.as_matrix())
# print(dfs.head())
#
# a = {
#     'aaa' : 7
# }
# a['bbb'] = 8
# print(a)