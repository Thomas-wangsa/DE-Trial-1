import pandas as pd
import time
from datetime import datetime
import json
import os
import sys, getopt

"""
# interval  = interval process in second
# batch     = quantity of process user id 
get value from external parameter
"""
###------------------------------------------------------------------------------------------------------------------###
interval    = 0
batch       = 0

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
###------------------------------------------------------------------------------------------------------------------###


"""
# append_raw_data_call()        = append raw data call history to 1 data
# read_demographic_data()       = read demographic data
# create_parent_path()          = create input file folder
# myconverter()                 = convert type data to string
# create_user_path()            = create user path file
"""
###------------------------------------------------------------------------------------------------------------------###
base_path = os.getcwd() + "/input_file/"
def append_raw_data_call() :
    print("WAITING FOR APPENDING THE RAW DATA CALL")
    raw_data_call_1 = pd.read_excel("Dataset.xlsx", sheet_name="raw_data_call_1")
    raw_data_call_2 = pd.read_excel("Dataset.xlsx", sheet_name="raw_data_call_2")
    raw_data_call_3 = pd.read_excel("Dataset.xlsx", sheet_name="raw_data_call_3")
    raw_data_call_4 = pd.read_excel("Dataset.xlsx", sheet_name="raw_data_call_4")
    print("APPENDING THE RAW DATA SUCESS")
    return pd.concat([raw_data_call_1, raw_data_call_2, raw_data_call_3,raw_data_call_4])


def read_demographic_data() :
    return pd.read_excel("Dataset.xlsx", sheet_name="demographic")

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
    else :
        print ("path already exist %s " % path)

def myconverter(o):
    if isinstance(o, datetime):
        return o.__str__()

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
    else :
        print ("path already exist %s " % path)

raw_data_call   = append_raw_data_call()
demographic     = read_demographic_data()
create_parent_path()
###------------------------------------------------------------------------------------------------------------------###



"""
# insert_file_demographic()         = input json file as demographic.json
# insert_file_call_log()            = input json file as call_log.json
# execution()                       = main function to parse data
# execution_data()                  = for count range based on count data diff batch size
"""
###------------------------------------------------------------------------------------------------------------------###
def insert_file_demographic(index) :
    data = {}
    # get data header/title
    for title in list(demographic):
        if (title == "flag_bad"):
            data[title] = 'bad' if demographic[title][index] == 1 else 'good'
            continue
        data[title] = demographic[title][index]
    # get data header/title
    # input json data to file
    demographic_json = {
        'type': "demographic",
        'data': json.dumps(data, default=myconverter)
    }
    with open(os.getcwd() + "/input_file/" + str(index + 1) + "/demographic.json", 'w') as outfile:
        json.dump(demographic_json, outfile)
    # input json data to file

def insert_file_call_log(index) :
    idx = raw_data_call.index[raw_data_call['user_id'] == index + 1]
    call_log_data = raw_data_call.loc[raw_data_call['user_id'] == index + 1]

    call_log_array = []

    if (len(call_log_data) > 0):
        for quantity in range(len(call_log_data)):
            # print quantity
            # print call_log_data['date'][idx[quantity]]
            if (pd.isnull(call_log_data['date'][idx[quantity]])):
                convert_date = None
            else:
                ts = int(call_log_data['date'][idx[quantity]])
                convert_date = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

            if (pd.isnull(call_log_data['duration'][idx[quantity]])):
                duration_result = None
            else:
                duration_result = int(call_log_data['duration'][idx[quantity]])

            if (pd.isnull(call_log_data['type_cat'][idx[quantity]])):
                category_result = None
            else:
                category_raw = int(call_log_data['type_cat'][idx[quantity]])
                category_result = "undefined"

                if (category_raw == 1):
                    category_result = "incoming"
                elif (category_raw == 2):
                    category_result = "missed call"
                elif (category_raw == 3):
                    category_result = "outgoing"
                elif (category_raw == 4):
                    category_result = "unknown"
                elif (category_raw == 5):
                    category_result = "voicemail"

            call_log_sub_array = {
                "date": convert_date,
                "duration": duration_result,
                "category": category_result
            }
            call_log_array.append(call_log_sub_array)

    call_log_json = {
        'type': 'call_log',
        'user_id': index + 1,
        'data': call_log_array
    }

    with open(os.getcwd() + "/input_file/" + str(index + 1) + "/call_log.json", 'w') as outfile:
        json.dump(call_log_json, outfile)

def execution(i):
    start_iterate = 0 + i;
    for y in range(batch):
        index = y + start_iterate
        if (index + 1 > len(demographic.index)): break
        create_user_path(index + 1)
        insert_file_demographic(index)
        insert_file_call_log(index)



def execution_data() :
    # for count range based on count data diff batch size
    iterate = len(demographic.index) / batch;
    modulus = len(demographic.index) % batch;
    if (modulus > 0):
        iterate += 1
    i = 0
    for x in range(iterate):
        start_time = datetime.today()
        print start_time
        execution(i)
        diff = interval - (datetime.today() - start_time).total_seconds()
        if(diff < 0) :
            diff = 0
        time.sleep(diff)

        i += batch

execution_data()