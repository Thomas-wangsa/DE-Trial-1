#!/usr/bin/python
import time
from datetime import datetime
import json
import os
import sys, getopt
import MySQLdb


# Change your credentials database here!
host    = "localhost"
user    = "root"
passwd  = "668482"
db_name = "test_kredivo"
# Change your credentials database here!


"""
# get_connection()              = get connection database
# create_table_input_data()     = generated input data table mysql
# get_current_path()            = get your worksheet path
# get_list_dir()                = read all list directory in specific path    
"""

###------------------------------------------------------------------------------------------------------------------###
def get_connection() :
    return MySQLdb.connect(host=host, user=user, passwd=passwd, db=db_name)

def create_table_input_data(cur) :
    cur.execute("CREATE TABLE IF NOT EXISTS `input_data` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY,`user_id` int NOT NULL UNIQUE,`group_age` tinyint NOT NULL,`group_salary` tinyint NOT NULL,`outgoing_duration` float NOT NULL,`incoming_duration` float NOT NULL,`num_missed_call` int NOT NULL,`num_unknown_call` int NOT NULL,`inserted_at` timestamp NOT NULL)")

def get_current_path() :
    return os.getcwd() + "/input_file/"

def get_list_dir(mypath) :
    list_dir = os.listdir(mypath)
    list_dir = [int(x) for x in list_dir]
    list_dir.sort()
    return list_dir
###------------------------------------------------------------------------------------------------------------------###




"""
# check_args()              = check args validation    
"""
###------------------------------------------------------------------------------------------------------------------###
def check_args() :
    try:
        opts, args = getopt.getopt(sys.argv[1:], "i:", ["interval="])
    except getopt.GetoptError:
        print 'interval args must defined,consumer-event --interval=5'
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-i", "--interval"):
            if opt in ("-i", "--interval"):
                return int(arg)
            else:
                print "out of scope"
                return 0
        else:
            print 'out of scope,consumer-event --interval=5'
            sys.exit(2)
interval = check_args()
if (interval < 1):
    print "interval args must defined,simulate-event --interval=5"
    sys.exit(1)
###------------------------------------------------------------------------------------------------------------------###



# get connection
db = get_connection()
# for execute query
cur = db.cursor()
# create table input data
create_table_input_data(cur)
# mypath located
mypath = get_current_path()
# all list dir
list_dir = get_list_dir(mypath)




"""
# check_user_exist()            = check user exist in database or not
# get_json_data()               = read json file in specific path
# get_group_of_age()            = grouping data based on age
# get_group_of_salary()         = grouping data based on salary   
"""

###------------------------------------------------------------------------------------------------------------------###
def check_user_exist(user_dir) :
    querystr        = "SELECT * FROM input_data WHERE user_id = " + str(user_dir)
    queryresult     =  cur.execute(querystr)
    if (queryresult > 0):
        print "user_id =" + str(user_dir) + " already exist in table"
        return True
    else:
        print "user_id = " + str(user_dir) + " on process "
        return False

def get_json_data(name_file_json,user_dir) :
    return json.loads(open(mypath + str(user_dir) + "/"+name_file_json, "r").read())

def get_group_of_age(v) :
    group_of_age = 4
    if (v >= 20 and v < 30):
        group_of_age = 1
    elif (v >= 30 and v < 40):
        group_of_age = 2
    elif (v >= 40 and v < 50):
        group_of_age = 3
    return group_of_age


def get_group_of_salary(v) :
    group_of_salary = 0
    if (v < 3000000):
        group_of_salary = 1
    elif (v >= 3000000 and v < 10000000):
        group_of_salary = 2
    elif (v >= 10000000 and v < 25000000):
        group_of_salary = 3
    elif (v >= 25000000):
        group_of_salary = 4
    return group_of_salary
###------------------------------------------------------------------------------------------------------------------###




"""
# execution()                   = main function to ETL Process
# extract_data()                = check the data in database  
"""

###------------------------------------------------------------------------------------------------------------------###

def execution(user_dir) :
    demographic_json    = get_json_data("demographic.json", user_dir)
    call_log_json       = get_json_data("call_log.json", user_dir)

    group_of_age        = None
    group_of_salary     = None
    outgoing_duration   = None
    incoming_duration   = None
    num_missed_call     = None
    num_unknown_call    = None

    for key, value_str in demographic_json.items():
        if (key == "data"):
            data_json = json.loads(value_str)
            for k, v in data_json.items():
                if (k == "d_age"):
                    group_of_age = get_group_of_age(v)
                if (k == "d_monthly_salary"):
                    group_of_salary = get_group_of_salary(v)

    for key, json_array in call_log_json.items():
        if (key == "data"):
            outgoing_duration = 0
            incoming_duration = 0
            num_missed_call = 0
            num_unknown_call = 0

            for item in json_array:
                # print item
                if (item['category'] == "outgoing"):
                    outgoing_duration += item['duration']
                if (item['category'] == "incoming"):
                    incoming_duration += item['duration']
                if (item['category'] == "missed call"):
                    num_missed_call += 1
                if (item['category'] == "unknown"):
                    num_unknown_call += 1

    sql = "INSERT INTO input_data "
    sql += "(user_id, group_age,group_salary,outgoing_duration,incoming_duration,num_missed_call,num_unknown_call) "
    sql += "VALUES (" + str(user_dir) + "," + str(group_of_age) + "," + str(group_of_salary) + "," + \
           str(outgoing_duration) + "," + str(incoming_duration) + "," + str(num_missed_call) + "," + str(
        num_unknown_call) + ")"
    cur.execute(sql)
    db.commit()




def extract_data() :
    print datetime.today()
    for index_dir in range(len(list_dir)):
        user_dir = list_dir[index_dir]
        start_time = datetime.today()
        print start_time
        if(check_user_exist(user_dir)) :
            continue
        else :
            execution(user_dir)
        #time.sleep(interval)
        diff = interval - (datetime.today() - start_time).total_seconds()
        if (diff < 0):
            diff = 0
        time.sleep(diff)


extract_data()
