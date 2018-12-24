#!/usr/bin/python
import pandas as pd
import time
from datetime import datetime
import json
import os
import sys, getopt
from os import listdir
from os.path import isfile, join

import MySQLdb

db = MySQLdb.connect(host="localhost",    # your host, usually localhost
                     user="root",         # your username
                     passwd="668482",  # your password
                     db="test_kredivo")        # name of the data base

interval = 0


try:
    opts, args = getopt.getopt(sys.argv[1:], "i:", ["interval="])
except getopt.GetoptError:
    print 'interval args must defined,consumer-event --interval=5'
    sys.exit(2)
for opt, arg in opts:
    if opt in ("-i", "--interval") :
        if opt in ("-i", "--interval"):
            interval = int(arg)
        else:
            print "out of scope"
    else:
        print 'out of scope,consumer-event --interval=5'
        sys.exit(2)

if (interval < 1):
    print "interval args must defined,simulate-event --interval=5"
    sys.exit(1)


# you must create a Cursor object. It will let
#  you execute all the queries you need
cur = db.cursor()

# Use all the SQL you like
cur.execute("CREATE TABLE IF NOT EXISTS `input_data` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY,`user_id` int NOT NULL UNIQUE,`group_age` tinyint NOT NULL,`group_salary` tinyint NOT NULL,`outgoing_duration` float NOT NULL,`incoming_duration` float NOT NULL,`num_missed_call` int NOT NULL,`num_unknown_call` int NOT NULL,`inserted_at` timestamp NOT NULL)")

mypath = os.getcwd() + "/input_file/"
list_dir = os.listdir(mypath)

print datetime.today()
for index_dir in range (len(list_dir)) :

    user_dir = list_dir[index_dir]
    querystr = "SELECT * FROM input_data WHERE user_id = "+str(list_dir[index_dir])
    queryresult = cur.execute(querystr)

    if(queryresult > 0) :
        print user_dir+" already exist in table"
        continue
    else :
        file_object_demographic = open(mypath+str(list_dir[index_dir])+"/demographic.json", "r")
        demographic_json = json.loads(file_object_demographic.read())

        file_object_call_log = open(mypath + str(list_dir[index_dir]) + "/call_log.json", "r")
        call_log_json = json.loads(file_object_call_log.read())

        group_of_age        = None
        group_of_salary     = None
        outgoing_duration   = None
        incoming_duration   = None
        num_missed_call     = None
        num_unknown_call    = None
        for key,value_str in demographic_json.items():
            if(key == "data") :
                data_json = json.loads(value_str)
                for k,v in data_json.items() :

                    if(k == "d_age") :
                        group_of_age = 4

                        if(v >= 20 and v < 30) :
                            group_of_age = 1
                        elif(v >= 30 and v < 40) :
                            group_of_age = 2
                        elif(v >= 40 and v < 50) :
                            group_of_age = 3

                    if(k == "d_monthly_salary") :
                        group_of_salary = 0

                        if(v < 3000000) :
                            group_of_salary = 1
                        elif(v >= 3000000 and v < 10000000) :
                            group_of_salary = 2
                        elif(v >= 10000000 and v < 25000000) :
                            group_of_salary = 3
                        elif(v >= 25000000) :
                            group_of_salary = 4



        sql  = "INSERT INTO input_data "
        sql += "(user_id, group_age,group_salary,outgoing_duration,incoming_duration,num_missed_call,num_unknown_call) "
        sql += "VALUES ("+user_dir+","+str(group_of_age)+","+str(group_of_salary)+","+\
               str(outgoing_duration)+","+str(incoming_duration)+","+str(num_missed_call)+","+str(num_unknown_call)+")"
        cur.execute(sql)
        db.commit()

    #print queryresult
    #print list_dir[index_dir]
    #time.sleep(interval * 60)
    #print datetime.today()