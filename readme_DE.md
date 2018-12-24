#Documentation of Data Engineer Coding Test

**List Library External Python 2.7**
1. pandas
2. MySQLdb


**Result**
1. simulate-event.py
- just type python simulate-event.py --interval=x --batch=y
- then system will append all of raw_data_call and also read the demographic data
- system will process data as requested and the output : demographic.json and raw_data_call.json

2. consumer-event.py
- please change the credentials data in consumer-event.py.
- execute with python consumer-event.py --interval=x
- the system will create table input_data automatically
- the system will check in each user id whether exist in that table or not
- if new user, system will get the data as requested and store it to database



