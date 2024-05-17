import mysql.connector

import numpy as np
import elabapy
import pandas as pd
import sqlite3
import json
import mysql.connector
from requests.exceptions import HTTPError
import csv
from datetime import datetime
import pandas as pd

server = "" #please put the servername
token = "" #please put the key
__LOG_FILE__ = "" #select a path on your system
__CALENDAR__ = "" #select a path on your system
__USERS__ = "" #select a path on your system

log = open(__LOG_FILE__, "w")   # 'r' for reading and 'w' for writing
now = datetime.now()

log.write("\nLOG FILE OF INTIALIZATION: " + now.strftime("%d/%m/%Y %H:%M:%S") + "\n")    # Write inside file

db = mysql.connector.connect(
    host="", # fill connection variables here
    # port="3806",
    user="", # fill connection variables here
    password="" # fill connection variables here,
    database="" # fill connection variables here)

db_cursor = db.cursor()

def db_close():
    return 1


def db_delete_table(tablename):
    return tablename

def db_sql_command():
    return 1

def elab_connect():
    return 1

def db_commit_close():
    log.close()
    db.commit()
    db_cursor.close()
    db.close()


def ETL_experiments():

  manager = elabapy.Manager(endpoint="",token= "", verify=False) #server name and token
  try:
        log.write("Connected to server \n")
        params = {
            "category": "Billable",
            "category_id": "2"
        }
        all_experiments = manager.get_all_experiments(params)
        count = 0
        db_cursor.execute("DROP TABLE IF EXISTS EXPERIMENTS")
        # Create table as per requirement
        log.write("CREATE TABLE EXPERIMENTS | ")
        sql = """create table EXPERIMENTS(
                       exp_title VARCHAR(200),
                       exp_user VARCHAR(40),
                       exp_start_date DATE,
                       exp_linked_Instrument VARCHAR(100),
                       exp_linked_Instrument_id INT,
                       exp_linked_usageid VARCHAR(100),
                       exp_linked_usageid_id INT,
                       exp_elab_id INT,
                       exp_JSON_date DATE,
                       exp_JSON_usage_hour  VARCHAR(6),
                       exp_JSON_support_hour VARCHAR(6),
                       exp_JSON_service_mode VARCHAR(60),
                       exp_JSON_operator VARCHAR(60),
                       exp_rechnung VARCHAR(60), 
                       exp_ab_rechnung VARCHAR(60))"""

        db_cursor.execute(sql)

        for all_exp in all_experiments:
            if (all_exp["category"] == "Billable"):

                exp_id = all_exp["id"]
                exp_each = manager.get_experiment(exp_id)
                exp_elab_id = exp_each["id"]
                exp_title = exp_each["title"]
                exp_user = exp_each["userid"]
                exp_start_date = datetime.strptime(exp_each["date"], '%Y-%m-%d')
                metadata = exp_each["metadata"]
                metadata_json = json.loads(metadata)
                linked_items = exp_each["links"]
                for i in linked_items:
                    if (i['name'] == "Instrument"):
                        exp_linked_Instrument_id = i['itemid']
                        exp_linked_Instrument = i['title']
                    elif (i['name'] == "Usage_ID"):
                        exp_linked_usageid = i['title']
                        exp_linked_usageid_id = i['itemid']



                exp_JSON_Measurement_date = datetime.strptime( metadata_json['extra_fields']['Measurement Date']['value'] ,'%Y-%m-%d')
                exp_JSON_Measurement_hour = metadata_json['extra_fields']['Measurement Hours']['value']
                if exp_JSON_Measurement_hour is None:
                    exp_JSON_Measurement_hour = 0
                exp_JSON_support_hours = metadata_json['extra_fields']['Support Hours']['value']
                if exp_JSON_support_hours is None:
                    exp_JSON_support_hours = 0
                exp_JSON_operation_mode = metadata_json['extra_fields']['Operation Mode']['value']
                exp_JSON_operator = metadata_json['extra_fields']['Operator']['value']

                print(exp_title, exp_user, exp_start_date, exp_linked_Instrument,exp_linked_Instrument_id,
                      exp_linked_usageid, exp_linked_usageid_id , exp_elab_id,
                      exp_JSON_Measurement_date, exp_JSON_Measurement_hour, exp_JSON_support_hours,
                      exp_JSON_operation_mode, exp_JSON_operator)

                sql = """INSERT INTO EXPERIMENTS (exp_title, exp_user, exp_start_date,exp_linked_Instrument, exp_linked_Instrument_id, exp_linked_usageid,exp_linked_usageid_id, exp_elab_id, exp_JSON_date, exp_JSON_usage_hour,exp_JSON_support_hour, exp_JSON_service_mode, exp_JSON_operator) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
                values = (exp_title, exp_user, exp_start_date, exp_linked_Instrument, exp_linked_Instrument_id, exp_linked_usageid, exp_linked_usageid_id, exp_elab_id, exp_JSON_Measurement_date,exp_JSON_Measurement_hour, exp_JSON_support_hours, exp_JSON_operation_mode, exp_JSON_operator)
                db_cursor.execute(sql, values)
                count = count+1

                params1 = {
                    "category": "5"
                }
                manager.post_experiment(exp_elab_id, params1)

                #params2 = {"tag": "Backoffice_API"}
                #manager.add_tag_to_experiment(exp_elab_id, params2)


        log.write("Number of rows inserted:  " + str(count) + "\n")


  except HTTPError as e:
        print(e)



def ETL_calendar_csv():
    df_events = pd.read_csv(__CALENDAR__ , header=0)
    db_cursor.execute("DROP TABLE IF EXISTS EVENTS\n")
    # Create table as per requirement
    log.write("CREATE TABLE EVENTS | ")
    sql = """create table EVENTS(
             event_title VARCHAR(200),
             event_elab_id INT ,
             event_start_date DATE,
             event_start_time TIME,
             event_end_date DATE,
             event_end_time TIME,
             event_reserved_hours  FLOAT,
             event_user_id INT ,
             event_Instrument VARCHAR(50) ,
             event_user_name VARCHAR(80))"""

    db_cursor.execute(sql)
    for row in range(len(df_events)):
        print(df_events["title"][row], df_events["id"][row], df_events["start"][row], df_events["end"][row], df_events["userid"][row], df_events["item_title"][row], df_events["fullname"][row] )
        start_date = df_events["start"][row][2:19]
        print(start_date)
        start_date1 = datetime.strptime(start_date, '%y-%m-%dT%H:%M:%S')
        end_date = df_events["end"][row][2:19]
        print(end_date)
        end_date1 = datetime.strptime(end_date, '%y-%m-%dT%H:%M:%S')

        diff_time = end_date1 - start_date1
        diff_time1 = diff_time.seconds / 3600
        print(diff_time)

        #print(type(df_events["title"][row]), type(df_events["id"][row]), type(df_events["start"][row]), type(df_events["end"][row]), type(df_events["userid"][row]), type(df_events["item_title"][row]), type(df_events["fullname"][row]))
        sql = """INSERT INTO EVENTS (event_title, event_elab_id, event_start_date, event_start_time , event_end_date, event_end_time, event_reserved_hours , event_user_id, event_Instrument, event_user_name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s); """
        values = (df_events["title"][row], int(df_events["id"][row]), start_date1, start_date1 , end_date1, end_date1 ,diff_time1, int(df_events["userid"][row]), df_events["item_title"][row], df_events["fullname"][row])
        db_cursor.execute(sql, values)
    log.write("Number of rows inserted:  " + str(row))

def ETL_Calendar_API():

    manager = elabapy.Manager(endpoint="",
                              token="",
                              verify=False) #server name and token

    db_cursor.execute("DROP TABLE IF EXISTS EVENTS_API\n")
    # Create table as per requirement
    log.write("CREATE TABLE EVENTS_API | ")
    sql = """create table EVENTS_API(
                 event_elab_id INT ,
                 event_Instrument_id INT,
                 event_start_date DATE,
                 event_start_time TIME,
                 event_end_date DATE,
                 event_end_time TIME,
                 event_reserved_hours FLOAT,
                 event_title VARCHAR(200),                
                 event_user_id INT )"""

    db_cursor.execute(sql)

    last_event_id =0;
    error_counter=0;
    correct_counter=0;
    while True:
        try:
            last_event_id = last_event_id + 1
            event = manager.get_event(last_event_id)
            print(event["id"] , event["team"] , event["item"] , event["title"], event["start"], event["end"], event["title"],
                  event["userid"], event["experiment"] , event["item_link"])
            start_date = event["start"][2:19]
            print(start_date)
            start_date1 = datetime.strptime(start_date, '%y-%m-%dT%H:%M:%S')
            end_date = event["end"][2:19]
            print(end_date)
            end_date1 = datetime.strptime(end_date, '%y-%m-%dT%H:%M:%S')

            diff_time = end_date1 - start_date1
            diff_time1 = diff_time.seconds / 3600
            print(diff_time)

            sql = """INSERT INTO EVENTS_API (event_elab_id, event_Instrument_id , event_start_date, event_start_time, event_end_date, event_end_time, event_reserved_hours, event_title, event_user_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s); """
            values = (event["id"], event["item"], start_date1, start_date1, end_date1, end_date1, diff_time1, event["title"], event["userid"])
            db_cursor.execute(sql, values)

            correct_counter = correct_counter + 1

        except HTTPError as e:
            error_counter = error_counter+1

            if(error_counter>25):
                break;

    log.write("Number of rows inserted:  " + str(correct_counter))
    #print("number of rows:"+ str(last_event_id))
    #print("number of errors:" + str(error_counter))
    #print("number of corrects:" + str(correct_counter))

def ETL_items():
    manager = elabapy.Manager(endpoint="",
                              token="",
                              verify=False) #server name and token

    db_cursor.execute("DROP TABLE IF EXISTS INSTRUMENTS")
    # Create table as per requirement
    log.write("CREATE TABLE INSTRUMENTS \n ")
    sql = """create table INSTRUMENTS(
                    inst_elab_id INT ,
                    inst_title VARCHAR(200),                
                    inst_user_id INT )"""

    db_cursor.execute(sql)
    db_cursor.execute("DROP TABLE IF EXISTS USAGEID")

    # Create table as per requirement
    log.write("CREATE TABLE USAGEID \n ")
    sql = """create table USAGEID(
                       usage_elab_id INT ,
                       usage_title VARCHAR(200),                
                       usage_user_id INT )"""

    db_cursor.execute(sql)

    try:
        all_items = manager.get_all_items()
        i = 0
        j= 0
        for each_item in all_items:
            if(each_item["category"] == "Instrument"):
                sql = """INSERT INTO INSTRUMENTS (inst_elab_id, inst_title , inst_user_id) VALUES (%s,%s,%s);"""
                values = (each_item["id"], each_item["title"], each_item["userid"])
                db_cursor.execute(sql, values)
                i = i+1

            elif (each_item["category"] == "Usage_ID"):
                sql = """INSERT INTO USAGEID (usage_elab_id, usage_title , usage_user_id) VALUES (%s,%s,%s);"""
                values = (each_item["id"], each_item["title"], each_item["userid"])
                db_cursor.execute(sql, values)
                j = j+1

    except HTTPError as e:
        print(e)


    log.write("Number of rows inserted for Instruments:  " + str(i) + "\n")
    log.write("Number of rows inserted for UsageId:  " + str(j) + "\n")


def ETL_users():
    df_users = pd.read_csv(__USERS__, header=0)
    db_cursor.execute("DROP TABLE IF EXISTS USERS\n")
    # Create table as per requirement
    log.write("CREATE TABLE USERS | ")
    sql = """create table USERS(
                 user_id INT,
                 user_name VARCHAR(90) ,
                 user_default_account VARCHAR(50),
                 user_email VARCHAR(90))"""

    db_cursor.execute(sql)
    for row in range(len(df_users)):
        print(df_users["user_id"][row], df_users["user_name"][row], df_users["default_account"][row], df_users["email"][row])
        sql = """INSERT INTO USERS (user_id, user_name, user_default_account, user_email ) VALUES (%s,%s,%s,%s); """
        values = (int(df_users["user_id"][row]), df_users["user_name"][row], df_users["default_account"][row], df_users["email"][row] )
        db_cursor.execute(sql, values)
    log.write("Number of rows inserted:  " + str(row))

ETL_experiments()
db_commit_close()