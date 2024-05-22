import mysql.connector

import numpy as np
import elabapy
import elabapi_python
import pandas as pd
import sqlite3
import json
import mysql.connector
from requests.exceptions import HTTPError
import csv
from datetime import datetime
import pandas as pd

server = ""
#token = ""
token = ""
__LOG_FILE__ = ""
__CALENDAR__ = ""
__USERS__ = ""

log = open(__LOG_FILE__, "w+")   # 'r' for reading and 'w' for writing
now = datetime.now()

log.write("\nLOG FILE OF INTIALIZATION: " + now.strftime("%d/%m/%Y %H:%M:%S") + "\n")    # Write inside file

db = mysql.connector.connect(
    host="",
    # port="",
    user="",
    password="",
    database="")

db_cursor = db.cursor()

configuration = elabapi_python.Configuration()
configuration.api_key['api_key'] = token
configuration.api_key_prefix['api_key'] = 'Authorization'
configuration.host = server
configuration.debug = False
configuration.verify_ssl = False

api_client = elabapi_python.ApiClient(configuration)
# fix issue with Authorization header not being properly set by the generated lib
api_client.set_default_header(header_name='Authorization', header_value=token)
# END CONFIG


def db_commit_close():
    log.close()
    db.commit()
    db_cursor.close()
    db.close()


def ETL_experiments_API2():

    experimentsApi = elabapi_python.ExperimentsApi(api_client)
    print(type(experimentsApi))
    all_experiments = experimentsApi.read_experiments()

    try:
        log.write("Connected to \n")
        params = {
            "limit": "99"
        }
  #      all_experiments = manager.get_all_experiments(params)
        count = 0
        db_cursor.execute("DROP TABLE IF EXISTS EXPERIMENTS_v2")
        # Create table as per requirement
        log.write("CREATE TABLE EXPERIMENTS_v2 | ")
        sql = """create table EXPERIMENTS_v2(
                       exp_title VARCHAR(200),
                       exp_user_id INT,
                       exp_start_date DATE,
                       exp_elab_id INT,
                       exp_ef_measurement_date DATE,
                       exp_ef_Instrument VARCHAR(60),
                       exp_ef_operation_mode VARCHAR(60),
                       exp_ef_operator VARCHAR(60),
                       exp_ef_Laboratory_Days  VARCHAR(6),
                       exp_ef_measurement_hours  VARCHAR(6),
                       exp_ef_service_hours VARCHAR(6),
                       exp_ef_UsageId VARCHAR(60),
                       exp_ef_path  VARCHAR(200),
                       exp_ef_comments  VARCHAR(200),
                       exp_rechnung INT NOT NULL AUTO_INCREMENT, 
                       exp_ab_rechnung VARCHAR(60),
                       PRIMARY KEY ( exp_rechnung ))"""

        db_cursor.execute(sql)
        for exp_each in all_experiments:
            if (exp_each.status_title == 'Finished'): #means finished
                exp_title = exp_each.title
                exp_user = exp_each.userid
                exp_start_date = exp_each.created_at
                exp_elab_id = exp_each.elabid
                metadata = exp_each.metadata
                print(exp_title)
                if(metadata is not None and exp_start_date[0:4]=='2024'):
                    metadata_json = json.loads(metadata)
                    json_list = metadata_json['extra_fields']
                    #for j in json_list:
                        #print(j)
                    exp_ef_Measurement_date = datetime.strptime(metadata_json['extra_fields']['Measurement Date']['value'], '%Y-%m-%d')
                    exp_ef_measurement_hours = metadata_json['extra_fields']['Measurement Hours']['value']
                    if exp_ef_measurement_hours is None:
                        exp_ef_measurement_hours = 0
                    exp_ef_service_hours = metadata_json['extra_fields']['Service Hours']['value']
                    if exp_ef_service_hours is None:
                         exp_ef_service_hours = 0
                    exp_ef_operation_mode = metadata_json['extra_fields']['Operation Mode']['value']
                    exp_ef_operator = metadata_json['extra_fields']['Operator']['value']
                    exp_ef_UsageId = metadata_json['extra_fields']['UsageID']['value']
                    exp_ef_Instrument = metadata_json['extra_fields']['Instrument']['value']
                    exp_ef_Laboratory_Days = metadata_json['extra_fields']['Laboratory Days']['value']
                    exp_ef_path= metadata_json['extra_fields']['Path to raw data']['value']
                    exp_ef_comments = metadata_json['extra_fields']['Comments']['value']

                print(exp_title, exp_user, exp_start_date, exp_elab_id,exp_ef_Measurement_date , exp_ef_measurement_hours ,exp_ef_service_hours ,exp_ef_operation_mode,
                      exp_ef_operator,exp_ef_UsageId,exp_ef_Instrument,exp_ef_Laboratory_Days,exp_ef_path,exp_ef_comments)

                sql = """INSERT INTO EXPERIMENTS_v2 (exp_title, exp_user_id, exp_start_date,exp_elab_id,exp_ef_Measurement_date,exp_ef_Instrument,exp_ef_operation_mode,
                exp_ef_operator,exp_ef_Laboratory_Days,exp_ef_measurement_hours,exp_ef_service_hours,exp_ef_UsageId,exp_ef_path,exp_ef_comments,exp_ab_rechnung) 
                         VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""

                values = (exp_title, int(exp_user), exp_start_date,exp_elab_id,exp_ef_Measurement_date,exp_ef_Instrument,exp_ef_operation_mode,exp_ef_operator,
                          exp_ef_Laboratory_Days,exp_ef_measurement_hours,exp_ef_service_hours,exp_ef_UsageId,exp_ef_path,exp_ef_comments,'000000')
                db_cursor.execute(sql, values)
                count = count + 1

               # params1 = {
                #    "category": "5"
               # }
                #manager.post_experiment(exp_elab_id, params1)

                # params2 = {"tag": "#API_ref: %s" % str(count)}  # .zfill(9)
                # manager.add_tag_to_experiment(exp_elab_id, params2)

        log.write("Number of rows inserted:  " + str(count) + "\n")



    except HTTPError as e:
        print(e)



def ETL_Events_API2():
    EventsApi = elabapi_python.EventsApi(api_client)
    #print(type(EventsApi))
    all_events = EventsApi.read_events()
    #print(type(all_events))


    db_cursor.execute("DROP TABLE IF EXISTS EVENTS_API2\n")
    # Create table as per requirement
    log.write("CREATE TABLE EVENTS_API2 | ")
    sql = """create table EVENTS_API2(
                 id INT ,
                 instrument VARCHAR(200),
                 start_date DATE,
                 start_time TIME,
                 end_date DATE,
                 end_time TIME,
                 reserved_hours FLOAT,
                 event_title VARCHAR(200),     
                 user_name VARCHAR(100),           
                 user_id INT )"""

    db_cursor.execute(sql)
    correct_counter=0
    for each_event in all_events:
        print(each_event.id , each_event.item_title , each_event.start , each_event.end , each_event.fullname, each_event.title, each_event.userid)
        event_start = each_event.start
        start_date = event_start[2:19]
        print(start_date)
        start_date1 = datetime.strptime(start_date, '%y-%m-%dT%H:%M:%S')
        event_end = each_event.end
        end_date = event_end[2:19]
        print(end_date)
        end_date1 = datetime.strptime(end_date, '%y-%m-%dT%H:%M:%S')
        diff_time = end_date1 - start_date1
        diff_time1 = diff_time.seconds / 3600
        diff_time2 = diff_time.days * 24
        diff_time3 = diff_time1 + diff_time2
        print(diff_time3)

        sql = """INSERT INTO EVENTS_API2 (id, instrument , start_date, start_time, end_date, end_time, reserved_hours, event_title, user_name, user_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s); """
        values = (each_event.id, each_event.item_title, start_date1, start_date1, end_date1, end_date1, diff_time3, each_event.title, each_event.fullname,each_event.userid )
        db_cursor.execute(sql, values)
        correct_counter = correct_counter+1

    log.write("Number of rows inserted:  " + str(correct_counter))





def ETL_INSTRUMENT_API2():
    db_cursor.execute("DROP TABLE IF EXISTS INSTRUMENT2")
    # Create table as per requirement
    log.write("CREATE TABLE INSTRUMENT2\n ")
    sql = """create table INSTRUMENT2(
                       id INT ,
                       title VARCHAR(200),                
                       user_id INT )"""

    db_cursor.execute(sql)

    all_items = elabapi_python.ItemsApi(api_client)
    itemsList = all_items.read_items(cat=2)
    list_len = len(itemsList)
    for x in range(list_len):
        print(itemsList[x].title ,itemsList[x].id, itemsList[x].userid )
        sql = """INSERT INTO INSTRUMENT2 (id, title , user_id) VALUES (%s,%s,%s);"""
        values = (itemsList[x].id, itemsList[x].title, itemsList[x].userid)
        db_cursor.execute(sql, values)
        print('--------------------------------------------')


def ETL_USAGEID_BILLING_INFO_API2():
    db_cursor.execute("DROP TABLE IF EXISTS USAGE_ID_BILLING_INFO_V2")
    # Create table as per requirement
    log.write("CREATE TABLE USAGE_ID_BILLING_INFO_V2\n ")
    sql = """create table USAGE_ID_BILLING_INFO_V2(
                       usage_internal_id INT ,
                       usage_title VARCHAR(200),                
                       user_id INT ,
                       bi_internal_id INT ,
                       bi_title VARCHAR(200),
                       bi_group VARCHAR(30),
                       bi_institution VARCHAR(50),
                       bi_project_title VARCHAR(60),
                       bi_usage_category VARCHAR(30),
                       bi_Principal_Investigator VARCHAR(50),
                       bi_science_contact VARCHAR(50),
                       bi_Leitweg_ID  VARCHAR(30),
                       bi_VAT_number VARCHAR(30),
                       bi_Billing_Address1 VARCHAR(100),
                       bi_Billing_Address2 VARCHAR(100),
                       bi_admin_contact  VARCHAR(50) )"""

    db_cursor.execute(sql)

    all_items = elabapi_python.ItemsApi(api_client)
    itemsList = all_items.read_items(cat=6)
    list_len = len(itemsList)
    for x in range(list_len):
        query = ''
        print(itemsList[x].title ,itemsList[x].id, itemsList[x].userid,)
        query = "title:'"+itemsList[x].title + "_BillingInformation'"
        print(query)
        billinginfo = all_items.read_items(q=query)
        list_len2 = len(billinginfo)
        if (list_len2):
            for j in range(list_len2):
                #print(billinginfo[j].title , billinginfo[j].id , billinginfo[j].userid)
                bi_title = billinginfo[j].title
                bi_id = billinginfo[j].id
                metadata = billinginfo[j].metadata
                #print(metadata)
                if (metadata):
                    metadata_json = json.loads(metadata)
                    json_list = metadata_json['extra_fields']
                    bi_group = metadata_json['extra_fields']['Group']['value']
                    bi_institution = metadata_json['extra_fields']['Institution']['value']
                    #bi_project_title = metadata_json['extra_fields']['Project title ']['value'] if(metadata_json['extra_fields']['Project title ']['value']) else 'None'
                    bi_project_title ='None'
                    bi_usage_category = metadata_json['extra_fields']['Usage Category']['value']
                    bi_Principal_Investigator = metadata_json['extra_fields']['Principal Investigator']['value']
                    bi_science_contact = metadata_json['extra_fields']['Scientific contact (name, e-mail, phone)']['value']
                    bi_Leitweg_ID = metadata_json['extra_fields']['Leitweg ID']['value']
                    bi_VAT_number = metadata_json['extra_fields']['VAT number']['value']
                    bi_Billing_Address1 = metadata_json['extra_fields']['Billing Address (line 1)']['value']
                    bi_Billing_Address2 = metadata_json['extra_fields']['Billing Address (line 2)']['value']
                    bi_admin_contact = metadata_json['extra_fields']['Administrative contact (name, e-mail, phone)']['value']

        else:
            bi_title = 'None'
            bi_id = 0


        sql = """INSERT INTO USAGE_ID_BILLING_INFO_V2 (usage_internal_id, usage_title , user_id , 
                                                       bi_internal_id , bi_title , bi_group , bi_institution , bi_project_title , bi_usage_category, 
                                                       bi_Principal_Investigator, bi_science_contact , bi_Leitweg_ID , bi_VAT_number , 
                                                       bi_Billing_Address1 , bi_Billing_Address2, bi_admin_contact) 
                                                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
        values = (itemsList[x].id, itemsList[x].title, itemsList[x].userid,  bi_id , bi_title , bi_group ,bi_institution ,bi_project_title ,bi_usage_category,
                  bi_Principal_Investigator,bi_science_contact,bi_Leitweg_ID,bi_VAT_number,bi_Billing_Address1,bi_Billing_Address2,bi_admin_contact)
        db_cursor.execute(sql, values)
        print('--------------------------------------------')



def ETL_BILLINGINFO_API2():
    db_cursor.execute("DROP TABLE IF EXISTS BILLING_INFO2")
    # Create table as per requirement
    log.write("CREATE TABLE BILLING_INFO2\n ")
    sql = """create table BILLING_INFO2(
                       id INT ,
                       title VARCHAR(200),                
                       user_id INT )"""

    db_cursor.execute(sql)

    all_items = elabapi_python.ItemsApi(api_client)
    itemsList = all_items.read_items(cat=15)
    list_len = len(itemsList)
    for x in range(list_len):
        print(itemsList[x].title ,itemsList[x].id, itemsList[x].userid)
        sql = """INSERT INTO BILLING_INFO2 (id, title , user_id) VALUES (%s,%s,%s);"""
        values = (itemsList[x].id, itemsList[x].title, itemsList[x].userid)
        db_cursor.execute(sql, values)
        print('--------------------------------------------')


def ETL_items_test():
    all_items = elabapi_python.ItemsApi(api_client)
    itemsList = all_items.read_items(q="title:'005-04-B-KRIMI_BillingInformation'")
    list_len = len(itemsList)
    for x in range(list_len):
        print(itemsList[x].title, itemsList[x].id, itemsList[x].userid , itemsList[x].category_title , itemsList[x].category)
        print(itemsList[x])


#ETL_experiments_API2()
#ETL_USAGEID_API2()
#ETL_Events_API2()

#ETL_USAGEID_API2()
#ETL_INSTRUMENT_API2()
#ETL_BILLINGINFO_API2()
ETL_items_test()
ETL_USAGEID_BILLING_INFO_API2()
db_commit_close()
