import pandas as pd
import sys
import os
import json
from os.path import dirname, abspath, join
import mysql.connector
from mysql.connector import Error as SQL_ERR
from api_initiator import api_call
from config import meta_db_configs, sim_part_price_config, sim_file_upload_api_config, country_code_account_type, external_id_code_account_type
from AMXcsv_to_AQcsv import AMX_to_AQ
from logger import get_logger
from sftp_transfer import sftp_file_transfer
from os import listdir

dir_path = dirname(abspath("__file__"))

log_file_name = "sim_upload_log.txt"

for file in listdir(dir_path):
    if file.endswith(".csv"):
        delete_csv_file_path = os.path.join(dir_path, file)
        os.remove(delete_csv_file_path)


logger = get_logger(dir_path, log_file_name)

sim_file_upload_url_list = []
try:
    sftp_file_transfer()
except Exception as e:
    logger.error("csv file transfer error :{}".format(e))

amx_csv_file_name = None
amx_csv_file_path = None
for file in listdir(dir_path):
    if file.endswith(".csv") and file != "AQ.csv":
        amx_csv_file_name = file
        amx_csv_file_path = os.path.join(dir_path, file)

print(amx_csv_file_name)

print(amx_csv_file_path)

country_code = amx_csv_file_name[0 : amx_csv_file_name.index("_")]

print(country_code)

df_country_iso_code , df_external_ID = AMX_to_AQ(amx_csv_file_path)

print(df_external_ID)

sim_file_upload_url_list.append(sim_file_upload_api_config["sim_file_upload"])




jdbcMetaDatabase = meta_db_configs["DATABASE"]
host =  meta_db_configs["HOST"]
username = meta_db_configs["USER"]
password = meta_db_configs["PASSWORD"]
jdbcPort = meta_db_configs["PORT"]

q_country_iso_code_account_id_query = "select id from accounts where COUNTRIES_ID = (select ID from countries where ISO_CODE = '%s') and type = %s order by type asc ;"
q_external_id_account_id_query = "select  id  from accounts where EXTERNAL_ID = '%s' and type = %s;"




print(meta_db_configs)
def mysql_connection():
       try:
            mysql_conn = mysql.connector.connect(host=host, user=username,
             database=jdbcMetaDatabase, password=password, port=jdbcPort)
            logger.info("mysql connection established")

       except Exception as e:
           logger.error("mysql connection error", e)
           sys.exit("mysql connection error")
       return mysql_conn

db = mysql_connection()
cursor = db.cursor()

try:
    q_country_iso_code_account_id = q_country_iso_code_account_id_query%(country_code, country_code_account_type["type_country_code"])
    cursor.execute(q_country_iso_code_account_id)
    account_id_country_code = cursor.fetchall()
    logger.info("account_id mapped with country_code : {}".format(account_id_country_code) )
    account_id_country_code = account_id_country_code[0][0]
except Exception as e:
    print("error : {} fetching account_id mapped with country code {} for account type {} ".format(e,country_code, country_code_account_type["type_country_code"]))
    logger.error("error : {} fetching account_id mapped with country code {} for account type {} ".format(e, country_code,
                                                                                                   country_code_account_type[
                                                                                                       "type_country_code"]))

sim_file_upload_url_list.append(str(account_id_country_code))

print(sim_file_upload_url_list)

sim_file_upload_url = "".join(sim_file_upload_url_list)

print(sim_file_upload_url)


try:
    sim_upload = api_call()
    token = sim_upload.auth_user()
    print("token recieved")
except Exception as e:
    print("authorization error")
    logger.info("authorization error")



try :
    status_code = None
    content = None
    AQ_csv_path = os.path.join(dir_path,"AQ.csv")
    status_code, content = sim_upload.sim_csv_upload(AQ_csv_path, sim_file_upload_url,  token)
    print("csv file {} upload status {} ".format(amx_csv_file_name, status_code))
    print("csv file {} upload content {} ".format(amx_csv_file_name, content))
    logger.info("csv file {} upload status {} ".format(amx_csv_file_name, status_code))
    logger.info("csv file {} upload content {} ".format(amx_csv_file_name, content))
except Exception as e:
    logger.error("csv file {} upload status {} ".format(amx_csv_file_name, status_code))
    logger.error("csv file {} upload content {} ".format(amx_csv_file_name, content))
    logger.error("error : {}".format(e))
    print(e)
    db.close()
    cursor.close()
    sys.exit("Error in file upload")



AQ_csv_path = os.path.join(dir_path,"AQ.csv")

try :
    content = content["files"][0]["error"]
except:
    try:
       df = pd.read_csv(AQ_csv_path, header = None)
       df.columns = ["ICCID", "HOME-IMSI", "MSISDN", "ICCID_1", "DONOR-IMSI", "MSISDN_1", "EUICCID", "SIM-TYPE"]
    except Exception as e:
        print(e)

df["country(ISO-3166)"] = df_country_iso_code

df["external_id"] = df_external_ID



seq_csv_data_processing = False
if len(df) > 1:
   print("ok")
   df_first = df.head(1)

   df_last = df.tail(1)

   seq_csv_data_processing = True



csv_data_sequential = False

q = "select iccid_from, iccid_to , batch_number, sim_count from sim_provisioned_ranges_level2 where iccid_from = (select id from sim_provisioned_range_details where imsi = '%s');"
q_= "select iccid_from, iccid_to , batch_number, sim_count from sim_provisioned_ranges_level2 where iccid_to = (select id from sim_provisioned_range_details where imsi = '%s');"



if seq_csv_data_processing:
    data_last = data_first = None

    try:
        q_last = q_ %(df_last['HOME-IMSI'][len(df)-1])

        cursor.execute(q_last)
        data_last = cursor.fetchall()
        data_last = data_last[0]
        print(data_last)

        q_first = q %(df_first['HOME-IMSI'][0])
        cursor.execute(q_first)
        data_first = cursor.fetchall()
        data_first = data_first[0]
        print(data_first)


        if data_last[0] == data_first[0] and data_last[1] == data_first[1] and data_last[2] == data_first[2] and data_last[3] == data_last[3] ==  len(df):
            csv_data_sequential = True
            print("imsi in csv file are in sequential order")
            logger.info("imsi in csv file are in sequential order")

            for index, row in df.iterrows():
                q_external_id_account_id_each = q_external_id_account_id_query%(row["external_id"], external_id_code_account_type["type_external_id"])

                data_external_id_account_id_each = None
                data_sim_part_price = data_sim_inventory = {}
                try:

                    try:
                        cursor.execute(q_external_id_account_id_each)
                        data_external_id_account_id_each = cursor.fetchall()
                        logger.info("Account_ID mapped with  enterprise id {} : {}".format(row["external_id"],
                                                                                           data_external_id_account_id_each))
                        data_external_id_account_id_each = data_external_id_account_id_each[0][0]
                        print(data_external_id_account_id_each)

                    except Exception as e:
                        logger.error("error : {} fetching Account_ID mapped with  enterprise id  : {} ".format(e, row["external_id"]))

                        print("error in fetching account_id from enterprise id")

                    iccid_from_each = int(data_first[0])

                    iccid_to_each = int(data_first[1])

                    batch_number_each = data_first[2]

                    sim_count_each = int(data_first[3])

                    simApplicableAccounts = f"{account_id_country_code},{ data_external_id_account_id_each}"

                    data_sim_part_price = {"batchNumber": batch_number_each, "iccidFrom": iccid_from_each, "iccidTo": iccid_to_each,
                                           "simCount": sim_count_each , "skuName": sim_part_price_config["skuName"], "skuDescription": sim_part_price_config["skuDescription"],
                                           "simVendor": sim_part_price_config["simVendor"], "serviceProvider": sim_part_price_config["serviceProvider"], "price": sim_part_price_config["price"],
                                           "priceDescription": sim_part_price_config["priceDescription"], "simApplicableAccounts": simApplicableAccounts}
                    print(data_sim_part_price)

                    data_sim_inventory = {"iccidFrom": iccid_from_each, "iccidTo": iccid_to_each, "simCount": str(sim_count_each),
                                          "accountId": str(account_id_country_code), "allocatedTo": str(data_external_id_account_id_each)}

                    print(data_sim_inventory)
                except Exception as e:
                    logger.error("error in sequential csv processing : {}".format(e))
                    print("error :", e)


                try:
                    status_code, content = sim_upload.sim_part_price(data_sim_part_price, token)
                except Exception as e:
                    print("error in sim part & price :{}, status_code: {} , content :{}".format(e, status_code, content))
                    logger.error("error in sim part & price :{}, status_code: {} , content :{}".format(e, status_code, content))

                try:
                    status_code, content = sim_upload.sim_inventory(data_sim_inventory, token)
                except Exception as e:
                    print("error in sim inventory :{}, status_code: {} , content :{}".format(e, status_code, content))
                    logger.error("error in sim inventory :{}, status_code: {} , content :{}".format(e, status_code, content))


    except Exception as e:
        print("error :{} in sequential order sim ".format(e))
    db.close()
    cursor.close()

if csv_data_sequential == False and not df.empty:
    print("imsi in csv file are not in sequential order")
    for index, row in df.iterrows():
        q_each = q%(row['HOME-IMSI'])
        q_external_id_account_id_each = q_external_id_account_id_query%(row["external_id"], external_id_code_account_type["type_external_id"])
        #q_external_id_account_id_each = q_external_id_account_id_query%(row["external_id"], external_id_code_account_type["type_external_id"])

        data_external_id_account_id_each = None
        iccid_from_each = iccid_to_each = batch_number_each = sim_count_each = simApplicableAccounts = None
        data_sim_part_price = data_sim_inventory = {}

        try:
            try:
                cursor.execute(q_each)
                data_each = cursor.fetchall()
                data_each = data_each[0]
                print(data_each)
            except Exception as e:
                logger.error("error in fetching non_seq sim data from database : {}".format(e))
                print("error in fetching non_seq sim")

            try:
                cursor.execute(q_external_id_account_id_each)
                data_external_id_account_id_each = cursor.fetchall()
                logger.info(
                    "Account_ID mapped with enterprise id {} : {}".format(row["external_id"],data_external_id_account_id_each))
                data_external_id_account_id_each = data_external_id_account_id_each[0][0]
                print(data_external_id_account_id_each)

            except Exception as e:
                logger.error("error : {} fetching Account_ID mapped with enterprise id  {} ".format(e, row["external_id"]))

                print("error in fetching account_id from enterprise ID")


            iccid_from_each = int(data_each[0])

            iccid_to_each = int(data_each[1])

            batch_number_each = data_each[2]

            sim_count_each = int(data_each[3])

            simApplicableAccounts = f"{account_id_country_code},{data_external_id_account_id_each}"



            data_sim_part_price = {"batchNumber": batch_number_each, "iccidFrom": iccid_from_each,
                                   "iccidTo": iccid_to_each,
                                   "simCount": sim_count_each,
                                   "skuName": sim_part_price_config["skuName"],
                                   "skuDescription": sim_part_price_config["skuDescription"],
                                   "simVendor": sim_part_price_config["simVendor"],
                                   "serviceProvider": sim_part_price_config["serviceProvider"],
                                   "price": sim_part_price_config["price"],
                                   "priceDescription": sim_part_price_config["priceDescription"],
                                   "simApplicableAccounts": simApplicableAccounts}

            data_sim_inventory =  {"iccidFrom":iccid_from_each,"iccidTo":iccid_to_each,"simCount": str(sim_count_each), \
                                   "accountId":str(account_id_country_code),"allocatedTo":str(data_external_id_account_id_each)}

            print(data_sim_part_price)

            print(data_sim_inventory)
        except Exception as e:
            print("error in fetching all non_seq sim")
            logger.error("error in sequential csv processing : {}".format(e))
            print("error :", e)


        try:
            status_code, content = sim_upload.sim_part_price(data_sim_part_price, token)
        except Exception as e:
            print("error in sim part & price :{}, status_code: {} , content :{}".format(e, status_code, content))
            logger.error("error in sim part & price :{}, status_code: {} , content :{}".format(e, status_code, content))

        try:
            status_code, content = sim_upload.sim_inventory(data_sim_inventory, token)
        except Exception as e:
            print("error in sim inventory :{}, status_code: {} , content :{}".format(e, status_code, content))
            logger.error("error in sim inventory :{}, status_code: {} , content :{}".format(e, status_code, content))

    db.close()
    cursor.close()


## removal of csv file  after execution of whole program


try:
  path_aq_csv_delete = os.path.join(dir_path,"AQ.csv")
  path_amx_csv_delete = os.path.join(dir_path,  amx_csv_file_name)
  os.remove(path_aq_csv_delete)
  os.remove(path_amx_csv_delete)

except Exception as e:
    print("error :", e)

print(path_aq_csv_delete)

#os.remove(path_aq_csv_delete)

db.close()
cursor.close()

