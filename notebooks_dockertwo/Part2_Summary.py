
# coding: utf-8

# In[23]:

# Saving dataframe to csv
def saveToCsv(dataframe, filePath, summaryParam):
    fp = re.split('/', filePath, flags=re.IGNORECASE)
    directory = fp[0] + "/" + fp[1]  + "/" + fp [2] + "/"
    directory += "Summary"
    create_directory(directory)
    new_file_path = directory + "/" + summaryParam + "_" + fp[3]
    dataframe.to_csv(new_file_path, sep=',')


# In[24]:

def summary_count_unique_accn_per_month_func(fileData, filePath):
#     Grouping by month and accession to get a list of unique month and accen pair
    d= pd.DataFrame({'count' : fileData.groupby( [ "month", "accession"] ).size()}).reset_index()
# Grouping by month to get the count of unique accn numbers in the month ()
    d= pd.DataFrame({'unique_accessions_accessed' : d.groupby( [ "month"] ).size()}).reset_index()
    try:
        saveToCsv(d, filePath, "Count_Unique_Accession_Number")
#         print("Unique accn Summary File Saved")
    except:
        print("Unique accn summary file couldn't be saved. Permission Denied Exception")
        pass


# In[25]:

def get_logger():
    create_directory("Files2")
    loglevel = logging.INFO            # DEBUG, CRITICAL, WARNING, ERROR
    logger = logging.getLogger("Application_Logs")
    logger2 = logging.getLogger("Application_Logs_Stream")
    if not getattr(logger, 'handler_set', None):
        logger.setLevel(logging.INFO)
#         Logfile handler
        handler = logging.FileHandler('Files2/logs.log')
        handler2 = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.addHandler(handler2)
        logger.setLevel(loglevel)
        logger.handler_set = True
#       Stream Handler
    if not getattr(logger, 'handler_set', None):
        logger2.setLevel(logging.INFO)
        handler2 = logging.StreamHandler()
        handler2.setFormatter(formatter)
        logger2.addHandler(handler2)
        logger2.setLevel(loglevel)
        logger2.handler_set = True
        
    return logger


def create_directory(dir_name):
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)


# In[26]:

def count_of_accession_for_code(fileData, filePath):

    d = fileData.groupby(['month','code'])['accession'].count().reset_index(name="Count per Response")
#     print(type(d))
    try:
        saveToCsv(d, filePath, "Count_Accession_For_Error_Code")
#         print("Count_Error_Codes_For_Accession Summary File Saved")
    except:
        print("Count_Error_Codes_For_Accession summary file couldn't be saved. Permission Denied Exception")
        pass


# In[27]:

def count_of_files_by_extention(fileData, filePath):

    d = fileData.groupby(['month','true_extention'])['true_extention'].count().reset_index(name="Count per Extension")
#     print(type(d))
    try:
        saveToCsv(d, filePath, "Count_of_files_by_extention")
#         print("Count_Error_Codes_For_Accession Summary File Saved")
    except:
        print("Count_of_files_by_extention summary file couldn't be saved. Permission Denied Exception")
        pass


# In[28]:

def summary_count_all_accn_for_month_func(fileData, filePath):
    d= fileData.groupby('month')['accession'].count().reset_index(name="# of accessions")
    try:
        saveToCsv(d, filePath, "Count_All_Accession_Number")

    except:
        print("Unique accn summary file couldn't be saved. Permission Denied Exception")
        pass


# In[29]:

import os, sys
import re
import pandas as pd
import numpy as np
import logging, sys, time


logger = get_logger()
logger.info("Summarizing Data")
sys.stdout.write("\rProgress : %d%%" % 0)
time.sleep(1)
sys.stdout.write("\rProgress : %d%%" % 1)
time.sleep(1)


dir_path = "Files2"
ls_dir = os.listdir(dir_path)
year = 0;

# Finding Directory or year for which csv are present (this will look for the last folder only)
for file in ls_dir:
    regexp = re.compile(r'.txt|.log')
#     print(file)
    if not(regexp.search(file)):
        year = file
sys.stdout.write("\rProgress : %d%%" % 4)    
# Setting Directory for the year    
if not(year == 0):
#     print(year)
    dir_path += "/" + str(year)
else:
    print("No Files found! Ending Program")
    sys.exit()
    
#Going into the Cleaned Files Directory



dir_path += "/Cleaned_Files/"
sys.stdout.write("\rProgress : %d%%" % 24)
ls_dir = os.listdir(dir_path)
# print(ls_dir)

# Looping over all cleaned csv files and summarizing the data
x = 24

for file in ls_dir:
    
    regexp = re.compile(r'.csv')
    x += 6
    
#     Looking only for csv files in the directory
    if(regexp.search(file)):
        logger.info("Summarizing for " + file)
        filePath = dir_path + file
        fileData = pd.read_csv(filePath,header = 0)
        fileData = fileData.ix[:, 2:24]
        sys.stdout.write("\rProgress : %d%%" % x)
#     ____________________________________________________
# CODE TO SUMMARIZE THE DATA

#     number of unique accn numbers - accesd per month
        summary_count_unique_accn_per_month_func(fileData, filePath)
        count_of_accession_for_code(fileData, filePath)
        summary_count_all_accn_for_month_func(fileData, filePath)
        count_of_files_by_extention(fileData, filePath)
        


# _______________________________________________________________

logger.info("Data Summarized and Saved")
logger.removeHandler("handler")
logging.shutdown()
sys.stdout.write("\rProgress : %d%%" % 100)
sys.stdout.flush()

