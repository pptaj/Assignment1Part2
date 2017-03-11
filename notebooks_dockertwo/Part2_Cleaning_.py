
# coding: utf-8

# In[2]:

def split_date(fileData):
    splitter = fileData['date'].apply(lambda x: x.split('-'))
    fileData['year'] = splitter.apply(lambda x: x[0])
    fileData['month'] = splitter.apply(lambda x: x[1])
    fileData['dayOfMonth'] = splitter.apply(lambda x: x[2])
    return fileData
    


# In[3]:

def clean_size(fileData):
    if (any(fileData['code'] == 304)):
        fileData['size'].fillna(0, inplace=True)
    return fileData


# In[4]:

def split_time(fileData):
    splitter = fileData['time'].apply(lambda x: x.split(':'))
    fileData['h'] = splitter.apply(lambda x: x[0])
    fileData['m'] = splitter.apply(lambda x: x[1])
    fileData['s'] = splitter.apply(lambda x: x[2])
    return fileData


# In[5]:

def create_directory(dir_name):
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)


# In[6]:

def remove_code(fileData):
    fileData = fileData[(fileData['code'] != 0)]
    return fileData


# In[7]:

def processed_csvs(fileData, filePath,i):
    fp = re.split('/', filePath, flags=re.IGNORECASE)
    new_file_path = fp[0] + "/" + fp[1]  + "/"
    create_directory(new_file_path + "Cleaned_Files")
    new_file_path += "Cleaned_Files/Cleaned_" + fp[2] 
    fileData.to_csv(new_file_path, sep=',')
#     if(i==1):
#         fileData.to_csv(new_file_path, sep=',')
#     else:
#         with open(new_file_path, 'a') as f:
#             fileData.to_csv(f, header=False)
#     i=i+1


# In[8]:

def true_extention(fileData):
    splitter = fileData['extention'].apply(lambda x: x.split('.'))
    try:
        fileData['file_name'] = splitter.apply(lambda x: x[0])
        fileData['true_extention'] = splitter.apply(lambda x: x[1])
#         no true_extention created
    except:
        pass
    return fileData


# In[9]:

def handling_leftovers(fileData):
    fileData['file_name'].replace(r'$^', np.nan, regex=True, inplace = True)
    fileData.fillna(999999, inplace=True)
#     print("_______________________________")
#     print(fileData['file_name'])
    
    return fileData


# In[10]:

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


# In[ ]:




# In[11]:

import os, sys
import re
import pandas as pd
import numpy as np
import logging, sys, time

logger = get_logger()
logger.info("Starting Cleaning of Data")
sys.stdout.write("\rProgress : %d%%" % 0)
time.sleep(1)
sys.stdout.write("\rProgress : %d%%" % 1)
time.sleep(1)


dir_path = "Files2"
ls_dir = os.listdir(dir_path)
year = 0;
sys.stdout.write("\rProgress : %d%%" % 3)
# Finding Directory or year for which csv are present

for file in ls_dir:
    regexp = re.compile(r'.txt|.log')
#     print(file)
    if not(regexp.search(file)):
        year = file

sys.stdout.write("\rProgress : %d%%" % 5)
# Setting Directory for the year    
if not(year == 0):
#     print(year)
    dir_path += "/" + str(year)
else:
    print("No Files found! Ending Program")
    sys.exit()
sys.stdout.write("\rProgress : %d%%" % 7)    
#Looping over all the csv to load and process the data
ls_dir = os.listdir(dir_path)

i=1;
sys.stdout.write("\rProgress : %d%%" % 10)
x = 10
# print(ls_dir)
for file in ls_dir:
#     only if file is csv
    logger.info("Cleaning " + file)
    regexp = re.compile(r'.csv')
    if(regexp.search(file)):
        filePath = dir_path + "/" + file
    #   Reading File data with pandas
        fileData = pd.read_csv(filePath,header = 0)

        
#       adding columns for year, month and day of month
        fileData = split_date(fileData)
#       adding columns for hour, minutes and seconds
        fileData = split_time(fileData)
#       adding column for true extension
        fileData = true_extention(fileData)
#       replace empty "size" with 0 if code equals 304
        fileData = clean_size(fileData) 
#       removinf code with value 0
        fileData = remove_code(fileData)
#     replacing all other NaNs with 99999 
        fileData = handling_leftovers(fileData)
#         print(fileData.shape)
#      processed data  
        processed_csvs(fileData, filePath,i)
        i=i+1;
        x+=6
        sys.stdout.write("\rProgress : %d%%" % x)
print("\nData Cleaned and Saved")
logger.removeHandler("handler")
logging.shutdown()
sys.stdout.write("\rProgress : %d%%" % 100)
sys.stdout.flush()

