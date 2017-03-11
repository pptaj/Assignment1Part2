
# coding: utf-8

# In[5]:

#Generate links foreach quarter
def qtr1_url(year):
    for i in range(1,4):
       url.append("http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/" + str(year) + "/"+"Qtr1/"+"log"+str(year)+"0"+str(i)+"01.zip")
       
def qtr2_url(year):
    for i in range(4,7):
       url.append("http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/" + str(year) + "/"+"Qtr2/"+"log"+str(year)+"0"+str(i)+"01.zip")
def qtr3_url(year):
    for i in range(7,10):
       url.append("http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/" + str(year) + "/"+"Qtr3/"+"log"+str(year)+"0"+str(i)+"01.zip")
      
def qtr4_url(year):
    for i in range(10,13):
       url.append("http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/" + str(year) + "/"+"Qtr4/"+"log"+str(year)+str(i)+"01.zip")
 
     
        
 


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



import urllib.response
import urllib.request
from bs4 import BeautifulSoup
import pandas as pd
import os
import re
from urllib.request import urlopen
import logging
import requests, zipfile, io, sys, time
#http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/2003/Qtr1/log20030101.zip
     
logger = get_logger()
logger.info("Starting Program")
sys.stdout.write("\rProgress : %d%%" % 0)
time.sleep(1)
sys.stdout.write("\rProgress : %d%%" % 1)
time.sleep(1)

year_read = ""
year = 0

with open("config.txt") as configfile:
    for line in configfile:
        name, val = line.partition("=")[::2]
        if (name.strip()=="year"):
            year_read = val
year = year_read.strip()

try:
    year = int(year)
    if(year>=2003 and year<=2016):    
        sys.stdout.write("\rProgress : %d%%" % 10)
        time.sleep(1)

        

        sys.stdout.write("\rProgress : %d%%" % 10)
        time.sleep(1)
        #Data available only till: Qtr1,2016
        url = []

        if year == "2016":
            qtr1_url(year)
        #Any other year excep for 2016, all four quarters    
        else:
            logger.info("Generating Links for log files")
            sys.stdout.write("\rProgress : %d%%" % 15)
            qtr1_url(year)
            sys.stdout.write("\rProgress : %d%%" % 20)
            qtr2_url(year)
            sys.stdout.write("\rProgress : %d%%" % 25)
            qtr3_url(year)
            sys.stdout.write("\rProgress : %d%%" % 30)
            qtr4_url(year)

        #print(url[5])
        #Extract all files into folder
        i = 30
        logger.info("Downloading and Extracting Log files")
        print("\nIt may take some time")
        for link in url:
            r = requests.get(link)
            z = zipfile.ZipFile(io.BytesIO(r.content))
            z.extractall(path="Files2/"+str(year))
            i+=5
            sys.stdout.write("\rProgress : %d%%" % i)
        logger.info("Downloading Completed") 
        logger.removeHandler("handler")
        logging.shutdown()
        sys.stdout.write("\rProgress : %d%%" % 100)
        sys.stdout.flush()
    else:
        print("Not a valid year")
        logger.removeHandler("handler")
        logging.shutdown()
        sys.stdout.write("\rProgress : %d%%" % 100)
        sys.stdout.flush()
except:
    print("exception Not a valid year")
    logger.removeHandler("handler")
    logging.shutdown()
    sys.stdout.write("\rProgress : %d%%" % 100)
    sys.stdout.flush()

def split_date(fileData):
    splitter = fileData['date'].apply(lambda x: x.split('-'))
    fileData['year'] = splitter.apply(lambda x: x[0])
    fileData['month'] = splitter.apply(lambda x: x[1])
    fileData['dayOfMonth'] = splitter.apply(lambda x: x[2])
    return fileData
    

def clean_size(fileData):
    if (any(fileData['code'] == 304)):
        fileData['size'].fillna(0, inplace=True)
    return fileData

def split_time(fileData):
    splitter = fileData['time'].apply(lambda x: x.split(':'))
    fileData['h'] = splitter.apply(lambda x: x[0])
    fileData['m'] = splitter.apply(lambda x: x[1])
    fileData['s'] = splitter.apply(lambda x: x[2])
    return fileData

def create_directory(dir_name):
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

def remove_code(fileData):
    fileData = fileData[(fileData['code'] != 0)]
    return fileData

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


def true_extention(fileData):
    splitter = fileData['extention'].apply(lambda x: x.split('.'))
    try:
        fileData['file_name'] = splitter.apply(lambda x: x[0])
        fileData['true_extention'] = splitter.apply(lambda x: x[1])
#         no true_extention created
    except:
        pass
    return fileData

def handling_leftovers(fileData):
    fileData['file_name'].replace(r'$^', np.nan, regex=True, inplace = True)
    fileData.fillna(999999, inplace=True)
#     print("_______________________________")
#     print(fileData['file_name'])
    
    return fileData

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


# Saving dataframe to csv
def saveToCsv(dataframe, filePath, summaryParam):
    fp = re.split('/', filePath, flags=re.IGNORECASE)
    directory = fp[0] + "/" + fp[1]  + "/" + fp [2] + "/"
    directory += "Summary"
    create_directory(directory)
    new_file_path = directory + "/" + summaryParam + "_" + fp[3]
    dataframe.to_csv(new_file_path, sep=',')

def summary_count_unique_accn_per_month_func(fileData, filePath):
#     Grouping by month and accession to get a list of unique month and accen pair
    if not fileData.empty:
        d= pd.DataFrame({'count' : fileData.groupby( [ "month", "accession"] ).size()}).reset_index()
    # Grouping by month to get the count of unique accn numbers in the month ()
        d= pd.DataFrame({'unique_accessions_accessed' : d.groupby( [ "month"] ).size()}).reset_index()
        try:
            saveToCsv(d, filePath, "Count_Unique_Accession_Number")
    #         print("Unique accn Summary File Saved")
        except:
            print("Unique accn summary file couldn't be saved. Permission Denied Exception")
            pass

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

def count_of_accession_for_code(fileData, filePath):

    d = fileData.groupby(['month','code'])['accession'].count().reset_index(name="Count per Response")
#     print(type(d))
    try:
        saveToCsv(d, filePath, "Count_Accession_For_Error_Code")
#         print("Count_Error_Codes_For_Accession Summary File Saved")
    except:
        print("Count_Error_Codes_For_Accession summary file couldn't be saved. Permission Denied Exception")
        pass

def count_of_files_by_extention(fileData, filePath):

    d = fileData.groupby(['month','true_extention'])['true_extention'].count().reset_index(name="Count per Extension")
#     print(type(d))
    try:
        saveToCsv(d, filePath, "Count_of_files_by_extention")
#         print("Count_Error_Codes_For_Accession Summary File Saved")
    except:
        print("Count_of_files_by_extention summary file couldn't be saved. Permission Denied Exception")
        pass

def summary_count_all_accn_for_month_func(fileData, filePath):
    d= fileData.groupby('month')['accession'].count().reset_index(name="# of accessions")
    try:
        saveToCsv(d, filePath, "Count_All_Accession_Number")

    except:
        print("Unique accn summary file couldn't be saved. Permission Denied Exception")
        pass

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

def zip_folder(folder_path, output_path):
    """Zip the contents of an entire folder (with that folder included
    in the archive). Empty subfolders will be included in the archive
    as well.
    """
    parent_folder = os.path.dirname(folder_path)
    # Retrieve the paths of the folder contents.
    contents = os.walk(folder_path)

    zip_file = zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED)
    for root, folders, files in contents:
        # Include all subfolders, including empty ones.
        for folder_name in folders:
            absolute_path = os.path.join(root, folder_name)
            relative_path = absolute_path.replace(parent_folder + '\\', '')
            zip_file.write(absolute_path, relative_path)
        for file_name in files:
            absolute_path = os.path.join(root, file_name)
            relative_path = absolute_path.replace(parent_folder + '\\', '')
            zip_file.write(absolute_path, relative_path)
            

import os, sys
import re
import pandas as pd
import numpy as np
import logging, sys, time
import zipfile
import boto
import boto.s3
from boto.s3.key import Key

logger = get_logger()
logger.info("Compiling Data")
sys.stdout.write("\rProgress : %d%%" % 0)
time.sleep(1)
sys.stdout.write("\rProgress : %d%%" % 1)
time.sleep(1)
new_dir = "Problem2_Compiled"
create_directory(new_dir)

dir_path = "Files2"
ls_dir = os.listdir(dir_path)


year=0 

aws_access_read = ""
aws_secret_read = ""

with open("config.txt") as configfile:
    for line in configfile:
        name, val = line.partition("=")[::2]
        if (name.strip()=="aws_access_key"):
            aws_access_read = val
        elif (name.strip()=="aws_secret_key"):
            aws_secret_read = val
            
sys.stdout.write("\rProgress : %d%%" % 10)
time.sleep(1)

aws_access_key = aws_access_read.strip()
aws_secret_key = aws_secret_read.strip()


if not (aws_access_key == "" or aws_access_read == ""):
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

    # Compiling Cleaned Files
    dir_path += "/Cleaned_Files"
    sys.stdout.write("\rProgress : %d%%" % 7)
    ls_dir = os.listdir(dir_path)

    x = 7
    for file in ls_dir:
        x += 3
        regexp = re.compile(r'Cleaned')
        if(regexp.search(file)):
            filePath = dir_path + "/" + file
            fileData = pd.read_csv(filePath,header = 0)    
            compiled_file_path = new_dir + "/Cleaned.csv"
            if(os.path.exists(compiled_file_path)):
                with open(compiled_file_path , 'a') as f:
                    fileData.to_csv(f, header=False)
            else:
                fileData.to_csv(compiled_file_path, header=True)
        sys.stdout.write("\rProgress : %d%%" % x)





    # ____________for loop ends_____________________________
    # Compiling Cleaned Files Ends    



        #Going into the Summary Files Directory
    # Compiling Summary Files
    dir_path += "/Summary"
    sys.stdout.write("\rProgress : %d%%" % 24)
    ls_dir = os.listdir(dir_path)

    for file in ls_dir:
        x += 1
        regexp = re.compile(r'Count_Accession_For_Error_Code')
        if(regexp.search(file)):
            filePath = dir_path + "/" + file
            fileData = pd.read_csv(filePath,header = 0)    
            compiled_file_path = new_dir + "/Count_Accession_For_Error_Code.csv"
            if(os.path.exists(compiled_file_path)):
                with open(compiled_file_path , 'a') as f:
                    fileData.to_csv(f, header=False)
            else:
                fileData.to_csv(compiled_file_path, header=True)


        regexp = re.compile(r'Count_Unique_Accession_Number_Cleaned')
        if(regexp.search(file)):
            filePath = dir_path + "/" + file
            fileData = pd.read_csv(filePath,header = 0)    
            compiled_file_path = new_dir + "/Count_Unique_Accession_Number_Cleaned.csv"
            if(os.path.exists(compiled_file_path)):
                with open(compiled_file_path , 'a') as f:
                    fileData.to_csv(f, header=False)
            else:
                fileData.to_csv(compiled_file_path, header=True)



        regexp = re.compile(r'Count_All_Accession_Number')
        if(regexp.search(file)):
            filePath = dir_path + "/" + file
            fileData = pd.read_csv(filePath,header = 0)    
            compiled_file_path = new_dir + "/Count_All_Accession_Number.csv"
            if(os.path.exists(compiled_file_path)):
                with open(compiled_file_path , 'a') as f:
                    fileData.to_csv(f, header=False)
            else:
                fileData.to_csv(compiled_file_path, header=True)


        regexp = re.compile(r'Count_of_files_by_extention')
        if(regexp.search(file)):
            filePath = dir_path + "/" + file
            fileData = pd.read_csv(filePath,header = 0)    
            compiled_file_path = new_dir + "/Count_of_files_by_extention.csv"
            if(os.path.exists(compiled_file_path)):
                with open(compiled_file_path , 'a') as f:
                    fileData.to_csv(f, header=False)
            else:
                fileData.to_csv(compiled_file_path, header=True)
        sys.stdout.write("\rProgress : %d%%" % x)

    # ____________for loop ends_____________________________
    # Compiling Summary Files Ends


    zip_folder('Problem2_Compiled', 'Problem2_Compiled.zip')
    logger.info("Problem2_Compiled Files Zipped")


    logger.info("Uploading to Amazon s3")
     #Uploading Files to S3

    bucket_name = aws_access_key.lower() 
    conn = boto.connect_s3(aws_access_key,aws_secret_key)

    #Checking if bucket exists
    bucket = conn.lookup(bucket_name)
    if bucket is None:
        bucket = conn.create_bucket(bucket_name, location=boto.s3.connection.Location.DEFAULT)

    testfile = "Problem2_Compiled.zip"
    def percent_cb(complete, total):
        sys.stdout.write('.')
        sys.stdout.flush()

    k = Key(bucket)
    k.key = testfile
    k.set_contents_from_filename(testfile,
    cb=percent_cb, num_cb=10)
    logger.info("Files Uploaded to S3")

    logger.info("Compiled and uploaded to Amazon s3")
    logger.removeHandler("handler")
    logging.shutdown()
    sys.stdout.write("\rProgress : %d%%" % 100)
    sys.stdout.flush()

else:
    print("Please check your config file for aws keys")
    logger.removeHandler("handler")
    logging.shutdown()
    sys.stdout.write("\rProgress : %d%%" % 100)
    sys.stdout.flush()

