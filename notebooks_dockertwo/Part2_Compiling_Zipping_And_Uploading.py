
# coding: utf-8

# In[2]:

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


# In[3]:

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
            


# In[6]:

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
        if (name=="aws_access_key"):
            aws_access_read = val
        elif (name=="aws_secret_key"):
            aws_secret_read = val
            
sys.stdout.write("\rProgress : %d%%" % 10)
time.sleep(1)

aws_access_key = aws_access_read.strip()
aws_secret_key = aws_secret_read.strip()



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

