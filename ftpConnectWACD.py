"""
Connect to WACD SFTP, get files and upload to GBQ
"""

# https://medium.com/@keagileageek/paramiko-how-to-ssh-and-file-transfers-with-python-75766179de73
# http://www.paramiko.org

# Protocol: SFTP - SSH File Transfer Protocol
Host = 'ftp.worldacdmarketdata.com'
User = 'LATAM'
Password = 'G5$sfUBWp6'

rootfolder='C:/Users/4328091/Desktop/WACD/'
write2folder=rootfolder+'csvs/'

import paramiko
import time
import csv
import os
import subprocess as sp

# SFTP Connection 
ssh_Client = paramiko.SSHClient()
ssh_Client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh_Client.connect(hostname=Host, username=User, password=Password)
ftp_client=ssh_Client.open_sftp()

# get list of all files in server
filesinserver=ftp_client.listdir()

for filename in filesinserver:
    # get file from server and write to WACD folder
    ftp_client.get(filename, rootfolder+filename)
    time.sleep(30)

ftp_client.close()

# TODO try to make the exe passwod input automatic (how to interact with the gui)
# place output inside this directory -> write2folder
# passphrase to decrypt file
psphrase = 'pgpLAh82kw7'

# run each exe
for filename in filesinserver:
    path2file=rootfolder+filename
    sp.run(path2file)

# full paths list for all csv outputs 
writtenfiles=os.listdir(write2folder)

# Get header info for one of the csv files
with open(writtenfiles[0], 'r') as infile:
    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames[0].split("\t")


# Wrap upload process in function
def LoadCSVtoGBQ(filenames, path, tableid, datasetid):
    """
    receives list of filenames to upload to bq dataset.project, all must be in same path
    """
    # https://cloud.google.com/bigquery/docs/loading-data-local
    from google.cloud import bigquery
    client = bigquery.Client('ed-cm-caranalytics-dev')
    dataset_id = datasetid # 'WACD'
    table_id = tableid # 'wacd_product'
    
    # https://cloud.google.com/bigquery/docs/datasets
    # Construct a full Dataset object to send to the API.
    dataset = client.dataset(dataset_id)
    dataset.location = "US" # TODO(developer): Specify the geographic location where the dataset should reside.
    dataset = client.create_dataset(dataset, exists_ok=True)  # Make an API request.

    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.create_disposition = 'CREATE_IF_NEEDED'
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.autodetect = True

    for filename in filenames:
        with open(path+filename, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

        job.result()  # Waits for table load to complete.
        print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))


productfiles=[x for x in writtenfiles if 'product' in x]
agentfiles=[x for x in writtenfiles if 'product' not in x]

LoadCSVtoGBQ(agentfiles, write2folder, "wacd_agent", "WACD")
LoadCSVtoGBQ(productfiles, write2folder, "wacd_product", "WACD")
