# Multi-threading example in python by Waqar Dongre


# Problem:
# Make Folders named Processing, Queue and Processed. 
# Write a code that makes a file(txt) every second in the Processing folder,
# picks up all the files from processing and moves all the files to queue every 5 seconds. 
# It then picks files from the queue folder and updates a column in MySQL/mongoDB table as 0/1 and moves the file to the Processed folder
# Also, make sure that no files are moved from Processing to queue until the queue folder is empty.


import time
import shutil
import os
import mysql.connector  # It needs to download "mysql-connector-python" lib
import threading


def create_folder(directory_name, path):
    # Path
    path = os.path.join(path, directory_name)

    # Creating the directory
    os.mkdir(path)
    print("Directory '% s' created" % directory_name)


def creating_txt_file_sec(path_param):
    while True:
        if run == "1":
            path = path_param
            with open(path + 'sample_text_file_' + str(time.time()) + '.txt', 'w') as f:
                f.write('')
                print('File named sample_text_file_' + str(time.time()) + '.txt created in Processing folder.')
    
                # sleeping for 1 sec
            time.sleep(1)
        elif run == "0":
            break
        

def moving_to_queue(from_processing_param, moveto_queue_param):
    while True:
        if run == "1":
            files_in_queue = os.listdir(moveto_queue_param)
            if len(files_in_queue) > 0:
                print("Cant process, some files are already are in the Queue folder.")
                # Wait to queue get empty, sleeping for 0.5 sec
                time.sleep(0.5)
            else:
                # sleeping for 5 sec
                time.sleep(5)
    
                path = from_processing_param
                moveto = moveto_queue_param
                files = os.listdir(path)
                files.sort()
    
                for f in files:
                    src = path + f
                    dst = moveto + f
                    shutil.move(src, dst)
                    print(f + " moved in Queue folder.")
        elif run == "0":
            break                    


def processing_files_in_queue(from_path_queue_param, moveto_path_processed_param, mysql_conn_details_dict):
    while True:
        if run == "1":
            path = from_path_queue_param
            moveto = moveto_path_processed_param
            files = os.listdir(path)
            if len(files) > 0:
                for f in files:
    
                    updating_table(f, mysql_conn_details_dict)
    
                    src = path + f
                    dst = moveto + f
                    shutil.move(src, dst)
                    print(f + " Moved to Processed folder.")
        elif run == "0":
            break


def updating_table(file_name, mysql_conn_details_dict):
    # establishing the connection
    db = mysql.connector.connect(user=mysql_conn_details_dict['user'], password=mysql_conn_details_dict['password'], host=mysql_conn_details_dict['host'], port=mysql_conn_details_dict['port'],
                                   database=mysql_conn_details_dict['database'])

    # Creating a cursor object using the cursor() method
    cursor = db.cursor()

    # query to insert processed file name and update it as processed
    sql = "INSERT INTO "+mysql_conn_details_dict['table_name']+" ("+mysql_conn_details_dict['column_1_name']+", "+mysql_conn_details_dict['column_2_name']+") VALUES ('" + str(file_name) + "', 1);"

    # Creating a table
    cursor.execute(sql)

    # committing
    db.commit()

    print(file_name + " Updated in the database.")

    # Closing the connection
    db.close()


if __name__ == "__main__":
    main_path = "path_for_3_folders/"
    
    # creating 3 folders
    folders_list = [Processing, Queue, Processed]
    for fol in folders_list:
        create_folder(fol, main_path)
    
    mysql_conn_details_dict = dict(user='root', password='admin', host='localhost', port='3306', database='sample_db', table_name = 'files_processed', column_1_name = 'file_name', column_2_name = 'file_is_processed')
    
    # creating threads
    t1 = threading.Thread(target=creating_txt_file_sec, args=(main_path + "Processing/",))
    t2 = threading.Thread(target=moving_to_queue, args=(main_path + "Processing/", main_path + "Queue/",))
    t3 = threading.Thread(target=processing_files_in_queue, args=(main_path + "Queue/", main_path + "Processed/", mysql_conn_details_dict,))

    global run
    str_q = "Enter 1 to start or 0 to stop program: "
    while True:
        run = str(input(str_q))
        if run == "1":
            str_q = "Enter 0 to stop program: "
            if t1.isAlive() == False:
                t1.start()
                t2.start()
                t3.start()
                print("Processing started.")
        elif run == "0":
            t1.join()
            t2.join()
            t3.join()            
            print("Stopping processing success.")
            break

        
# SQL scripts

#CREATE SCHEMA `sample_db` ;

#CREATE TABLE `sample_db`.`files_processed` (
#  `file_name` VARCHAR(200) NOT NULL,
#  `file_is_processed` TINYINT NOT NULL DEFAULT 0,
#  PRIMARY KEY (`file_name`));