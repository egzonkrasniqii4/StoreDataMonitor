import os
import ftplib
import time
import pyodbc
import threading
import schedule
import logging

# Local directory for temporary files and logs
local_temp_dir = "./temp"
os.makedirs(local_temp_dir, exist_ok=True)

# Logging setup
log_file = os.path.join(local_temp_dir, "logfile.log")
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger()

# Configuration for multiple stores
stores_config = [
    {"ftp_host": "0.0.0.0", "ftp_user": "adminftp", "ftp_password": "1234", "store_name": "TEST 1 "},
    {"ftp_host": "0.0.0.1", "ftp_user": "adminftp", "ftp_password": "1234", "store_name": "TEST 2 "},
    # Add more store configurations here
]


# SQL Server connection settings
sql_server = "EGZON"
database = "PeopleCounter"
username = "sa"
password = "sasa"
table_name = "[dbo].[PeopleCounterDivio]"


def insert_into_database(store, lan_ip, count_left, count_right, received_time):
    try:
        connection = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={sql_server};DATABASE={database};UID={username};PWD={password}"
        )
        cursor = connection.cursor()

        query = f"""
        INSERT INTO {table_name} ([Store], [lan_ip], [CountLeft], [CountRight], [received_time])
        VALUES (?, ?, ?, ?, ?)
        """
        cursor.execute(query, (store, lan_ip, count_left, count_right, received_time))
        connection.commit()
        logger.info(f"Inserted row: {store}, {lan_ip}, {count_left}, {count_right}, {received_time}")
    except Exception as e:
        logger.error(f"Error inserting into database: {e}")
    finally:
        connection.close()


def parse_last_line(line):
    try:
        counts = line.split(" ")[0]
        count_left, count_right = map(int, counts.split("/"))
        return count_left, count_right
    except Exception as e:
        logger.error(f"Error parsing line: {line} - {e}")
        return None, None


def monitor_ftp(store_config):
    prev_content = None
    prev_last_line = None
    local_file_path = os.path.join(local_temp_dir, f"{store_config['store_name']}_temp.txt")
    lan_ip = store_config["ftp_host"]

    while True:
        try:
            with ftplib.FTP() as ftp:
                ftp.connect(store_config["ftp_host"], 21)
                ftp.login(store_config["ftp_user"], store_config["ftp_password"])

                with open(local_file_path, "wb") as local_file:
                    ftp.retrbinary(f"RETR test.txt", local_file.write)

            with open(local_file_path, "r") as file:
                content = file.readlines()

            if content != prev_content:
                prev_content = content
                last_line = content[-1].strip() if content else ""

                if last_line == prev_last_line:
                    logger.info(f"No new data for {store_config['store_name']}. Skipping insert.")
                else:
                    prev_last_line = last_line
                    count_left, count_right = parse_last_line(last_line)
                    if count_left is not None and count_right is not None:
                        insert_into_database(store_config["store_name"], lan_ip, count_left, count_right, time.strftime("%Y-%m-%d %H:%M:%S"))
        except Exception as e:
            logger.error(f"Error for {store_config['store_name']}: {e}")

        time.sleep(1800)  # 30 minutes


def delete_temp_files():
    try:
        for file_name in os.listdir(local_temp_dir):
            file_path = os.path.join(local_temp_dir, file_name)
            if os.path.isfile(file_path):
                os.remove(file_path)
        logger.info("All temporary files deleted at midnight.")
    except Exception as e:
        logger.error(f"Error deleting temp files: {e}")


schedule.every().day.at("00:00").do(delete_temp_files)


def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(60)


# Start monitoring for each store
threads = []
for store_config in stores_config:
    thread = threading.Thread(target=monitor_ftp, args=(store_config,))
    thread.start()
    threads.append(thread)

# Start the scheduler in a separate thread
scheduler_thread = threading.Thread(target=run_scheduler)
scheduler_thread.daemon = True
scheduler_thread.start()

# Wait for all threads to complete
for thread in threads:
    thread.join()
