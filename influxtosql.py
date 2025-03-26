from dotenv import load_dotenv
import os
import logging
from influxdb_client import InfluxDBClient
import pymysql

os.environ['LANG'] = 'en_US.UTF-8'
os.environ['LC_ALL'] = 'en_US.UTF-8'


load_dotenv()  # Load variables from .env file

# Set up logging
logging.basicConfig(
    filename='sync_influx_to_mysql.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# InfluxDB details from environment variables
influx_token='xxxxxxxxxx'
influx_org='xxxxxxxxxx'
influx_bucket='xxxxxxxxxx'
influx_url='xxxxxxxxxx'

# MySQL details from environment variables
mysql_host='xxxxxxxxxx'
mysql_user='xxxxxxxxxx'
mysql_password='xxxxxxxxxx'
mysql_database='xxxxxxxxxx'
mysql_port= 3306

try:
    # Connect to InfluxDB
    client = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
    query_api = client.query_api()
    logging.info("Connected to InfluxDB")

    # Fetch data from InfluxDB for multiple measurements
    queries = {
        "machine_log": '''
            from(bucket: "trial")
            |> range(start: today())
            |> filter(fn: (r) => r["_measurement"] == "machine_log")
            |> filter(fn: (r) => r["login_id"] != "" and exists r["login_id"])
            |> filter(fn: (r) => r["machine"] != "" and exists r["machine"])
            |> filter(fn: (r) => r["_value"] != "" and exists r["_value"])
            |> group(columns: ["machine", "login_id"])
            |> last()
            |> group(columns: ["machine"])
            |> group()
            |> keep(columns: ["machine", "login_id", "staff_id","order_number", "_field", "_value"])
        ''',
        "mode_duration": '''
            from(bucket: "trial")
            |> range(start: today())
            |> filter(fn: (r) => r["_measurement"] == "machine_mode")
            |> filter(fn: (r) => r["_field"] == "mode_duration")
            |> filter(fn: (r) => r["machine"] != "" and exists r["machine"])
            |> group(columns: ["machine", "mode_session"])
            |> last()
            |> group(columns:["machine"])
            |> group()
            |> keep(columns: ["machine","mode_session","machine_mode", "order_number", "staff_id","_field", "_value"])
        ''',
        "mode_time": '''
            from(bucket: "trial")
            |> range(start: today())
            |> filter(fn: (r) => r["_measurement"] == "machine_mode")
            |> filter(fn: (r) => r["_field"] == "mode_time")
            |> filter(fn: (r) => r["machine"] != "" and exists r["machine"])
            |> group(columns: ["machine", "mode_session"])
            |> last()
            |> group(columns:["machine"])
            |> group()
            |> keep(columns: ["machine","mode_session","machine_mode", "order_number", "staff_id","_field", "_value"])
        ''',
        "machine_session": '''
            from(bucket: "trial")
            |> range(start: today())
            |> filter(fn: (r) => r["_measurement"] == "machine_session")
            |> filter(fn: (r) => r["session_id"] != "" and exists r["session_id"])
            |> filter(fn: (r) => r["machine"] != "" and exists r["machine"])
            |> filter(fn: (r) => r["machine_mode"] != "" and exists r["machine_mode"])
            |> filter(fn: (r) => r["_field"] == "session_status")
            |> group(columns: ["machine", "session_id"])
            |> last()
            |> group(columns: ["machine"])
            |> group()
            |> keep(columns: ["machine", "order_number", "staff_id", "machine_mode", "session_id", "_value"])
        ''',
        "machine_session_duration": '''
            from(bucket: "trial")
            |> range(start: today())
            |> filter(fn: (r) => r["_measurement"] == "machine_session")
            |> filter(fn: (r) => r["session_id"] != "" and exists r["session_id"])
            |> filter(fn: (r) => r["machine"] != "" and exists r["machine"])
            |> filter(fn: (r) => r["machine_mode"] != "" and exists r["machine_mode"])
            |> filter(fn: (r) => r["_field"] == "session_duration")
            |> group(columns: ["machine", "session_id"])
            |> last()
            |> group(columns: ["machine"])
            |> group()
            |> keep(columns: ["machine", "order_number", "staff_id", "_measurement", "machine_mode", "session_id", "_field", "_value"])
        ''',
        "machine_cycle": '''
            from(bucket: "trial")
            |> range(start: today())
            |> filter(fn: (r) => r["_measurement"] == "machine_session")
            |> filter(fn: (r) => r["session_id"] != "" and exists r["session_id"])
            |> filter(fn: (r) => r["machine"] != "" and exists r["machine"])
            |> filter(fn: (r) => r["_field"] == "cycle_count")
            |> group(columns:["machine","session_id"])
            |> last()
            |> group(columns: ["machine"])
            |> filter(fn: (r) => r["_value"] != 0 and r["_value"] > 3)
            |> group()
            |> keep(columns: ["machine", "machine_mode", "order_number", "session_id", "staff_id", "_measurement", "_value"])
        ''',
        "machine_temp": '''
            from(bucket: "trial")
            |> range(start: today())
            |> filter(fn: (r) => r["_measurement"] == "machine_session")
            |> filter(fn: (r) => r["session_id"] != "" and exists r["session_id"])
            |> filter(fn: (r) => r["machine"] != "" and exists r["machine"])
            |> filter(fn: (r) => r["_field"] == "temp")
            |> mean()
            |> group(columns: ["machine", "session_id"])
            |> keep(columns: ["machine", "machine_mode", "order_number", "session_id", "staff_id", "_measurement", "_value"])
        ''',
        "machine_length": '''
            from(bucket: "trial")
            |> range(start: today())
            |> filter(fn: (r) => r["_measurement"] == "machine_session")
            |> filter(fn: (r) => r["session_id"] != "" and exists r["session_id"])
            |> filter(fn: (r) => r["machine"] != "" and exists r["machine"])
            |> filter(fn: (r) => r["_field"] == "length")
            |> group(columns:["machine","session_id"])
            |> last()
            |> group(columns: ["machine"])
            |> filter(fn: (r) => r["_value"] != 0 and r["_value"] > 3)
            |> group()
            |> keep(columns: ["machine", "machine_mode", "order_number", "session_id", "staff_id", "_measurement", "_value"])
        ''',
        "machine_speed": '''
            from(bucket: "trial")
            |> range(start: today())
            |> filter(fn: (r) => r["_measurement"] == "machine_session")
            |> filter(fn: (r) => r["machine_mode"] != "" and exists r["machine_mode"])
            |> filter(fn: (r) => r["session_id"] != "" and exists r["session_id"])
            |> filter(fn: (r) => r["machine"] != "" and exists r["machine"])
            |> filter(fn: (r) => r["_field"] == "speed")
            |> filter(fn: (r) => r["_value"] != 0)
            |> group(columns: ["machine", "session_id"])
            |> unique(column: "_value")
            |> group(columns: ["machine"])
            |> group()
            |> keep(columns: ["machine", "_measurement", "machine_mode", "order_number", "session_id", "staff_id", "_field", "_value"])
        ''',
        "maintenance_log": '''
            from(bucket: "trial")
            |> range(start: today())
            |> filter(fn: (r) => r["_measurement"] == "maintenance")
            |> filter(fn: (r) => r["machine_mode"] == "maintenance")
            |> filter(fn: (r) => r["mode_session"] != "" and exists r["mode_session"])
            |> filter(fn: (r) => r["machine"] != "" and exists r["machine"])
            |> filter(fn: (r) => r["_field"] == "alert_time" or r["_field"] == "attend_time")
            |> filter(fn: (r) => r["_value"] != "" and exists r["_value"])
            |> group(columns: ["machine", "mode_session"])
            |> last()
            |> group(columns: ["machine"])
            |> group()
            |> keep(columns: ["machine", "machine_mode", "order_number", "staff_id", "mode_session", "_measurement", "_field", "_value"])
        ''',
        "maintenance_time_taken": '''
            from(bucket: "trial")
            |> range(start: today())
            |> filter(fn: (r) => r["_measurement"] == "maintenance")
            |> filter(fn: (r) => r["machine_mode"] == "maintenance")
            |> filter(fn: (r) => r["mode_session"] != "" and exists r["mode_session"])
            |> filter(fn: (r) => r["machine"] != "" and exists r["machine"])
            |> filter(fn: (r) => r["_field"] == "time_taken")
            |> group(columns: ["machine", "mode_session"])
            |> last()
            |> group(columns: ["machine"])
            |> group()
            |> keep(columns: ["machine", "order_number", "machine_mode", "mode_session", "staff_id", "maintenance_id", "_measurement", "_field", "_value"])
        ''',
        "maintenance_duration": '''
            from(bucket: "trial")
            |> range(start: today())
            |> filter(fn: (r) => r["_measurement"] == "maintenance")
            |> filter(fn: (r) => r["_field"] == "maintenance_duration")
            |> filter(fn: (r) => r["machine_mode"] == "maintenance")
            |> filter(fn: (r) => r["mode_session"] != "" and exists r["mode_session"])
            |> filter(fn: (r) => r["machine"] != "" and exists r["machine"])
            |> group(columns: ["machine", "mode_session"])
            |> last()
            |> group(columns: ["machine"])
            |> group()
            |> keep(columns: ["_value", "_field", "_measurement", "machine", "machine_mode", "order_number", "staff_id", "mode_session", "maintenance_id"])
        '''
    }
    results = {name: query_api.query(org=influx_org, query=qry) for name, qry in queries.items()}
    logging.info("Data fetched from InfluxDB")

except Exception as e:
    logging.error(f"Failed to fetch data from InfluxDB: {e}")
    raise

try:

    # Connect to MySQL
    db = pymysql.connect(
        host="192.168.1.3",
        user="iriv",
        password="smart_factory",
        database="mis",
        port=33306
    )
    cursor = db.cursor()  # Initialize cursor
    logging.info("Connected to MySQL")

    for measurement, result in results.items():
        for table in result:
            for record in table.records:
                try:
                    if measurement == "machine_log":
                        machine = record.values.get('machine')
                        staff_id = record.values.get('staff_id')
                        order_number = record.values.get('order_number')
                        login_id = record.values.get('login_id')
                        field = record.values.get('_field')
                        value = record.values.get('_value')
                        
                        cursor.execute(
                            "INSERT INTO machine_log (machine, staff_id, login_id, order_number, field, value, timestamp) "
                            "VALUES (%s, %s, %s, %s, %s, %s, FROM_UNIXTIME(%s/1000)) "
                            "ON DUPLICATE KEY UPDATE field = %s, value = %s",
                            (machine, staff_id, login_id, order_number, field, value, login_id, field, value)
                        )
                        logging.info(f"Inserted/Updated record in machine_log for login_id: {login_id}")

                    elif measurement == "mode_duration":
                        session_id = record.values.get('mode_session')
                        machine = record.values.get('machine')
                        staff_id = record.values.get('staff_id')
                        order_number = record.values.get('order_number')
                        mode = record.values.get('machine_mode')
                        duration = record.values.get('_value')
                        
                        cursor.execute(
                            "INSERT INTO machine_mode (session_id, machine, staff_id, order_number, mode, duration, timestamp) "
                            "VALUES (%s, %s, %s, %s, %s, %s, FROM_UNIXTIME(%s/1000)) "
                            "ON DUPLICATE KEY UPDATE mode = %s, duration = %s",
                            (session_id, machine, staff_id, order_number, mode, duration, session_id, mode, duration)
                        )
                        logging.info(f"Inserted/Updated record in machine_mode for session_id: {session_id}")

                    elif measurement == "machine_session":
                        session_id = record.values.get('session_id')
                        machine = record.values.get('machine')
                        staff_id = record.values.get('staff_id')
                        order_number = record.values.get('order_number')
                        machine_mode = record.values.get('machine_mode')
                        status = record.values.get('_value')
                        
                        cursor.execute(
                            "INSERT INTO machine_session (session_id, machine, staff_id, order_number, machine_mode, status, timestamp)"
                            "VALUES (%s, %s, %s, %s, %s, %s, FROM_UNIXTIME(%s/1000))"
                            "ON DUPLICATE KEY UPDATE status = %s",
                            (session_id, machine, staff_id, order_number, machine_mode, status, session_id, status)
                        )
                        logging.info(f"Inserted/Updated record in machine_session for session_id: {session_id}")

                    elif measurement == "machine_session_duration":
                        session_id = record.values.get('session_id')
                        machine = record.values.get('machine')
                        staff_id = record.values.get('staff_id')
                        order_number = record.values.get('order_number')
                        machine_mode = record.values.get('machine_mode')
                        field = record.values.get('_field')
                        duration = record.values.get('_value')
                        
                        cursor.execute(
                            "INSERT INTO machine_session_duration (session_id, machine, staff_id, order_number, machine_mode, field, duration, timestamp) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, FROM_UNIXTIME(%s/1000)) "
                            "ON DUPLICATE KEY UPDATE machine_mode = %s, field = %s, duration = %s",
                            (session_id, machine, staff_id, order_number, machine_mode, field, duration, session_id, machine_mode, field, duration) 
                        )
                        logging.info(f"Inserted/Updated record in machine_session_duration for session_id: {session_id}")

                    elif measurement == "machine_cycle":
                        session_id = record.values.get('session_id')
                        machine = record.values.get('machine')
                        staff_id = record.values.get('staff_id')
                        order_number = record.values.get('order_number')
                        machine_mode = record.values.get('machine_mode')
                        cycle_count = record.values.get('_value')
                        
                        cursor.execute(
                            "INSERT INTO machine_cycle (session_id, machine, staff_id, order_number, machine_mode, cycle_count, timestamp) "
                            "VALUES (%s, %s, %s, %s, %s, %s, FROM_UNIXTIME(%s/1000)) "
                            "ON DUPLICATE KEY UPDATE machine_mode = %s, cycle_count = %s",
                            (session_id, machine, staff_id, order_number, machine_mode, cycle_count, session_id, machine_mode, cycle_count)
                        )
                        logging.info(f"Inserted/Updated record in machine_cycle for session_id: {session_id}")

                    elif measurement == "machine_temp":
                        session_id = record.values.get('session_id')
                        machine = record.values.get('machine')
                        staff_id = record.values.get('staff_id')
                        order_number = record.values.get('order_number')
                        machine_mode = record.values.get('machine_mode')
                        temp = record.values.get('_value')
                        
                        cursor.execute(
                            "INSERT INTO machine_temp (session_id, machine, staff_id, order_number, machine_mode, temp, timestamp) "
                            "VALUES (%s, %s, %s, %s, %s, %s, FROM_UNIXTIME(%s/1000)) "
                            "ON DUPLICATE KEY UPDATE machine_mode = %s, temp = %s",
                            (session_id, machine, staff_id, order_number, machine_mode, temp, session_id, machine_mode, temp)
                        )
                        logging.info(f"Inserted/Updated record in machine_temp for session_id: {session_id}")

                    elif measurement == "machine_length":
                        session_id = record.values.get('session_id')
                        machine = record.values.get('machine')
                        staff_id = record.values.get('staff_id')
                        order_number = record.values.get('order_number')
                        machine_mode = record.values.get('machine_mode')
                        length = record.values.get('_value')
                        
                        cursor.execute(
                            "INSERT INTO machine_length (session_id, machine, staff_id, order_number, machine_mode, length, timestamp) "
                            "VALUES (%s, %s, %s, %s, %s, %s, FROM_UNIXTIME(%s/1000)) "
                            "ON DUPLICATE KEY UPDATE machine_mode = %s, length = %s",
                            (session_id, machine, staff_id, order_number, machine_mode, length, session_id, machine_mode, length)
                        )
                        logging.info(f"Inserted/Updated record in machine_length for session_id: {session_id}")

                    elif measurement == "machine_speed":
                        session_id = record.values.get('session_id')
                        machine = record.values.get('machine')
                        staff_id = record.values.get('staff_id')
                        order_number = record.values.get('order_number')
                        machine_mode = record.values.get('machine_mode')
                        speed = record.values.get('_value')
                        
                        cursor.execute(
                            "INSERT INTO machine_speed (session_id, machine, staff_id, order_number, machine_mode, speed, timestamp)"
                            "VALUES (%s, %s, %s, %s, %s ,%s, FROM_UNIXTIME(%s/1000))",
                            (session_id, machine, staff_id, order_number, machine_mode, speed, session_id)
                        )
                        logging.info(f"Inserted/Updated record in machine_speed for session_id: {session_id}")

                    elif measurement == "maintenance_log":
                        mode_session = record.values.get('mode_session')
                        machine = record.values.get('machine')
                        staff_id = record.values.get('staff_id')
                        machine_mode = record.values.get('machine_mode')
                        log = record.values.get('_field')
                        time = record.values.get('_value')
                        
                        cursor.execute(
                            "INSERT INTO maintenance_logs (mode_session, machine, staff_id, machine_mode, log, time, timestamp) "
                            "VALUES (%s, %s, %s, %s, %s,%s, FROM_UNIXTIME(%s/1000)) "
                            "ON DUPLICATE KEY UPDATE time = %s",
                            (mode_session, machine, staff_id, machine_mode, log, time, mode_session, time)
                        )
                        logging.info(f"Inserted/Updated record in maintenance_alert_log for mode_session: {mode_session}")

                    elif measurement == "maintenance_time_taken":
                        mode_session = record.values.get('mode_session')
                        machine = record.values.get('machine')
                        staff_id = record.values.get('staff_id')
                        machine_mode = record.values.get('machine_mode')
                        maintenance_id = record.values.get('maintenance_id')
                        time_taken = record.values.get('_value')
                        
                        cursor.execute(
                            "INSERT INTO maintenance_time_taken (mode_session, machine, staff_id, machine_mode, maintenance_id, time_taken, timestamp) "
                            "VALUES (%s, %s, %s, %s, %s, %s, FROM_UNIXTIME(%s/1000)) "
                            "ON DUPLICATE KEY UPDATE time_taken = %s",
                            (mode_session, machine, staff_id, machine_mode, maintenance_id, time_taken, mode_session, time_taken)
                        )
                        logging.info(f"Inserted/Updated record in maintenance_time_taken for maintenance_id: {maintenance_id}")

                    elif measurement == "maintenance_duration":
                        mode_session = record.values.get('mode_session')
                        machine = record.values.get('machine')
                        staff_id = record.values.get('staff_id')
                        machine_mode = record.values.get('machine_mode')
                        maintenance_id = record.values.get('maintenance_id')
                        duration = record.values.get('_value')
                        
                        cursor.execute(
                            "INSERT INTO maintenance_duration (mode_session, machine, staff_id, machine_mode, maintenance_id, duration, timestamp) "
                            "VALUES (%s, %s, %s, %s, %s, %s, FROM_UNIXTIME(%s/1000)) "
                            "ON DUPLICATE KEY UPDATE duration = %s",
                            (mode_session, machine, staff_id, machine_mode, maintenance_id, duration, mode_session, duration)
                        )
                        logging.info(f"Inserted/Updated record in maintenance_duration for maintenance_id: {maintenance_id}")

                except Exception as e:
                    logging.error(f"Failed to insert/update record in {measurement}: {e}")

    db.commit()
    logging.info("Data committed to MySQL")

except Exception as e:
    logging.error(f"MySQL operation failed: {e}")
    raise

finally:
    # Close connections
    try:
        cursor.close()
        db.close()
        logging.info("Closed MySQL connection")
    except Exception as e:
        logging.error(f"Failed to close MySQL connection: {e}")

    try:
        client.close()
        logging.info("Closed InfluxDB connection")
    except Exception as e:
        logging.error(f"Failed to close InfluxDB connection: {e}")

