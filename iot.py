'''
This is a Python program to simulate an IoT data pipeline
from a field to database. The assumption made in this program is that
data is collected by a sensor as a function of time.

In order for this to happen the following pseudo code was followed:

1. Import Key libraries and methods
-> time --> For time access and conversions
-> datetime --> For manipulating date and time types
-> math --> Provides access to mathematical functons defined by the C std
-> pprint --> Provides pretty printing capabilities
-> os --> For accessing OS dependent functionality
-> signal --> Signal handlers for asynchronous events
-> sys --> System specific parameters and functions
-> environs --> For hiding sensitive data like passwords and keys

2. Initialize global variables with both sensitive and common data.
-> client = None
-> database name = some name
-> measurement = some name --> used by influxdb

3. Check whether the database exists. -- use get_list_database method as
stipulated in the InfluxDBClient class ran in Python.

4. Connect to the server using host and port parameters --> Tweak, retry for
x times. x = 5

5. Connect to the database using the host and port provided. If the database
does not exist, create one.

6. Create dummy sensor data and populate the database

7. Show data


'''



from influxdb import InfluxDBClient
import requests

import time
import datetime
import math
import pprint
import os
import signal
import sys


from environs import Env


# Initialise environment variables
env = Env()
env.read_env()

client = None
dbname = env('dbname')
measurement = 'sinwave'


def check_db():
    '''returns True if the database exists'''
    dbs = client.get_list_database()
    for db in dbs:
        if db['name'] == dbname:
            return True
    return False

def server_check(host, port, nretries=5):
    '''wait for the server to come online for waiting_time, nretries times.'''
    url = 'http://{}:{}'.format(host, port)
    waiting_time = 1
    for i in range(nretries):
        try:
            requests.get(url)
            return
        except requests.exceptions.ConnectionError:
            print('waiting for', url)
            time.sleep(waiting_time)
            waiting_time *= 2
            pass
    print('cannot connect to', url)
    sys.exit(1)

def db_connection(host, port, reset):
    '''connect to the database, and create it if it does not exist'''
    global client
    print('connecting to database: {}:{}'.format(host,port))
    client = InfluxDBClient(host, port, retries=5, timeout=1)
    server_check(host, port)
    create = False
    if not check_db():
        create = True
        print('creating database...')
        client.create_database(dbname)
    else:
        print('database already exists')
    client.switch_database(dbname)
    if not create and reset:
        client.delete_series(measurement=measurement)


def sensor_data(num_of_measurements):
    '''
    This is a simulated sensor function to create  sin-based data
    and insert these measurements to the db.
    num_of_measurements = 0 means : insert measurements until the end of time.
    '''
    i = 0
    if num_of_measurements==0:
        num_of_measurements = sys.maxsize
    for i in range(num_of_measurements):
        x = i/10.
        y = math.sin(x)
        data = [{
            'measurement':measurement,
            'time':datetime.datetime.now(),
            'tags': {
                'corn_field_sensor' : x
                },
                'fields' : {
                    'potatoe_field_sensor' : y
                    },
            }]
        client.write_points(data)
        pprint.pprint(data)
        time.sleep(1)

def get_entries():
    '''returns all entries in the database.'''
    results = client.query('select * from {}'.format(measurement))
    # we decide not to use the x tag
    return list(results[(measurement, None)])


if __name__ == '__main__':
    import sys

    from optparse import OptionParser
    parser = OptionParser('%prog [OPTIONS] <host> <port>')
    parser.add_option(
        '-r', '--reset', dest='reset',
        help='reset database',
        default=False,
        action='store_true'
        )
    parser.add_option(
        '-n', '--num_of_measurements',
        dest='num_of_measurements',
        type='int',
        help='reset database',
        default=0
        )

    options, args = parser.parse_args()
    if len(args)!=2:
        parser.print_usage()
        print('please specify two arguments')
        sys.exit(1)
    host, port = args
    db_connection(host, port, options.reset)
    def signal_handler(sig, frame):
        print()
        print('stopping')
        pprint.pprint(get_entries())
        sys.exit(0)
    signal.signal(signal.SIGINT, signal_handler)

    sensor_data(options.num_of_measurements)

    pprint.pprint(get_entries())
