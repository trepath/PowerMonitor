import configparser
import os
import time

import psycopg2
import plotly.graph_objects as go
from flask import Flask, render_template, jsonify, send_file, request, abort
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

# Read the configuration file
config = configparser.ConfigParser()
config.read('config.ini')

# PostgreSQL connection details
DB_HOST = config['PostgreSQL']['DB_HOST']
DB_NAME = config['PostgreSQL']['DB_NAME']
DB_USER = config['PostgreSQL']['DB_USER']
DB_PASS = config['PostgreSQL']['DB_PASS']

# File Locations
JAWS_LOGS = config['File Locations']['JAWS_LOGS']

# Define color codes and labels for status codes
status_colors = {
    0: 'rgb(200, 200, 200)',
    1: 'rgb(100, 200, 100)',
    500: 'rgb(100, 100, 100)',
    504: 'rgb(255, 128, 128)',
    403: 'rgb(255, 128, 0)',
    6: 'rgb(255, 0, 0)',
    2: 'rgb(255, 0, 0)',
    400: 'rgb(0, 0, 255)',
    200: 'rgb(0, 128, 128)'
    # Add more as needed
}
status_labels = {
    0: 'Credit',
    1: 'Success',
    500: '500 - Insurer Service Error',
    504: '504 - Timeout',
    403: '403 - Authentication Error',
    6: 'Critical Error',
    2: 'Critical Error WAWA',
    400: '400 - Broker Error',
    200: '200 - Success DOC Credit'
    # Add more as needed
}

# Define a cache to store the graph data
cache = {
    'graph_data': {
        'pq_clientid': None,
        'timestamp': None,
        'data': None,
    },
    'minute_breakdown': {
        'pq_clientid': None,
        'current_time': None,
        'timestamp': None,
        'data': None,
    },
}


# Create Flask web server
app = Flask(__name__)

# Initialize Flask Limiter
limiter = Limiter(
    app,
    default_limits=["200 per day", "50 per hour"]  # Limit each client to 200 requests per day and 50 requests per hour
)



@app.route('/')
def home():
    return render_template('index.html')


def reverse_lookup(dictionary, value):
    for key, val in dictionary.items():
        if val == value:
            return key
    return None


@app.route('/graph_data/<timestamp>')
@limiter.limit("10/minute")  # Limit this endpoint to 10 requests per minute
def minute_breakdown(timestamp):
    print("minute_breakdown " + timestamp)

    input_string = request.args.get('pq_clientid', None)  # Get the pq_clientid from the query string
    words = input_string.split("-")

    pq_clientid =  words[0]  # "none"
    if len(words) > 1:
        server = words[1]  # "none"
    else:
        server = "none"

    #pq_clientid = request.args.get('pq_clientid', None)  # Get the pq_clientid from the query string
    print(cache['minute_breakdown'])
    if cache['minute_breakdown']['pq_clientid'] == pq_clientid and cache['minute_breakdown'][
        'timestamp'] == timestamp and time.time() - cache['minute_breakdown']['current_time'] < 60:
        print("Using cached data")
        return cache['minute_breakdown']['data']

    # Connect to the PostgreSQL server
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

    # Create a cursor to execute queries
    cursor = conn.cursor()

    # Trim any leading or trailing spaces
    timestamp = timestamp.strip()

    # Convert the timestamp string to a datetime object
    timestamp_datetime = datetime.strptime(timestamp, "%Y-%m-%d %H:%M")

    # Calculate the start and end timestamps for the selected hour
    start_timestamp = timestamp_datetime
    end_timestamp = start_timestamp + timedelta(hours=1)

    # SQL query string
    sql_query = (
        "SELECT date_trunc('minute', lsr.stamp) as minute, lsd.status, COUNT(*) "
        "FROM log_service_requests AS lsr "
        "JOIN log_service_requests_details AS lsd ON lsr.srnumber = lsd.srnumber "
        "WHERE lsr.stamp >= %s and lsr.stamp < %s "
    )

    if pq_clientid != 'None':
        sql_query += "AND lsr.pq_clientid = %s "

    if server != 'none':
        if server == 'Testing':
            # Use servername starting with "UAT" for testing
            sql_query += "AND lsr.servername LIKE 'UAT%' "
        elif server == 'Production':
            # Use servername starting with "PROD" for production
            sql_query += "AND lsr.servername LIKE 'PROD%' "

    sql_query += "GROUP BY date_trunc('minute', lsr.stamp), lsd.status"

    if pq_clientid != 'None':
        cursor.execute(sql_query, (start_timestamp, end_timestamp, pq_clientid))
    else:
        cursor.execute(sql_query, (start_timestamp, end_timestamp))

    results = cursor.fetchall()

    # Extract minutes, statuses, and values from the results
    minutes, statuses, values = zip(*results)

    # Create a dictionary mapping minutes to dictionaries that map statuses to values
    data = {}
    for minute, status, value in results:
        if minute not in data:
            data[minute] = {}
        data[minute][status] = value

    # Close the cursor and the connection
    cursor.close()
    conn.close()

    # Create the per-minute breakdown graph using Plotly
    fig = go.Figure(data=[
        go.Bar(
            name=status_labels.get(status, str(status)),
            x=list(data.keys()),
            y=[data[minute].get(status, 0) for minute in data],
            marker_color=status_colors.get(status, 'rgb(128, 128, 128)'))
        # default color if status is not in the dictionary
        for status in set(statuses)
    ])

    # Configure the layout of the per-minute breakdown graph
    fig.update_layout(
        barmode='stack',
        title='Online Rating Request per Minute: {}'.format(timestamp),
        xaxis_title='Timestamp',
        yaxis_title='Total Number'
    )

    cache['minute_breakdown']['pq_clientid'] = pq_clientid
    cache['minute_breakdown']['timestamp'] = timestamp
    cache['minute_breakdown']['current_time'] = time.time()
    cache['minute_breakdown']['data'] = jsonify(fig.to_json())
    return cache['minute_breakdown']['data']

@app.route('/graph_data/client', defaults={'pq_clientid': None})
@app.route('/graph_data/client/<pq_clientid>')
@limiter.limit("10/minute")  # Limit this endpoint to 10 requests per minute
def graph_data(pq_clientid):
    print("graph_data")
    print(pq_clientid)

    #input_string = request.args.get('pq_clientid', None)  # Get the pq_clientid from the query string
    words = pq_clientid.split("-")

    pq_clientid =  words[0]  # "none"
    if len(words) > 1:
        server = words[1]  # "none"
    else:
        server = "none"


    # SQL query string
    sql_query = (
        "SELECT date_trunc('hour', lsr.stamp), lsd.status, COUNT(*) "
        "FROM log_service_requests AS lsr "
        "JOIN log_service_requests_details AS lsd ON lsr.srnumber = lsd.srnumber "
        "WHERE lsr.stamp >= NOW() - INTERVAL '24 hours' "
    )

    if pq_clientid != 'None':
        sql_query += "AND lsr.pq_clientid = %s "

    if server != 'None':
        if server == 'Testing':
            # Use servername starting with "UAT" for testing
            sql_query += "AND lsr.servername LIKE 'UAT%' "
        elif server == 'Production':
            # Use servername starting with "PROD" for production
            sql_query += "AND lsr.servername LIKE 'PROD%' "

    sql_query += "GROUP BY date_trunc('hour', lsr.stamp), lsd.status"

    print(sql_query)
    if cache['graph_data']['pq_clientid'] == pq_clientid and cache['graph_data']['sql_query'] == sql_query and time.time() - cache['graph_data']['timestamp'] < 60:
        print("cache")
        return cache['graph_data']['data']

    # Connect to the PostgreSQL server
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

    # Create a cursor to execute queries
    cursor = conn.cursor()

    cursor.execute(sql_query)

    print("Query: " + sql_query)

    results = cursor.fetchall()

    print(sql_query)
    print(results)
    # Extract timestamps, statuses, and values from the results
    timestamps, statuses, values = zip(*results)

    # Create a dictionary mapping timestamps to dictionaries that map statuses to values
    data = {}
    for timestamp, status, value in results:
        if timestamp not in data:
            data[timestamp] = {}
        data[timestamp][status] = value

    # Close the cursor and the connection
    cursor.close()
    conn.close()

    # Create the stacked bar graph using Plotly
    fig = go.Figure(data=[
        go.Bar(name=status_labels.get(status, status), x=list(data.keys()),
               y=[data[timestamp].get(status, 0) for timestamp in data],
               marker_color=status_colors.get(status, 'rgb(128, 128, 128)')) # default color if status is not in the dictionary
        for status in set(statuses)
    ])

    # Configure the layout of the bar graph
    fig.update_layout(
        barmode='stack',
        title='Requests by Hour for last 24 hours',
        xaxis_title='Hour',
        yaxis_title='Total Requests'
    )

    # At the end of the function, when storing the results in the cache, also store the SQL query
    cache['graph_data']['pq_clientid'] = pq_clientid
    cache['graph_data']['sql_query'] = sql_query
    cache['graph_data']['timestamp'] = time.time()
    cache['graph_data']['data'] = jsonify(fig.to_json())
    return cache['graph_data']['data']

from datetime import datetime, timedelta

@app.route('/graph_data/<timestamp>/<status>')
@limiter.limit("10/minute")  # Limit this endpoint to 10 requests per minute
def minute_breakdown_details(timestamp, status):
    # And for /graph_data/<timestamp>
    #if cache['minute_breakdown_details']['timestamp'] and time.time() - cache['minute_breakdown_details']['timestamp'] < 60:
    #    return cache['minute_breakdown']['data']

    # Connect to the PostgreSQL server
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

    # Create a cursor to execute queries
    cursor = conn.cursor()

    # Trim any leading or trailing spaces
    timestamp = timestamp.strip()

    # Convert the timestamp string to a datetime object
    timestamp_datetime = datetime.strptime(timestamp, "%Y-%m-%d %H:%M")

    # Calculate the start and end timestamps for the selected hour
    start_timestamp = timestamp_datetime
    end_timestamp = start_timestamp + timedelta(hours=1)

    #Reverse the status label dictionary
    status_labels_reverse = {v: k for k, v in status_labels.items()}
    # Convert the status string to a status code
    status = status_labels_reverse[status]

    cursor.execute(
        "SELECT date_trunc('minute', lsr.stamp) as minute, lsd.status as status, lsd.insurer as insurer, lsd.responsetime as responsetime, lsd.responsefile as responsefile, "
        "lsd.requestfile as requestfile, lsr.requestfile as sr_requestfile, lsr.responsefile as sr_responsefile, lsr.lob as lob, lsr.servername as servername, lsr.subbrokerid as subbrokerid, "
        "lsr.prov as prov, lsr.quotenumber as quotenumber, lsr.pq_clientid as pq_clientid, b.brokername as brokername "
        "FROM log_service_requests AS lsr "
        "JOIN log_service_requests_details AS lsd ON lsr.srnumber = lsd.srnumber "
        "JOIN broker AS b ON lsr.pq_clientid = b.pq_clientid "
        "WHERE lsr.stamp >= %s and lsr.stamp < %s and lsd.status = %s",
        (start_timestamp, end_timestamp, status)
    )

    results = cursor.fetchall()

    # Convert results to a list of dictionaries
    result_list = []
    columns = [column[0] for column in cursor.description]  # Get column names
    for row in results:
        result_dict = dict(zip(columns, row))
        result_list.append(result_dict)

    # Close the cursor and the connection
    cursor.close()
    conn.close()

    # Return JSON representation of the result_list
    return jsonify(result_list)

@app.route('/process_file_location', methods=['POST'])
@limiter.limit("10/minute")  # Limit this endpoint to 10 requests per minute
def process_file_location():
    data = request.get_json()
    responsefile_path = data['responsefile_path']
    # Process the file location and perform the desired action
    # Remap sv-hv15 to sv-hv15.servepoint.net
    print(data)
    base_directory = '\\\\sv-hv15.servpoint.net'
    print(base_directory)
    print(JAWS_LOGS)
    responsefile_path = responsefile_path.replace('\\\\sv-hv15', '')
    responsefile_path = base_directory + responsefile_path
    print(responsefile_path)

    # Verify that the final path is still within the base_directory
    if not str(responsefile_path).startswith(str(base_directory)):
        abort(400, description='Invalid file path.')

    # Return the file for download
    return send_file(responsefile_path, as_attachment=True)
@app.route('/brokers')
def brokers():
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    cursor = conn.cursor()
    cursor.execute(
        "SELECT DISTINCT broker.brokername, broker.pq_clientid "
        "FROM broker "
        "JOIN log_service_requests AS lsr ON broker.pq_clientid = lsr.pq_clientid "
        "WHERE lsr.stamp >= NOW() - INTERVAL '24 hours' "
        "ORDER BY broker.brokername ASC"
    )

    results = cursor.fetchall()
    print(results)
    return jsonify(results)

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
