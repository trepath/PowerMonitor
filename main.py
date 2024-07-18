import configparser
import os
import time

import pandas as pd
import psycopg2
import plotly.graph_objects as go
import plotly.io as pio
import requests as requests
from flask import Flask, render_template, jsonify, send_file, request, abort, render_template_string
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

from flask_limiter.util import get_remote_address

# Read the configuration file
config = configparser.ConfigParser()
config.read('config.ini')

# PostgreSQL connection details
DB_HOST = config['PostgreSQL']['DB_HOST']
DB_HOST_MAIN = config['PostgreSQL']['DB_HOST_MAIN']
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
    6: 'rgb(200, 95, 0)',
    2: 'rgb(255, 0, 0)',
    400: 'rgb(0, 0, 255)',
    200: 'rgb(0, 128, 128)',
    300: 'rgb(0, 128, 128)'
    # Add more as needed
}
status_labels = {
    0: 'Credit',
    1: 'Success',
    500: '500 - Insurer Service Error',
    504: '504 - Timeout',
    403: '403 - Authentication Error',
    6: 'JAWS Validation Error - Creds?',
    2: 'Critical Error WAWA',
    400: '400 - Broker Error',
    200: '200 - Success DOC Credit',
    300: '300 - Undocumented'
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
    'last_hour_graph_data': {
        'timestamp': None,
        'data': None,
    },
    'graph_top_brokers': {
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
@limiter.limit("30/minute")  # Limit this endpoint to 10 requests per minute
def minute_breakdown(timestamp):
    print("minute_breakdown " + timestamp)

    input_string = request.args.get('pq_clientid', None)  # Get the pq_clientid from the query string
    words = input_string.split("-")

    pq_clientid = words[0]  # "none"
    if len(words) > 1:
        server = words[1]  # "none"
    else:
        server = "none"

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
            sql_query += "AND lsr.servername LIKE 'UAT%%' "
        elif server == 'Production':
            # Use servername starting with "PROD" for production
            sql_query += "AND lsr.servername LIKE 'PROD%%' "

    sql_query += "GROUP BY date_trunc('minute', lsr.stamp), lsd.status"

    query_params = (start_timestamp, end_timestamp, pq_clientid) if pq_clientid != 'None' else (
        start_timestamp, end_timestamp)

    print("query_params:" + str(query_params))

    return execute_and_cache_query('minute_breakdown', pq_clientid, timestamp, sql_query, 'pq_clientid',
                                   server + ' JAWS Requests At: {}'.format(timestamp), 'Timestamp',
                                   'Total Number', start_timestamp, end_timestamp, query_params)


@app.route('/graph_data_client/<pq_clientid>/<server>')
@limiter.limit("30/minute")  # Limit this endpoint to 10 requests per minute
def graph_data(pq_clientid, server):
    print("graph_data")
    print(pq_clientid)

    # SQL query string
    sql_query = (
        "SELECT date_trunc('hour', lsr.stamp), lsd.status, COUNT(*) "
        "FROM log_service_requests AS lsr "
        "JOIN log_service_requests_details AS lsd ON lsr.srnumber = lsd.srnumber "
        "WHERE lsr.stamp >= NOW() - INTERVAL '24 hours' "
    )

    query_params = []

    if pq_clientid != 'None':
        sql_query += "AND lsr.pq_clientid = %s "
        query_params.append(pq_clientid)

    if server != 'None':
        if server == 'Testing':
            # Use servername starting with "UAT" for testing
            sql_query += "AND lsr.servername LIKE 'UAT%%' "
        elif server == 'Production':
            # Use servername starting with "PROD" for production
            sql_query += "AND lsr.servername LIKE 'PROD%%' "

    sql_query += "GROUP BY date_trunc('hour', lsr.stamp), lsd.status"

    # Starttime stamp is 24hrs ago now
    start_timestamp = datetime.now() - timedelta(hours=24)
    end_timestamp = datetime.now()

    return execute_and_cache_query('graph_data', pq_clientid, None, sql_query, 'pq_clientid',
                                   server + ' JAWS Requests for last 24 hours', 'Hour', 'Total Requests',
                                   start_timestamp, end_timestamp, tuple(query_params))


from datetime import datetime, timedelta


@app.route('/minute_breakdown_details/<timestamp>/<status>/<server>')
@limiter.limit("30/minute")  # Limit this endpoint to 10 requests per minute
def minute_breakdown_details(timestamp, status, server):
    # And for /graph_data/<timestamp>
    # if cache['minute_breakdown_details']['timestamp'] and time.time() - cache['minute_breakdown_details']['timestamp'] < 60:
    #    return cache['minute_breakdown']['data']
    print("minute_breakdown_details " + timestamp + " " + status + " " + server)
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
    # end_timestamp = start_timestamp + timedelta(hours=1)
    end_timestamp = start_timestamp + timedelta(minutes=1)

    # Reverse the status label dictionary
    status_labels_reverse = {v: k for k, v in status_labels.items()}
    # Convert the status string to a status code
    status = status_labels_reverse[status]
    print("status " + str(status))
    # SQL query string
    sql_query = (
        "SELECT date_trunc('minute', lsr.stamp) as minute, lsr.srnumber as srnumber, lsd.status as status, lsd.insurer as insurer, lsd.responsetime as responsetime, lsd.responsefile as responsefile, "
        "lsd.requestfile as requestfile, lsr.requestfile as sr_requestfile, lsr.responsefile as sr_responsefile, lsr.lob as lob, lsr.servername as servername, lsr.subbrokerid as subbrokerid, "
        "lsr.prov as prov, lsr.quotenumber as quotenumber, lsr.pq_clientid as pq_clientid, b.brokername as brokername, lsr.username as username "
        "FROM log_service_requests AS lsr "
        "JOIN log_service_requests_details AS lsd ON lsr.srnumber = lsd.srnumber "
        "JOIN broker AS b ON lsr.pq_clientid = b.pq_clientid "
        "WHERE lsr.stamp >= %s and lsr.stamp < %s"
    )

    print(server)

    if server != 'None':
        if server == 'Testing':
            # Use servername starting with "UAT" for testing
            sql_query += " AND lsr.servername LIKE 'UAT%%' "
        elif server == 'Production':
            # Use servername starting with "PROD" for production
            sql_query += " AND lsr.servername LIKE 'PROD%%' "

    print(sql_query)
    print((start_timestamp, end_timestamp, status))
    cursor.execute(sql_query, (start_timestamp, end_timestamp))

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
@limiter.limit("30/minute")  # Limit this endpoint to 10 requests per minute
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


@app.route('/brokers/<server>')
@limiter.limit("30/minute")  # Limit this endpoint to 10 requests per minute
def brokers(server):
    print("brokers " + server)
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    cursor = conn.cursor()

    sql_query = (
        "SELECT DISTINCT broker.brokername, broker.pq_clientid "
        "FROM broker "
        "JOIN log_service_requests AS lsr ON broker.pq_clientid = lsr.pq_clientid "
        "WHERE lsr.stamp >= NOW() - INTERVAL '24 hours' "
    )

    if server != 'None':
        if server == 'Testing':
            # Use servername starting with "UAT" for testing
            sql_query += " AND lsr.servername LIKE 'UAT%%' "
        elif server == 'Production':
            # Use servername starting with "PROD" for production
            sql_query += " AND lsr.servername LIKE 'PROD%%' "

    sql_query += " ORDER BY broker.brokername ASC"

    cursor.execute(sql_query)

    results = cursor.fetchall()
    print(results)
    # Close the cursor and the connection
    cursor.close()
    conn.close()

    return jsonify(results)


def execute_and_cache_query(route, pq_clientid, timestamp, sql_query, cache_key, plot_title, xaxis_title, yaxis_title,
                            start_timestamp, end_timestamp, query_params=None):
    if cache[route][cache_key] == pq_clientid and cache[route]['sql_query'] == sql_query and time.time() - cache[route][
        'timestamp'] < 60:
        print("Using cached data")
        return cache[route]['data']

    # Connect to the PostgreSQL server
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

    # Create a cursor to execute queries
    cursor = conn.cursor()

    if query_params:
        cursor.execute(sql_query, query_params)
    else:
        cursor.execute(sql_query)

    results = cursor.fetchall()

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
               marker_color=status_colors.get(status, 'rgb(128, 128, 128)'))
        # default color if status is not in the dictionary
        for status in set(statuses)
    ])

    if query_params != () and query_params is not None and len(query_params) == 2:
        print('query_params: ' + str(query_params))
        # Configure the layout of the bar graph
        fig.update_layout(
            barmode='stack',
            title=plot_title,
            xaxis_title=xaxis_title,
            yaxis_title=yaxis_title,
            xaxis=dict(range=[query_params[0], query_params[1]])
        )
    else:
        # If the start and end timestamps are more than 2 hours apart, set the tick interval to 1 hour
        if (end_timestamp - start_timestamp).total_seconds() > 7200:
            tick_interval = 'H'
            # Get all timestamps within the desired range
            timestamp_range = pd.date_range(start=start_timestamp, end=end_timestamp, freq=tick_interval)
            print(timestamp_range)

            # set the xaxis dictionary to include the tickformat
            xaxis_dict = dict(
                tickmode='array',  # Set the tick mode to custom
                tickvals=timestamp_range,  # Set the tick values to cover the desired range
                ticktext=[timestamp.strftime('%H:00') for timestamp in timestamp_range],
                dtick=1
            )

        else:
            tick_interval = 'min'
            # Get all timestamps within the desired range
            timestamp_range = pd.date_range(start=start_timestamp, end=end_timestamp, freq=tick_interval)
            print(timestamp_range)

            # set the xaxis dictionary to include the tickformat
            xaxis_dict = dict(
                tickmode='array',  # Set the tick mode to custom
                tickvals=timestamp_range,  # Set the tick values to cover the desired range
                ticktext=[timestamp.strftime('%H:%M') for timestamp in timestamp_range],
                dtick=1
            )

        # Get the corresponding data values for each timestamp
        data_values = [data[timestamp][status] if timestamp in data and status in data[timestamp] else 0
                       for timestamp in timestamp_range
                       for status in set(statuses)]

        # Configure the layout of the bar graph
        fig.update_layout(
            barmode='stack',
            title=plot_title,
            xaxis_title=xaxis_title,
            yaxis_title=yaxis_title,
            xaxis=xaxis_dict
        )

    # At the end of the function, when storing the results in the cache, also store the SQL query
    cache[route][cache_key] = pq_clientid
    cache[route]['sql_query'] = sql_query
    cache[route]['timestamp'] = time.time()
    cache[route]['data'] = jsonify(fig.to_json())
    return cache[route]['data']


@app.route('/graph_data_last_hour')
@limiter.limit("30/minute")  # Limit this endpoint to 10 requests per minute
def graph_data_last_hour():
    server = "Production"

    # Check the cache
    if cache['last_hour_graph_data']['timestamp'] and time.time() - cache['last_hour_graph_data']['timestamp'] < 60:
        print("Using cached data")
        return cache['last_hour_graph_data']['data']

    # Connect to the PostgreSQL server
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

    # Create a cursor to execute queries
    cursor = conn.cursor()

    # SQL query string
    sql_query = (
        "SELECT date_trunc('hour', lsr.stamp) as hour, lsd.insurer, lsd.status, COUNT(*) "
        "FROM log_service_requests AS lsr "
        "JOIN log_service_requests_details AS lsd ON lsr.srnumber = lsd.srnumber "
        "WHERE lsr.stamp >= NOW() - INTERVAL '1 hour' and (lsd.responsetime != 0 and lsd.status != 0) "
    )

    if server != 'none':
        if server == 'Testing':
            # Use servername starting with "UAT" for testing
            sql_query += "AND lsr.servername LIKE 'UAT%%' "
        elif server == 'Production':
            # Use servername starting with "PROD" for production
            sql_query += "AND lsr.servername LIKE 'PROD%%' "

    sql_query += "GROUP BY date_trunc('hour', lsr.stamp), lsd.insurer, lsd.status"
    print("sql_query: " + sql_query)
    cursor.execute(sql_query)

    results = cursor.fetchall()

    # Extract hours, insurers, statuses, and values from the results
    hours, insurers, statuses, values = zip(*results)

    # Create a dictionary mapping insurers to dictionaries that map statuses to values
    data = {}
    for hour, insurer, status, value in results:
        if insurer not in data:
            data[insurer] = {}
        data[insurer][status] = value

    # Close the cursor and the connection
    cursor.close()
    conn.close()

    # Create the stacked bar graph using Plotly
    fig = go.Figure(data=[
        go.Bar(name=status_labels.get(status, status), y=list(data.keys()),
               x=[data[insurer].get(status, 0) for insurer in data],
               orientation='h',  # this makes the graph horizontal
               marker_color=status_colors.get(status, 'rgb(128, 128, 128)'))
        # default color if status is not in the dictionary
        for status in set(statuses)
    ])

    # Configure the layout of the bar graph
    fig.update_layout(
        barmode='stack',
        title='Insurer Requests in the Last Hour',
        xaxis_title='Count',
        yaxis_title='Insurer',
    )

    # Cache the results
    cache['last_hour_graph_data']['timestamp'] = time.time()
    cache['last_hour_graph_data']['data'] = jsonify(fig.to_json())

    return cache['last_hour_graph_data']['data']


from datetime import datetime, timedelta


@app.route('/insurer_breakdown/<duration>/<status>/<insurer>/<server>')
@limiter.limit("30/minute")  # Limit this endpoint to 10 requests per minute
def insurer_breakdown(duration, status, insurer, server):
    # Reverse the status label dictionary
    status_labels_reverse = {v: k for k, v in status_labels.items()}

    print("Status raw:" + str(status))
    # Convert the status string to a status code
    status = status_labels_reverse[status]
    print("Status processed:" + str(status))

    # And for /graph_data/<timestamp>
    # if cache['minute_breakdown_details']['timestamp'] and time.time() - cache['minute_breakdown_details']['timestamp'] < 60:
    #    return cache['minute_breakdown']['data']
    print("insurer_breakdown details: " + duration + " " + str(status) + " " + insurer + " " + server)
    # Connect to the PostgreSQL server
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

    # Create a cursor to execute queries
    cursor = conn.cursor()

    # Get current timestamp
    current_timestamp = datetime.now()

    # Calculate the start timestamp by subtracting the duration
    start_timestamp = current_timestamp - timedelta(hours=int(duration))

    # Use the current timestamp as the end timestamp
    end_timestamp = current_timestamp

    # SQL query string
    sql_query = (
        "SELECT date_trunc('minute', lsr.stamp) as minute, lsr.srnumber as srnumber, lsd.status as status, lsd.insurer as insurer, lsd.responsetime as responsetime, lsd.responsefile as responsefile, "
        "lsd.requestfile as requestfile, lsr.requestfile as sr_requestfile, lsr.responsefile as sr_responsefile, lsr.lob as lob, lsr.servername as servername, lsr.subbrokerid as subbrokerid, "
        "lsr.prov as prov, lsr.quotenumber as quotenumber, lsr.pq_clientid as pq_clientid, b.brokername as brokername, lsr.username as username "
        "FROM log_service_requests AS lsr "
        "JOIN log_service_requests_details AS lsd ON lsr.srnumber = lsd.srnumber "
        "JOIN broker AS b ON lsr.pq_clientid = b.pq_clientid "
        "WHERE lsr.stamp >= %s and lsr.stamp < %s and lsd.insurer = %s and lsd.status = %s "
    )

    print(server)

    if server != 'None':
        if server == 'Testing':
            # Use servername starting with "UAT" for testing
            sql_query += " AND lsr.servername LIKE 'UAT%%' "
        elif server == 'Production':
            # Use servername starting with "PROD" for production
            sql_query += " AND lsr.servername LIKE 'PROD%%' "

    print(sql_query)
    # cursor.execute(sql_query, (start_timestamp, end_timestamp, insurer, status))
    cursor.execute(sql_query, (start_timestamp, end_timestamp, insurer, status))

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

@app.route('/broker_quoting_details/<pq_clientid>/<interval>/<server>')
@limiter.limit("30/minute")  # Limit this endpoint to 10 requests per minute
def broker_quoting_details(pq_clientid, interval, server):
    print("broker_quoting_details: " + pq_clientid + " " + interval + " " + server)

    # SQL query string
    sql_query = (
        "SELECT date_trunc('hour', lsr.stamp), lsd.status, COUNT(*) "
        "FROM log_service_requests AS lsr "
        "JOIN log_service_requests_details AS lsd ON lsr.srnumber = lsd.srnumber "
        "WHERE lsr.stamp >= NOW() - INTERVAL '24 hours' "
    )

    query_params = []

    if pq_clientid != 'None':
        sql_query += "AND lsr.pq_clientid = %s "
        query_params.append(pq_clientid)

    if server != 'None':
        if server == 'Testing':
            # Use servername starting with "UAT" for testing
            sql_query += "AND lsr.servername LIKE 'UAT%%' "
        elif server == 'Production':
            # Use servername starting with "PROD" for production
            sql_query += "AND lsr.servername LIKE 'PROD%%' "

    sql_query += "GROUP BY date_trunc('hour', lsr.stamp), lsd.status"

    # Starttime stamp is 24hrs ago now
    start_timestamp = datetime.now() - timedelta(hours=24)
    end_timestamp = datetime.now()

    return execute_and_cache_query('graph_data', pq_clientid, None, sql_query, 'pq_clientid',
                                   server + ' JAWS Requests for last 24 hours', 'Hour', 'Total Requests',
                                   start_timestamp, end_timestamp, tuple(query_params))

@app.route('/current-brokers')
@limiter.limit("30/minute")  # Limit this endpoint to 10 requests per minute
def currentBrokers():
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    cursor = conn.cursor()

    sql_query = (
        "SELECT DISTINCT broker.brokername, broker.pq_clientid "
        "FROM broker "
        "WHERE broker.expiry >= NOW() - INTERVAL '1 month' "
        "AND (broker.product_id LIKE '%RC%' OR broker.product_id LIKE '%PQ%' OR broker.product_id LIKE '%RQ%') "
        "ORDER BY broker.brokername ASC"
    )

    cursor.execute(sql_query)

    results = cursor.fetchall()
    print(results)

    # Close the cursor and the connection
    cursor.close()
    conn.close()
    return jsonify(results)

@app.route('/broker-quick-stats/<broker_pq_clientid>')
@limiter.limit("30/minute")  # Limit this endpoint to 10 requests per minute
def brokerQuickStats(broker_pq_clientid):
    print("brokerQuickStats: " + broker_pq_clientid)
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    cursor = conn.cursor()

    # First find the most recent pq hotfix
    sql_query = (
        "SELECT hotfix_rid "
        "FROM hotfix "
        "WHERE sw_type = 'PQ' "
        "ORDER BY date_created DESC "
        "LIMIT 1"
    )

    cursor.execute(sql_query)
    result = cursor.fetchone()
    print('Current hotfix rid: ' + str(result[0]))

    if broker_pq_clientid != 'None' and broker_pq_clientid != 'undefined':
        # Find all the brokers that have an expiry date after 1 month ago, return all relevant data
        sql_query_brokers = (
            "SELECT b.brokername, b.pq_clientid, b.prod_id, b.expiry, software_info.hotfix_rid as hotfix_rid, %s AS current_hotfixrid  "
            "FROM broker as b, software_info "
            "WHERE b.pq_clientid = software_info.pq_clientid "
        )
        sql_query_brokers += "AND b.pq_clientid = %s AND b.expiry >= NOW() - INTERVAL '1 month' and b.product_id LIKE '%%PQ%%' "
        sql_query_brokers += "AND b.testing is FALSE and b.demo is FALSE "
        sql_query_brokers += "ORDER BY b.brokername ASC"
        cursor.execute(sql_query_brokers, (result[0], broker_pq_clientid, ))
    else:
        # Find all the brokers that have an expiry date after 1 month ago, return all relevant data
        sql_query_brokers = (
            "SELECT b.brokername, b.pq_clientid, b.prod_id, b.expiry, software_info.hotfix_rid as hotfix_rid, %s AS current_hotfixrid "
            "FROM broker as b, software_info "
            "WHERE b.pq_clientid = software_info.pq_clientid "
        )
        sql_query_brokers += "AND b.expiry >= NOW() - INTERVAL '1 month' "
        sql_query_brokers += "AND %s != hotfix_rid AND b.product_id LIKE '%%PQ%%' "
        sql_query_brokers += "AND b.testing is FALSE and b.demo is FALSE "
        sql_query_brokers += "AND b.brokername NOT LIKE '%%PowerSoft%%' "
        sql_query_brokers += "AND b.brokername NOT LIKE '%%Powersoft%%' "
        sql_query_brokers += "ORDER BY b.brokername ASC"
        cursor.execute(sql_query_brokers, (result[0], result[0], ))

    results = cursor.fetchall()
    print(results)

    # Close the cursor and the connection
    cursor.close()
    conn.close()

    return jsonify(results)


@app.route('/broker-rate-engines/<broker_pq_clientid>')
@limiter.limit("30/minute")  # Limit this endpoint to 10 requests per minute
def brokerRateEngines(broker_pq_clientid):
    print("brokerRateEngines: " + broker_pq_clientid)
    if broker_pq_clientid == 'undefined' or broker_pq_clientid == 'None':
        return brokerQuickStats(broker_pq_clientid)

    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    cursor = conn.cursor()

    # First find the most recent pq hotfix
    sql_query = (
        "SELECT rateengine.insurer, rateengine.prov, rateengine.line, rateengine.version as current_version, client_re.version "
        "FROM client_re "
        "JOIN rateengine ON client_re.rid = rateengine.rid "
        "WHERE client_re.pq_clientid = %s "
        "ORDER BY rateengine.insurer, rateengine.prov, rateengine.line"
    )

    cursor.execute(sql_query, (broker_pq_clientid,))  # Pass the parameter as a tuple

    results = cursor.fetchall()
    print(results)

    # Close the cursor and the connection
    cursor.close()
    conn.close()

    return jsonify(results)


@app.route('/broker-hotfix/<broker_pq_clientid>')
@limiter.limit("30/minute")  # Limit this endpoint to 10 requests per minute
def brokerHotfix(broker_pq_clientid):
    print("brokerHotfix: " + broker_pq_clientid)
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    cursor = conn.cursor()

    # Return if broker_pq_clientid is None
    if broker_pq_clientid == 'undefined' or broker_pq_clientid == 'None':
        return jsonify(None)
    else:
        # First find the most recent pq hotfix
        sql_query = (
            "select software_info.sw_type, software_info.version, software_info.hotfix_rid  "
            "as broker_rid, hotfix.hotfix_rid as current_rid "
            "from software_info, hotfix "
            "where pq_clientid = %s and software_info.sw_type = hotfix.sw_type  "
            "and software_info.version = hotfix.sw_version "
        )

    cursor.execute(sql_query, (broker_pq_clientid,))  # Pass the parameter as a tuple

    results = cursor.fetchall()
    print(results)

    # Close the cursor and the connection
    cursor.close()
    conn.close()

    return jsonify(results)

@app.route('/broker-quote-history/<broker_pq_clientid>')
@limiter.limit("30/minute")  # Limit this endpoint to 10 requests per minute
def brokerQuoteHistory(broker_pq_clientid):
    print("brokerQuoteHistory: " + broker_pq_clientid)
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    cursor = conn.cursor()

    # First find the most recent pq hotfix
    sql_query = (
        "SELECT lsr.pq_clientid,"
        "       lsr.subbrokerid,"
        "       COUNT(*) AS total_requests"
        " FROM log_service_requests lsr"
        " JOIN log_service_requests_details lsrd"
        "   ON lsr.srnumber = lsrd.srnumber"
        " WHERE lsr.stamp >= NOW() - INTERVAL '30 days'"
        "   AND lsr.servername LIKE 'PROD%'"
        " GROUP BY lsr.pq_clientid, lsr.subbrokerid"
        " ORDER BY total_requests DESC;"
    )

    cursor.execute(sql_query, (broker_pq_clientid,))  # Pass the parameter as a tuple

    results = cursor.fetchall()
    print(results)

    # Close the cursor and the connection
    cursor.close()
    conn.close()

    return jsonify(results)

@app.route('/top-brokers')
@limiter.limit("30/minute")  # Limit this endpoint to 10 requests per minute
def graph_top_brokers():
    server = "Production"

    # Check the cache, if the data is less than 300 seconds old, use it
    if cache['graph_top_brokers']['timestamp'] and time.time() - cache['graph_top_brokers']['timestamp'] < 300:
        print("Using cached data")
        return cache['graph_top_brokers']['data']

    # Connect to the PostgreSQL server
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

    # Create a cursor to execute queries
    cursor = conn.cursor()

    # SQL query string
    sql_query = (
        "SELECT lsr.pq_clientid,"
        "       b.brokername,"
        "       lsr.subbrokerid,"
        "       COUNT(*) AS total_requests"
        " FROM log_service_requests lsr"
        " JOIN log_service_requests_details lsrd"
        "   ON lsr.srnumber = lsrd.srnumber"
        " JOIN broker b"
        "   ON lsr.pq_clientid = b.pq_clientid"
        " WHERE lsr.stamp >= NOW() - INTERVAL '30 days'"
    )

    if server != 'none':
        if server == 'Testing':
            # Use servername starting with "UAT" for testing
            sql_query += "AND lsr.servername LIKE 'UAT%%' "
        elif server == 'Production':
            # Use servername starting with "PROD" for production
            sql_query += "AND lsr.servername LIKE 'PROD%%' "

    sql_query += " GROUP BY lsr.pq_clientid, b.brokername, lsr.subbrokerid ORDER BY total_requests DESC LIMIT 15;"

    print("sql_query: " + sql_query)
    cursor.execute(sql_query)

    results = cursor.fetchall()

    # Process the SQL results using pandas
    df = pd.DataFrame(results, columns=["pq_clientid", "brokername", "subbrokerid", "total_requests"])

    # Sort the DataFrame by total_requests in descending order
    df = df.sort_values(by="total_requests", ascending=False)

    # Create the horizontal bar graph of top 10 brokers by total requests using Plotly
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["total_requests"][::-1],  # Reverse the order of the total_requests column
        y=df["brokername"][::-1],  # Reverse the order of the brokername column
        orientation='h'
    ))

    fig.update_layout(
        title="Top 15 Brokers",
        xaxis_title="Total Requests in 30 Days",
        yaxis_title="Broker Names",
        margin=dict(l=100, r=20, t=70, b=50)
    )

    # Close the cursor and the connection
    cursor.close()
    conn.close()

    # Cache the results
    cache['graph_top_brokers']['timestamp'] = time.time()
    cache['graph_top_brokers']['data'] = jsonify(fig.to_json())

    return cache['graph_top_brokers']['data']


@app.route('/broker-detailed-history/<broker_pq_clientid>')
@limiter.limit("30/minute")  # Limit this endpoint to 10 requests per minute
def brokerDetailedHistory(broker_pq_clientid):

    print("brokerQuoteHistory: " + broker_pq_clientid)
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    cursor = conn.cursor()

    # First find the most recent pq hotfix
    sql_query = (
        """
            SELECT COUNT(DISTINCT username) AS distinct_usernames_count
            FROM public.log_service_requests
            WHERE pq_clientid = %s
            AND stamp >= NOW() - INTERVAL '30 days';
        """
    )

    cursor.execute(sql_query, (broker_pq_clientid,))
    result = cursor.fetchone()
    numUsersActual = str(result[0])
    print('NUmber of users who quoted: ' + numUsersActual)

    # Find how many licenses the broker has
    sql_query = (
        """
            SELECT numlicense
            FROM public.broker
            WHERE pq_clientid = %s;
        """
    )

    cursor.execute(sql_query, (broker_pq_clientid,))
    result = cursor.fetchone()
    numUsersAllowed = str(result[0])
    print('Number of users allowed: ' + numUsersAllowed)

    # Find the number of quotes in the last 30 days
    sql_query = (
        """
            SELECT details.status, COUNT(*) AS status_count
            FROM public.log_service_requests AS requests
            INNER JOIN public.log_service_requests_details AS details
            ON requests.srnumber = details.srnumber
            WHERE requests.pq_clientid = %s
            AND requests.stamp >= NOW() - INTERVAL '30 days'
            GROUP BY details.status;
        """
    )

    cursor.execute(sql_query, (broker_pq_clientid,))
    result = cursor.fetchall()
    quoteStatusBreakdown = result

    # Find the number of quotes in the last 30 days for last year
    sql_query = (
        """
            SELECT details.status, COUNT(*) AS status_count
            FROM public.log_service_requests AS requests
            INNER JOIN public.log_service_requests_details AS details
            ON requests.srnumber = details.srnumber
            WHERE requests.pq_clientid = %s
            AND requests.stamp >= NOW() - INTERVAL '1 year' - INTERVAL '30 days'
            AND requests.stamp < NOW() - INTERVAL '1 year'
            GROUP BY details.status;
        """
    )

    cursor.execute(sql_query, (broker_pq_clientid,))
    result = cursor.fetchall()
    quoteStatusBreakdownLastYear = result

    # Close the cursor and the connection
    cursor.close()
    conn.close()

    # Sample data for the pie chart

    # Extract labels and values from the data array
    labels = [item[0] for item in quoteStatusBreakdown]
    values = [item[1] for item in quoteStatusBreakdown]

    # Create the pie chart
    fig = go.Figure(data=[go.Pie(labels=[status_labels[label] for label in labels], values=values,customdata=labels,      # Use status codes as custom data
        hovertemplate='Status: %{customdata}<br>Value: %{value}<br>Percentage: %{percent}',  # Customize hover tooltip
        marker=dict(colors=[status_colors[label] for label in labels])  # Set custom colors based on status codes
    )])

    # Set a title for the chart
    fig.update_layout(title_text='Last Months Quote Status Breakdown')

    # Create a dictionary to store the data
    response_data = {
        "numUsersActual": numUsersActual,
        "numUsersAllowed": numUsersAllowed,
        "quoteStatusBreakdown": quoteStatusBreakdown,
        "quoteStatusBreakdownLastYear": quoteStatusBreakdownLastYear,
        "pieChart": fig.to_json()
    }

    # Return the JSON response using jsonify
    return jsonify(response_data)

@app.route('/broker-quoting-history/<broker_pq_clientid>')
@limiter.limit("30/minute")  # Limit this endpoint to 10 requests per minute
def brokerQuotingHistory(broker_pq_clientid):

    print("brokerQuoteHistory: " + broker_pq_clientid)
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    cursor = conn.cursor()

    # First find the most recent pq hotfix
    sql_query = (
        "SELECT date_trunc('month', lsr.stamp), COUNT(*) "
        "FROM log_service_requests AS lsr "
        "JOIN log_service_requests_details AS lsd ON lsr.srnumber = lsd.srnumber "
        "WHERE lsr.stamp >= NOW() - INTERVAL '1 year' "
        "AND lsr.pq_clientid = %s "
        "GROUP BY date_trunc('month', lsr.stamp) "
        "ORDER BY date_trunc('month', lsr.stamp) "
    )

    cursor.execute(sql_query, (broker_pq_clientid,))
    results = cursor.fetchall()

    # Close the cursor and the connection
    cursor.close()
    conn.close()

    # Extract data from the query results
    months = []
    statuses = []
    counts = []

    for row in results:
        months.append(row[0].strftime('%b %Y'))  # Convert the timestamp to a string format
        counts.append(row[1])

    # Create the bar graph using Plotly
    fig = go.Figure()

    fig.add_trace(go.Bar(x=months, y=counts))

    # Update layout and labels
    fig.update_layout(
        title_text='Broker Quoting History',
        xaxis_title='Month',
        yaxis_title='Number of Quotes'
    )

    # Show the bar graph
    #fig.show()

    # If you want to return the JSON response with Plotly data, use the following instead of fig.show()
    return jsonify(fig.to_json())


@app.route('/health-check')
@limiter.limit("30/minute")
def healthCheck():
    messages = []

    try:
        # Attempt to connect to the database
        conn = psycopg2.connect(
            host=DB_HOST_MAIN,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cursor = conn.cursor()

        # Your query
        query = """
        SELECT COUNT(*)
        FROM public.rw_progresslog
        WHERE rstamp >= CURRENT_DATE
        AND service = 'IAR'
        AND server != 'UATRater1'
        AND status = 'NO RESPONSE';
        """

        cursor.execute(query)
        count = cursor.fetchone()[0]  # Fetch the count result

        # Determine status and color based on the count
        if count == 0:
            health_status = "All systems operational"
            color = "green"
        elif count < 5:
            health_status = f"'NO RESPONSE' entries today: {count} - Monitor systems"
            color = "orange"
        else:
            health_status = f"'NO RESPONSE' entries today: {count} - System issues detected"
            color = "red"

        cursor.close()
        conn.close()
    except Exception as e:
        # Handle the exception if the connection to the database fails
        health_status = "Unable to connect to the SQL server - Critical system issue"
        color = "red"
        # Optionally, you can log the exception message for debugging
        print("Database connection failed:", str(e))

        # Close the cursor and the connection
        cursor.close()
        conn.close()
    finally:
        # Close the cursor and the connection
        cursor.close()
        conn.close()

    #return jsonify({"status": health_status, "color": color})
    messages.append({"status": health_status, "color": color})

    # Add more health checks here


    # Wrap messages in an object
    return jsonify({"messages": messages})


jawsServers = [
    {'name': '<a href="https://jaws.power-soft.com/balancer">jaws.power-soft.com</a>', 'url': 'https://jaws.power-soft.com/axis2/services/ServiceProvider?wsdl', 'expected_text': 'Grabenwerks Service'},
    {'name': 'prodjaws1.servpoint.net', 'url': 'http://prodjaws1.servpoint.net/axis2/services/ServiceProvider?wsdl', 'expected_text': 'Grabenwerks Service'},
    {'name': 'prodjaws2.servpoint.net', 'url': 'http://prodjaws2.servpoint.net/axis2/services/ServiceProvider?wsdl', 'expected_text': 'Grabenwerks Service'},
    {'name': 'prodjaws3.servpoint.net', 'url': 'http://prodjaws3.servpoint.net/axis2/services/ServiceProvider?wsdl', 'expected_text': 'Grabenwerks Service'},
    {'name': '<a href="https://jawstest.power-soft.com/balancer">jawstest.power-soft.com</a>', 'url': 'https://jaws.power-soft.com/axis2/services/ServiceProvider?wsdl',
     'expected_text': 'Grabenwerks Service'},
    {'name': 'uatjaws1.servpoint.net', 'url': 'http://uatjaws1.servpoint.net/axis2/services/ServiceProvider?wsdl',
     'expected_text': 'Grabenwerks Service'},
    {'name': 'uatjaws2.servpoint.net', 'url': 'http://uatjaws2.servpoint.net/axis2/services/ServiceProvider?wsdl',
     'expected_text': 'Grabenwerks Service'},
    {'name': 'devjaws1.powersoft.net', 'url': 'http://devjaws1.powersoft.net/axis2/services/ServiceProvider?wsdl',
     'expected_text': 'Grabenwerks Service'},
    # Add more servers as needed
]

rateworksServers = [
    {'name': '<a href="http://prodraterspool.servpoint.net/balancer">prodraterspool.servpoint.net</a>', 'url': 'http://prodraterspool.servpoint.net/rateworks.wsdl', 'expected_text': 'RateWorks'},
    {'name': 'prodrater1.servpoint.net', 'url': 'http://prodrater1.servpoint.net/rateworks.wsdl', 'expected_text': 'RateWorks'},
    {'name': 'prodrater2.servpoint.net', 'url': 'http://prodrater2.servpoint.net/rateworks.wsdl', 'expected_text': 'RateWorks'},
    {'name': 'prodrater3.servpoint.net', 'url': 'http://prodrater3.servpoint.net/rateworks.wsdl', 'expected_text': 'RateWorks'},
    {'name': 'prodrater4.servpoint.net', 'url': 'http://prodrater4.servpoint.net/rateworks.wsdl', 'expected_text': 'RateWorks'},
    {'name': '<a href="http://uatraterspool.servpoint.net/balancer">uatraterspool.servpoint.net</a>', 'url': 'http://uatraterspool.servpoint.net/rateworks.wsdl', 'expected_text': 'RateWorks'},
    {'name': 'uatrater1.servpoint.net', 'url': 'http://uatrater1.servpoint.net/rateworks.wsdl', 'expected_text': 'RateWorks'},
    {'name': 'uatrater2.servpoint.net', 'url': 'http://uatrater2.servpoint.net/rateworks.wsdl', 'expected_text': 'RateWorks'},
    {'name': 'qarater1.servpoint.net', 'url': 'http://qarater1.servpoint.net/rateworks.wsdl', 'expected_text': 'RateWorks'},
]

def check_server_status(url, expected_text):
    try:
        response = requests.get(url)
        if expected_text in response.text:
            return '#64C864'  # Olive color
        else:
            return 'red'
    except Exception as e:
        print(f"Error checking server {url}: {e}")
        return 'red'

@app.route('/server-status')
@limiter.limit("30/minute")
def server_status():
    status_html = '<br><b>Jaws Servers</b><br>'
    for server in jawsServers:
        color = check_server_status(server['url'], server['expected_text'])
        status_html += f'<div style="background-color: {color}; padding: 5px; margin: 2px; height: 12px;">{server["name"]}</div>'

    status_html += '<br><b>Rateworks Servers</b><br>'
    for server in rateworksServers:
        color = check_server_status(server['url'], server['expected_text'])
        status_html += f'<div style="background-color: {color}; padding: 5px; margin: 2px; height: 12px;">{server["name"]}</div>'


    return render_template_string(status_html)

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
