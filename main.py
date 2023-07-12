import configparser
import os
import time

import psycopg2
import plotly.graph_objects as go
from flask import Flask, render_template, jsonify
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

# Define color codes and labels for status codes
status_colors = {
    0: 'rgb(200, 200, 200)',
    1: 'rgb(100, 200, 100)',
    500: 'rgb(100, 100, 100)',
    504: 'rgb(255, 128, 128)',
    403: 'rgb(255, 128, 0)',
    6: 'rgb(255, 0, 0)',
    2: 'rgb(255, 0, 0)',
    400: 'rgb(0, 0, 255)'
    # Add more as needed
}
status_labels = {
    0: 'Credit',
    1: 'Success',
    500: 'Insurer Service Error',
    504: 'Timeout',
    403: 'Authentication Error',
    6: 'Critical Error',
    2: 'Critical Error WAWA',
    400: 'Broker Error'
    # Add more as needed
}

# Define a cache to store the graph data
cache = {
    'graph_data': {
        'timestamp': None,
        'data': None,
    },
    'minute_breakdown': {
        'request': None,  # The request object for the current request
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


@app.route('/graph_data')
@limiter.limit("10/minute")  # Limit this endpoint to 10 requests per minute
def graph_data():
    if cache['graph_data']['timestamp'] and time.time() - cache['graph_data']['timestamp'] < 60:
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

    # Retrieve data from the PostgreSQL server and aggregate by hour and status
    cursor.execute(
        "SELECT date_trunc('hour', lsr.stamp), lsd.status, COUNT(*) "
        "FROM log_service_requests AS lsr "
        "JOIN log_service_requests_details AS lsd ON lsr.srnumber = lsd.srnumber "
        "WHERE lsr.stamp >= NOW() - INTERVAL '24 hours' "
        "GROUP BY date_trunc('hour', lsr.stamp), lsd.status")
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
               marker_color=status_colors.get(status, 'rgb(128, 128, 128)')) # default color if status is not in the dictionary
        for status in set(statuses)
    ])

    # Configure the layout of the bar graph
    fig.update_layout(
        barmode='stack',
        title='Total Online Requests per Hour',
        xaxis_title='Hour',
        yaxis_title='Total Requests'
    )

    cache['graph_data']['timestamp'] = time.time()
    cache['graph_data']['data'] = jsonify(fig.to_json())
    return cache['graph_data']['data']

    # Convert the figure to JSON and return the data
    return jsonify(fig.to_json())

from datetime import datetime, timedelta

@app.route('/graph_data/<timestamp>')
@limiter.limit("10/minute")  # Limit this endpoint to 10 requests per minute
def minute_breakdown(timestamp):
    # And for /graph_data/<timestamp>
    if cache['minute_breakdown']['timestamp'] and time.time() - cache['minute_breakdown']['timestamp'] < 60:
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

    # Retrieve data from the PostgreSQL server for the specified hour and group by minute and status
    cursor.execute(
        "SELECT date_trunc('minute', lsr.stamp) as minute, lsd.status, COUNT(*) "
        "FROM log_service_requests AS lsr "
        "JOIN log_service_requests_details AS lsd ON lsr.srnumber = lsd.srnumber "
        "WHERE lsr.stamp >= %s and lsr.stamp < %s "
        "GROUP BY date_trunc('minute', lsr.stamp), lsd.status", (start_timestamp, end_timestamp))

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

    # And for /graph_data/<timestamp>
    cache['minute_breakdown']['timestamp'] = time.time()
    cache['minute_breakdown']['data'] = jsonify(fig.to_json())

    # Convert the figure to JSON and return the data
    return jsonify(fig.to_json())

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

    # Retrieve data from the PostgreSQL server for the specified hour and group by minute and status
    cursor.execute(
        "SELECT date_trunc('minute', lsr.stamp) as minute, lsd.status "
        "FROM log_service_requests AS lsr "
        "JOIN log_service_requests_details AS lsd ON lsr.srnumber = lsd.srnumber "
        "WHERE lsr.stamp >= %s and lsr.stamp < %s and lsd.status = %s",
        (start_timestamp, end_timestamp, status))

    results = cursor.fetchall()

    # Convert results to a list of dictionaries
    result_list = []
    for minute, status in results:
        result_list.append({'minute': minute, 'status': status})

    # Close the cursor and the connection
    cursor.close()
    conn.close()

    # Return JSON representation of the result_list
    return jsonify(result_list)

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
