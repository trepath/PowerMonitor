import configparser
import os
import time
import psycopg2
import plotly.graph_objects as go
from flask import Flask, render_template, jsonify

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
    500: 'rgb(200, 64, 64)',
    504: 'rgb(255, 128, 128)',
    403: 'rgb(255, 128, 0)'
    # Add more as needed
}
status_labels = {
    0: 'Credit',
    1: 'Success',
    500: 'Insurer Service Error',
    504: 'Timeout',
    403: 'Authentication Error'
    # Add more as needed
}

# Create Flask web server
app = Flask(__name__)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/graph_data')
def graph_data():
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
        title='Total SR Numbers per Hour',
        xaxis_title='Hour',
        yaxis_title='Total SR Numbers'
    )

    # Convert the figure to JSON and return the data
    return jsonify(fig.to_json())

from datetime import datetime, timedelta

@app.route('/graph_data/<timestamp>')
@app.route('/graph_data/<timestamp>')
def minute_breakdown(timestamp):
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
        title='SR Numbers Breakdown for Hour: {}'.format(timestamp),
        xaxis_title='Timestamp',
        yaxis_title='SR Numbers'
    )

    # Convert the figure to JSON and return the data
    return jsonify(fig.to_json())


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
