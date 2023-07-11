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

    # Retrieve data from the PostgreSQL server and aggregate by hour
    #cursor.execute("SELECT date_trunc('hour', stamp), count(*) FROM log_service_requests WHERE stamp >= NOW() - INTERVAL '24 hours' GROUP BY 1")
    cursor.execute(
        "SELECT date_trunc('hour', lsr.stamp), COUNT(*) "
        "FROM log_service_requests AS lsr "
        "JOIN log_service_requests_details AS lsd ON lsr.srnumber = lsd.srnumber "
        "WHERE lsr.stamp >= NOW() - INTERVAL '24 hours' "
        "GROUP BY date_trunc('hour', lsr.stamp)")
    results = cursor.fetchall()

    # Extract timestamps and values from the results
    timestamps = [row[0] for row in results]
    values = [row[1] for row in results]

    # Close the cursor and the connection
    cursor.close()
    conn.close()

    # Create the bar graph using Plotly
    fig = go.Figure(data=[go.Bar(x=timestamps, y=values)])

    # Configure the layout of the bar graph
    fig.update_layout(
        title='Total SR Numbers per Hour',
        xaxis_title='Hour',
        yaxis_title='Total SR Numbers'
    )

    # Convert the figure to JSON and return the data
    return jsonify(fig.to_json())

from datetime import datetime, timedelta

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

    # Print the timestamp for verification
    print(timestamp)

    # Convert the timestamp string to a datetime object
    timestamp_datetime = datetime.strptime(timestamp, "%Y-%m-%d %H:%M")

    # Calculate the start and end timestamps for the selected hour
    start_timestamp = timestamp_datetime
    end_timestamp = start_timestamp + timedelta(hours=1)

    # Retrieve data from the PostgreSQL server for the specified hour
    #cursor.execute("SELECT date_trunc('minute', stamp) as minute, count(*) FROM log_service_requests WHERE stamp >= %s AND stamp < %s GROUP BY 1", (start_timestamp, end_timestamp))
    cursor.execute(
        "SELECT date_trunc('minute', lsr.stamp) as minute, COUNT(*) "
        "FROM log_service_requests AS lsr "
        "JOIN log_service_requests_details AS lsd ON lsr.srnumber = lsd.srnumber "
        "WHERE lsr.stamp >= %s and lsr.stamp < %s "
        "GROUP BY 1", (start_timestamp, end_timestamp))

    results = cursor.fetchall()

    # Extract timestamps and values from the results
    timestamps = [row[0] for row in results]
    values = [row[1] for row in results]

    # Close the cursor and the connection
    cursor.close()
    conn.close()

    # Create the per-minute breakdown graph using Plotly
    fig = go.Figure(data=[go.Bar(x=timestamps, y=values)])

    # Configure the layout of the per-minute breakdown graph
    fig.update_layout(
        title='SR Numbers Breakdown for Hour: {}'.format(timestamp),
        xaxis_title='Timestamp',
        yaxis_title='SR Numbers'
    )

    # Convert the figure to JSON and return the data
    return jsonify(fig.to_json())


if __name__ == '__main__':
    app.run(debug=True)
