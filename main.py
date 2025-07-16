import configparser
import os
import time
from contextlib import contextmanager

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
    500: 'rgb(256, 0, 0)',
    503: 'rgb(200, 0, 0)',
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
    503: '503 - Insurer Service Error',
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

# Database connection helpers
@contextmanager
def get_db_connection(main=False):
    host = DB_HOST_MAIN if main else DB_HOST
    conn = None
    try:
        conn = psycopg2.connect(host=host, database=DB_NAME,
                                 user=DB_USER, password=DB_PASS)
        yield conn
    except Exception as e:
        app.logger.error(f"DB connection failed: {e}")
        abort(500, description="Database connection failed")
    finally:
        if conn:
            conn.close()

@contextmanager
def get_db_cursor(main=False):
    with get_db_connection(main=main) as conn:
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()

@app.route('/')
def home():
    return render_template('index.html')



def reverse_lookup(dictionary, value):
    return next((k for k,v in dictionary.items() if v==value), None)


@app.route('/graph_data/<timestamp>')
@limiter.limit("30/minute")  # Limit this endpoint to 10 requests per minute
def minute_breakdown(timestamp):
    print("minute_breakdown " + timestamp)

    input_string = request.args.get('pq_clientid', '')
    pq_clientid, *server = input_string.split('-')
    server = server[0] if server else 'none'
    ts = datetime.strptime(timestamp.strip(), "%Y-%m-%d %H:%M")
    start_ts, end_ts = ts, ts + timedelta(hours=1)
    sql = ("SELECT date_trunc('minute',lsr.stamp) as minute,lsd.status,COUNT(*) "
           "FROM log_service_requests lsr JOIN log_service_requests_details lsd "
           "ON lsr.srnumber=lsd.srnumber "
           "WHERE lsr.stamp>=%s AND lsr.stamp<%s ")
    params = [start_ts, end_ts]
    if pq_clientid != 'None':
        sql += "AND lsr.pq_clientid=%s ";
        params.append(pq_clientid)
    if server == 'Testing':
        sql += "AND lsr.servername LIKE 'UAT%%' "
    elif server == 'Production':
        sql += "AND lsr.servername LIKE 'PROD%%' "
    sql += "GROUP BY minute,lsd.status"
    return execute_and_cache_query('minute_breakdown', pq_clientid, timestamp,
                                   sql, 'pq_clientid', f"{server} JAWS Requests At: {timestamp}",
                                   'Timestamp', 'Total Number', start_ts, end_ts, tuple(params))


@app.route('/graph_data_client/<pq_clientid>/<server>')
@limiter.limit("30/minute")  # Limit this endpoint to 10 requests per minute
def graph_data(pq_clientid, server):
    print("graph_data")
    print(pq_clientid)

    start_ts, end_ts = datetime.now() - timedelta(hours=24), datetime.now()
    sql = ("SELECT date_trunc('hour',lsr.stamp),lsd.status,COUNT(*) "
           "FROM log_service_requests lsr JOIN log_service_requests_details lsd "
           "ON lsr.srnumber=lsd.srnumber "
           "WHERE lsr.stamp>=NOW()-INTERVAL '24 hours' ")
    params = []
    if pq_clientid != 'None': sql += "AND lsr.pq_clientid=%s "; params.append(pq_clientid)
    if server == 'Testing':
        sql += "AND lsr.servername LIKE 'UAT%%' "
    elif server == 'Production':
        sql += "AND lsr.servername LIKE 'PROD%%' "
    sql += "GROUP BY 1,2"
    return execute_and_cache_query('graph_data', pq_clientid, None,
                                   sql, 'pq_clientid', f"{server} JAWS Requests for last 24 hours",
                                   'Hour', 'Total Requests', start_ts, end_ts, tuple(params))


from datetime import datetime, timedelta


@app.route('/minute_breakdown_details/<timestamp>/<status>/<server>')
@limiter.limit("30/minute")  # Limit this endpoint to 10 requests per minute
def minute_breakdown_details(timestamp, status, server):
    # And for /graph_data/<timestamp>
    # if cache['minute_breakdown_details']['timestamp'] and time.time() - cache['minute_breakdown_details']['timestamp'] < 60:
    #    return cache['minute_breakdown']['data']
    print("minute_breakdown_details " + timestamp + " " + status + " " + server)
    ts = datetime.strptime(timestamp.strip(), "%Y-%m-%d %H:%M")
    start_ts, end_ts = ts, ts + timedelta(minutes=1)
    status_code = reverse_lookup(status_labels, status)
    sql = ("SELECT date_trunc('minute',lsr.stamp) as minute,lsr.srnumber,lsd.status,lsd.insurer,"
           "lsd.responsetime,lsd.responsefile,lsd.requestfile,lsr.requestfile as sr_requestfile,"
           "lsr.responsefile as sr_responsefile,lsr.lob,lsr.servername,lsr.subbrokerid,"
           "lsr.prov,lsr.quotenumber,lsr.pq_clientid,b.brokername,lsr.username "
           "FROM log_service_requests lsr JOIN log_service_requests_details lsd ON lsr.srnumber=lsd.srnumber "
           "JOIN broker b ON lsr.pq_clientid=b.pq_clientid "
           "WHERE lsr.stamp>=%s AND lsr.stamp<%s AND lsd.status=%s ")
    if server == 'Testing':
        sql += "AND lsr.servername LIKE 'UAT%%' "
    elif server == 'Production':
        sql += "AND lsr.servername LIKE 'PROD%%' "
    with get_db_cursor() as cur:
        cur.execute(sql, (start_ts, end_ts, status_code))
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
    return jsonify([dict(zip(cols, r)) for r in rows])

@app.route('/process_file_location', methods=['POST'])
@limiter.limit("30/minute")  # Limit this endpoint to 10 requests per minute
def process_file_location():
    data=request.get_json(); path=data['responsefile_path']
    base='\\sv-hv15.servpoint.net'
    full=base+path.replace('\\sv-hv15','')
    if not full.startswith(base): abort(400)
    return send_file(full,as_attachment=True)


@app.route('/brokers/<server>')
@limiter.limit("30/minute")  # Limit this endpoint to 10 requests per minute
def brokers(server):
    print("brokers " + server)
    sql = ("SELECT DISTINCT b.brokername,b.pq_clientid "
           "FROM broker b JOIN log_service_requests l ON b.pq_clientid=l.pq_clientid "
           "WHERE l.stamp>=NOW()-INTERVAL '24 hours' ")
    if server == 'Testing':
        sql += "AND l.servername LIKE 'UAT%%' "
    elif server == 'Production':
        sql += "AND l.servername LIKE 'PROD%%' "
    sql += "ORDER BY b.brokername"
    with get_db_cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return jsonify(rows)


def execute_and_cache_query(route, pq_clientid, timestamp, sql_query, cache_key, plot_title, xaxis_title, yaxis_title,
                            start_timestamp, end_timestamp, query_params=None):
    # Use cache if fresh
    entry = cache.get(route, {})
    if entry.get(cache_key) == pq_clientid and entry.get('sql_query') == sql_query and time.time() - entry.get(
            'timestamp', 0) < 60:
        return entry['data']

    # Execute query
    try:
        with get_db_cursor() as cur:
            if query_params:
                cur.execute(sql_query, query_params)
            else:
                cur.execute(sql_query)
            results = cur.fetchall()
    except Exception as e:
        app.logger.error(f"Query failed for {route}: {e}")
        abort(500, description="Database query failed")

    # Build data dict
    data_dict = {}
    for ts_val, status, count in results:
        data_dict.setdefault(ts_val, {})[status] = count
    statuses = set(status for _, status, _ in results)

    # Create Plotly figure
    fig = go.Figure([
        go.Bar(
            name=status_labels.get(st, st),
            x=list(data_dict.keys()),
            y=[data_dict.get(ts, {}).get(st, 0) for ts in data_dict],
            marker_color=status_colors.get(st, 'rgb(128,128,128)')
        ) for st in statuses
    ])
    # Configure layout with dynamic ticks
    if (end_timestamp - start_timestamp).total_seconds() > 7200:
        freq = 'H'
        fmt = '%H:00'
    else:
        freq = 'min'
        fmt = '%H:%M'
    ticks = pd.date_range(start_timestamp, end_timestamp, freq=freq)
    fig.update_layout(
        barmode='stack',
        title=plot_title,
        xaxis_title=xaxis_title,
        yaxis_title=yaxis_title,
        xaxis=dict(tickmode='array', tickvals=ticks, ticktext=[t.strftime(fmt) for t in ticks])
    )

    # Cache and return
    cache[route] = {cache_key: pq_clientid, 'sql_query': sql_query, 'timestamp': time.time(),
                    'data': jsonify(fig.to_json())}
    return cache[route]['data']


@app.route('/graph_data_last_hour')
@limiter.limit("30/minute")  # Limit this endpoint to 10 requests per minute
def graph_data_last_hour():
    server = "Production"

    # Check the cache
    entry = cache.get('last_hour_graph_data', {})
    if entry.get('timestamp') and time.time() - entry['timestamp'] < 60:
        return entry['data']

    # SQL query string
    sql_query = (
        "SELECT date_trunc('hour', lsr.stamp) as hour, lsd.insurer, lsd.status, COUNT(*) "
        "FROM log_service_requests AS lsr "
        "JOIN log_service_requests_details AS lsd ON lsr.srnumber = lsd.srnumber "
        "WHERE lsr.stamp >= NOW() - INTERVAL '1 hour' "
        "AND NOT (lsd.responsetime = 0 AND lsd.status = 0 AND lsd.insurer = 'PEA') "
        "GROUP BY hour, lsd.insurer, lsd.status"
    )

    # Optionally filter by server
    if server == 'Testing':
        sql_query = sql_query.replace("WHERE", "WHERE lsr.servername LIKE 'UAT%%' AND ")
    elif server == 'Production':
        sql_query = sql_query.replace("WHERE", "WHERE lsr.servername LIKE 'PROD%%' AND ")

    # Execute query with managed cursor
    try:
        with get_db_cursor() as cur:
            cur.execute(sql_query)
            results = cur.fetchall()
    except Exception as e:
        app.logger.error(f"graph_data_last_hour failed: {e}")
        abort(500, description="Database query failed")

    # Aggregate results
    data = {}
    statuses = set()
    for hour, insurer, status, count in results:
        data.setdefault(insurer, {})[status] = count
        statuses.add(status)

    # Build Plotly figure
    fig = go.Figure([
        go.Bar(
            name=status_labels.get(st, st),
            y=list(data.keys()),
            x=[data[ins].get(st, 0) for ins in data],
            orientation='h',
            marker_color=status_colors.get(st, 'rgb(128,128,128)')
        ) for st in statuses
    ])
    fig.update_layout(
        barmode='stack',
        title='Insurer Requests in the Last Hour',
        xaxis_title='Count',
        yaxis_title='Insurer',
    )

    # Cache and return
    cache['last_hour_graph_data'] = {'timestamp': time.time(), 'data': jsonify(fig.to_json())}
    return cache['last_hour_graph_data']['data']

import json
from datetime import datetime, timedelta


@app.route('/insurer_breakdown/<duration>/<status>/<insurer>/<server>')
@limiter.limit("30/minute")  # Limit this endpoint to 10 requests per minute
def insurer_breakdown(duration, status, insurer, server):
    status_code = reverse_lookup(status_labels, status)
    end_ts = datetime.now()
    start_ts = end_ts - timedelta(hours=int(duration))
    sql = ("SELECT date_trunc('minute',lsr.stamp) as minute,lsr.srnumber,lsd.status,lsd.insurer,"
           "lsd.responsetime,lsd.responsefile,lsd.requestfile,lsr.requestfile as sr_req,"
           "lsr.responsefile as sr_resp,lsr.lob,lsr.servername,lsr.subbrokerid,"
           "lsr.prov,lsr.quotenumber,lsr.pq_clientid,b.brokername,lsr.username "
           "FROM log_service_requests lsr "
           "JOIN log_service_requests_details lsd ON lsr.srnumber=lsd.srnumber "
           "JOIN broker b ON lsr.pq_clientid=b.pq_clientid "
           "WHERE lsr.stamp>=%s AND lsr.stamp<%s AND lsd.insurer=%s AND lsd.status=%s ")
    if server == 'Testing':
        sql += "AND lsr.servername LIKE 'UAT%%' "
    elif server == 'Production':
        sql += "AND lsr.servername LIKE 'PROD%%' "
    with get_db_cursor() as cur:
        cur.execute(sql, (start_ts, end_ts, insurer, status_code))
        cols = [c[0] for c in cur.description]
        data = cur.fetchall()
    return jsonify([dict(zip(cols, r)) for r in data])

@app.route('/broker_quoting_details/<pq_clientid>/<interval>/<server>')
@limiter.limit("30/minute")  # Limit this endpoint to 10 requests per minute
def broker_quoting_details(pq_clientid, interval, server):
    print("broker_quoting_details: " + pq_clientid + " " + interval + " " + server)

    start_ts, end_ts = datetime.now() - timedelta(hours=24), datetime.now()
    sql = ("SELECT date_trunc('hour',lsr.stamp),lsd.status,COUNT(*) "
           "FROM log_service_requests lsr JOIN log_service_requests_details lsd "
           "ON lsr.srnumber=lsd.srnumber "
           "WHERE lsr.stamp>=NOW()-INTERVAL '24 hours' ")
    params = []
    if pq_clientid != 'None': sql += "AND lsr.pq_clientid=%s "; params.append(pq_clientid)
    if server == 'Testing':
        sql += "AND lsr.servername LIKE 'UAT%%' "
    elif server == 'Production':
        sql += "AND lsr.servername LIKE 'PROD%%' "
    sql += "GROUP BY 1,2"
    return execute_and_cache_query('graph_data', pq_clientid, None,
                                   sql, 'pq_clientid', f"{server} JAWS Requests for last 24 hours",
                                   'Hour', 'Total Requests', start_ts, end_ts, tuple(params))

@app.route('/current-brokers')
@limiter.limit("30/minute")  # Limit this endpoint to 10 requests per minute
def currentBrokers():
    sql = ("SELECT DISTINCT brokername,pq_clientid FROM broker "
           "WHERE expiry>=NOW()-INTERVAL '1 month' AND (product_id LIKE '%RC%' OR product_id LIKE '%PQ%' OR product_id LIKE '%RQ%') "
           "ORDER BY brokername")
    with get_db_cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return jsonify(rows)

@app.route('/broker-quick-stats/<broker_pq_clientid>')
@limiter.limit("30/minute")  # Limit this endpoint to 30 requests per minute
def brokerQuickStats(broker_pq_clientid):
    print("brokerQuickStats: " + broker_pq_clientid)
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT hotfix_rid FROM hotfix WHERE sw_type='PQ' ORDER BY date_created DESC LIMIT 1")
            rid = cur.fetchone()
            if not rid: abort(500, 'No PQ hotfix')
            current = rid[0]
            base_q = (
                "SELECT b.brokername,b.pq_clientid,b.prod_id,b.expiry,si.hotfix_rid AS hotfix_rid,%s AS current_hotfixrid "
                "FROM broker b JOIN software_info si ON b.pq_clientid=si.pq_clientid ")
            params = [current]
            if broker_pq_clientid not in ('None', 'undefined'):
                base_q += "WHERE b.pq_clientid=%s AND b.expiry>=NOW()-INTERVAL '1 month' AND b.product_id LIKE '%%PQ%%' AND b.testing IS FALSE AND b.demo IS FALSE "
                params.append(broker_pq_clientid)
            else:
                base_q += "WHERE b.expiry>=NOW()-INTERVAL '1 month' AND si.hotfix_rid!=%s AND b.product_id LIKE '%%PQ%%' AND b.testing IS FALSE AND b.demo IS FALSE AND b.brokername NOT LIKE '%%PowerSoft%%' "
                params.append(current)
            cur.execute(base_q, params)
            data = cur.fetchall()
        return jsonify(data)
    except Exception as e:
        app.logger.error(f"broker_quick_stats failed: {e}")
        abort(500)


@app.route('/broker-rate-engines/<broker_pq_clientid>')
@limiter.limit("30/minute")  # Limit this endpoint to 10 requests per minute
def brokerRateEngines(broker_pq_clientid):
    print("brokerRateEngines: " + broker_pq_clientid)
    if broker_pq_clientid in ('None','undefined'):
        return brokerQuickStats(broker_pq_clientid)
    sql = ("SELECT re.insurer,re.prov,re.line,re.version AS current_version,cr.version "
           "FROM client_re cr JOIN rateengine re ON cr.rid=re.rid "
           "WHERE cr.pq_clientid=%s ORDER BY re.insurer,re.prov,re.line")
    with get_db_cursor() as cur:
        cur.execute(sql, (broker_pq_clientid,))
        rows = cur.fetchall()
    return jsonify(rows)


@app.route('/broker-hotfix/<broker_pq_clientid>')
@limiter.limit("30/minute")  # Limit this endpoint to 10 requests per minute
def brokerHotfix(broker_pq_clientid):
    print("brokerHotfix: " + broker_pq_clientid)
    if broker_pq_clientid in ('None', 'undefined'):
        return jsonify(None)
    sql = ("SELECT si.sw_type,si.version,si.hotfix_rid AS broker_rid,h.hotfix_rid AS current_rid "
           "FROM software_info si JOIN hotfix h ON si.sw_type=h.sw_type AND si.version=h.sw_version "
           "WHERE si.pq_clientid=%s")
    with get_db_cursor() as cur:
        cur.execute(sql, (broker_pq_clientid,))
        rows = cur.fetchall()
    return jsonify(rows)

@app.route('/broker-quote-history/<broker_pq_clientid>')
@limiter.limit("30/minute")  # Limit this endpoint to 10 requests per minute
def brokerQuoteHistory(broker_pq_clientid):
    print("brokerQuoteHistory: " + broker_pq_clientid)
    sql = ("SELECT pq_clientid,subbrokerid,COUNT(*) FROM log_service_requests "
           "WHERE stamp>=NOW()-INTERVAL '30 days' AND servername LIKE 'PROD%%' AND pq_clientid=%s "
           "GROUP BY pq_clientid,subbrokerid ORDER BY COUNT(*) DESC")
    with get_db_cursor() as cur:
        cur.execute(sql, (broker_pq_clientid,))
        rows = cur.fetchall()
    return jsonify(rows)


@app.route('/top-brokers')
@limiter.limit("30/minute")  # Limit this endpoint to 10 requests per minute
def graph_top_brokers():
    server = "Production"

    # Check the cache, if the data is less than 300 seconds old, use it
    if cache['graph_top_brokers']['timestamp'] and time.time() - cache['graph_top_brokers']['timestamp'] < 300:
        print("Using cached data")
        return cache['graph_top_brokers']['data']

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
    ##cursor.execute(sql_query)

    ##results = cursor.fetchall()

    with get_db_cursor() as cur:
        cur.execute(sql_query)
        results = cur.fetchall()

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

    # Cache the results
    cache['graph_top_brokers']['timestamp'] = time.time()
    cache['graph_top_brokers']['data'] = jsonify(fig.to_json())

    return cache['graph_top_brokers']['data']


@app.route('/broker-detailed-history/<broker_pq_clientid>')
@limiter.limit("30/minute")  # Limit this endpoint to 10 requests per minute
def brokerDetailedHistory(broker_pq_clientid):
    # Fetch user and license counts and status breakdowns in a single managed cursor
    with get_db_cursor() as cur:
        # 1) Count distinct users in the last 30 days
        cur.execute(
            """
            SELECT COUNT(DISTINCT username)
            FROM public.log_service_requests
            WHERE pq_clientid = %s
              AND stamp >= NOW() - INTERVAL '30 days'
            """,
            (broker_pq_clientid,)
        )
        num_users_actual = cur.fetchone()[0]

        # 2) Fetch allowed license count
        cur.execute(
            """
            SELECT numlicense
            FROM public.broker
            WHERE pq_clientid = %s
            """,
            (broker_pq_clientid,)
        )
        num_users_allowed = cur.fetchone()[0]

        # 3) Breakdown of statuses in last 30 days
        cur.execute(
            """
            SELECT details.status, COUNT(*) AS status_count
            FROM public.log_service_requests AS requests
            JOIN public.log_service_requests_details AS details
              ON requests.srnumber = details.srnumber
            WHERE requests.pq_clientid = %s
              AND requests.stamp >= NOW() - INTERVAL '30 days'
            GROUP BY details.status
            """,
            (broker_pq_clientid,)
        )
        breakdown = cur.fetchall()

        # 4) Breakdown of statuses for the same 30‑day window one year ago
        cur.execute(
            """
            SELECT details.status, COUNT(*) AS status_count
            FROM public.log_service_requests AS requests
            JOIN public.log_service_requests_details AS details
              ON requests.srnumber = details.srnumber
            WHERE requests.pq_clientid = %s
              AND requests.stamp >= NOW() - INTERVAL '1 year' - INTERVAL '30 days'
              AND requests.stamp < NOW() - INTERVAL '1 year'
            GROUP BY details.status
            """,
            (broker_pq_clientid,)
        )
        breakdown_last_year = cur.fetchall()

    # Prepare Plotly pie chart of this month's breakdown
    labels = [status_labels[status] for status, _ in breakdown]
    values = [count for _, count in breakdown]
    custom = [status for status, _ in breakdown]
    fig = go.Figure(data=[
        go.Pie(
            labels=labels,
            values=values,
            customdata=custom,
            hovertemplate='Status: %{customdata}<br>Value: %{value}<br>Percent: %{percent}',
            marker=dict(colors=[status_colors[s] for s, _ in breakdown])
        )
    ])
    fig.update_layout(title_text='Last Months Quote Status Breakdown')

    # Return all data and chart JSON
    return jsonify({
        "numUsersActual": num_users_actual,
        "numUsersAllowed": num_users_allowed,
        "quoteStatusBreakdown": breakdown,
        "quoteStatusBreakdownLastYear": breakdown_last_year,
        "pieChart": fig.to_json()
    })

@app.route('/broker-quoting-history/<broker_pq_clientid>')
@limiter.limit("30/minute")  # Limit this endpoint to 10 requests per minute
def brokerQuotingHistory(broker_pq_clientid):

    print("brokerQuoteHistory: " + broker_pq_clientid)
    with get_db_cursor() as cur:
        cur.execute("SELECT date_trunc('month',stamp),COUNT(*) FROM log_service_requests WHERE pq_clientid=%s AND stamp>=NOW()-INTERVAL '1 year' GROUP BY 1 ORDER BY 1",(broker_pq_clientid,))
        data = cur.fetchall()
    months=[r[0].strftime('%b %Y') for r in data]
    counts=[r[1] for r in data]
    fig = go.Figure(go.Bar(x=months,y=counts))
    fig.update_layout(title="Broker Quoting History", xaxis_title="Month", yaxis_title="Quotes")
    return jsonify(fig.to_json())

@app.route('/health-check')
@limiter.limit("30/minute")
def healthCheck():
    messages = []
    try:
        # Use context managers for the database connection and cursor
        with psycopg2.connect(
            host=DB_HOST_MAIN,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        ) as conn:
            with conn.cursor() as cursor:
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
    except Exception as e:
        # Handle the exception if the connection to the database fails
        health_status = "Unable to connect to the SQL server - Critical system issue"
        color = "red"
        # Optionally, you can log the exception message for debugging
        print("Database connection failed:", str(e))

    messages.append({"status": health_status, "color": color})

    # Wrap messages in an object
    return jsonify({"messages": messages})


jawsServers = [
    {'name': '<a href="https://jaws.power-soft.com/balancer">jaws.power-soft.com</a>', 'url': 'https://jaws.power-soft.com/axis2/services/ServiceProvider?wsdl', 'expected_text': 'Grabenwerks Service'},
    {'name': '<a href="http://prodjaws1.servpoint.net/axis2/services/ServiceProvider?wsdl">prodjaws1.servpoint.net</a>', 'url': 'http://prodjaws1.servpoint.net/axis2/services/ServiceProvider?wsdl', 'expected_text': 'Grabenwerks Service'},
    {'name': '<a href="http://prodjaws2.servpoint.net/axis2/services/ServiceProvider?wsdl">prodjaws2.servpoint.net</a>', 'url': 'http://prodjaws2.servpoint.net/axis2/services/ServiceProvider?wsdl', 'expected_text': 'Grabenwerks Service'},
    {'name': '<a href="http://prodjaws3.servpoint.net/axis2/services/ServiceProvider?wsdl">prodjaws3.servpoint.net</a>', 'url': 'http://prodjaws3.servpoint.net/axis2/services/ServiceProvider?wsdl', 'expected_text': 'Grabenwerks Service'},
    {'name': '<a href="https://jawstest.power-soft.com/balancer">jawstest.power-soft.com</a>', 'url': 'https://jaws.power-soft.com/axis2/services/ServiceProvider?wsdl',
     'expected_text': 'Grabenwerks Service'},
    {'name': '<a href="http://uatjaws1.servpoint.net/axis2/services/ServiceProvider?wsdl">uatjaws1.servpoint.net</a>', 'url': 'http://uatjaws1.servpoint.net/axis2/services/ServiceProvider?wsdl',
     'expected_text': 'Grabenwerks Service'},
    {'name': '<a href="http://uatjaws2.servpoint.net/axis2/services/ServiceProvider?wsdl">uatjaws2.servpoint.net</a>', 'url': 'http://uatjaws2.servpoint.net/axis2/services/ServiceProvider?wsdl',
     'expected_text': 'Grabenwerks Service'},
    {'name': '<a href="http://devjaws1.powersoft.net/axis2/services/ServiceProvider?wsdl">devjaws1.powersoft.net</a>', 'url': 'http://devjaws1.powersoft.net/axis2/services/ServiceProvider?wsdl',
     'expected_text': 'Grabenwerks Service'},
    # Add more servers as needed
]

rateworksServers = [
    {'name': '<a href="http://prodraterspool.servpoint.net/balancer">prodraterspool.servpoint.net</a>', 'url': 'http://prodraterspool.servpoint.net/rateworks.wsdl', 'expected_text': 'RateWorks'},
    {'name': '<a href="http://prodrater1.servpoint.net/rateworks.wsdl">prodrater1.servpoint.net</a>', 'url': 'http://prodrater1.servpoint.net/rateworks.wsdl', 'expected_text': 'RateWorks'},
    {'name': '<a href="http://prodrater2.servpoint.net/rateworks.wsdl">prodrater2.servpoint.net</a>', 'url': 'http://prodrater2.servpoint.net/rateworks.wsdl', 'expected_text': 'RateWorks'},
    {'name': '<a href="http://prodrater3.servpoint.net/rateworks.wsdl">prodrater3.servpoint.net</a>', 'url': 'http://prodrater3.servpoint.net/rateworks.wsdl', 'expected_text': 'RateWorks'},
    {'name': '<a href="http://prodrater4.servpoint.net/rateworks.wsdl">prodrater4.servpoint.net</a>', 'url': 'http://prodrater4.servpoint.net/rateworks.wsdl', 'expected_text': 'RateWorks'},
    {'name': '<a href="http://prodrater5.servpoint.net/rateworks.wsdl">prodrater5.servpoint.net</a>', 'url': 'http://prodrater5.servpoint.net/rateworks.wsdl', 'expected_text': 'RateWorks'},
    {'name': '<a href="http://uatraterspool.servpoint.net/balancer">uatraterspool.servpoint.net</a>', 'url': 'http://uatraterspool.servpoint.net/rateworks.wsdl', 'expected_text': 'RateWorks'},
    {'name': '<a href="http://uatrater1.servpoint.net/rateworks.wsdl">uatrater1.servpoint.net</a>', 'url': 'http://uatrater1.servpoint.net/rateworks.wsdl', 'expected_text': 'RateWorks'},
    {'name': '<a href="http://uatrater2.servpoint.net/rateworks.wsdl">uatrater2.servpoint.net</a>', 'url': 'http://uatrater2.servpoint.net/rateworks.wsdl', 'expected_text': 'RateWorks'},
    {'name': '<a href="http://qarater1.servpoint.net/rateworks.wsdl">qarater1.servpoint.net</a>', 'url': 'http://qarater1.servpoint.net/rateworks.wsdl', 'expected_text': 'RateWorks'},
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
@limiter.limit("2/minute")
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

import json
from datetime import timedelta
import plotly.graph_objects as go

# at the top of your file, your legend and color maps
quote_status_colors = {
    'S': 'rgb(100, 200, 100)',
    'E': 'red',
    'F': 'pink',
    'R': 'purple',
    'N': 'lightblue',
    'FW': 'black'
}
default_quote_color = 'grey'
legend_map = {
    'S':  'Success',
    'E':  'Internal error',
    'F':  'Request failure',
    'R':  'RateEngine error',
    'N':  'No Rates / Missing',
    'FW': 'Unable to save'
}

@app.route('/quote-log-last-hour')
@limiter.limit("30 per minute")
def quote_log_last_hour():
    try:
        # 1) Group by type + service mode ("P" vs other) + status_code
        sql = """
            SELECT
              TRIM(type) AS typ,
              CASE WHEN servicemode = 'P' THEN 'Prod' ELSE 'Test' END AS sm,
              TRIM(status_code) AS st,
              COUNT(*) AS cnt
            FROM public.quote_log
            WHERE stamp >= NOW() - INTERVAL '1 hour'
              AND TRIM(type) IN ('RC','CQ')
            GROUP BY typ, sm, st
            ORDER BY typ, sm, st;
        """
        with get_db_cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            # rows like: [('CQ','P','S',42), ('CQ','other','S',49), ...]

        # 2) Pivot into a dict keyed by (type,service_mode)
        keys = [('RC','Prod'), ('RC','Test'), ('CQ','Prod'), ('CQ','Test')]
        y_labels = [f"{typ} {sm}" for typ, sm in keys]
        data = {k: {} for k in keys}
        statuses = set()
        for typ, sm, st, cnt in rows:
            data[(typ,sm)][st] = cnt
            statuses.add(st)

        # 3) Build a horizontal stacked bar with four bars
        fig = go.Figure()
        for st in sorted(statuses):
            fig.add_trace(go.Bar(
                y = y_labels,
                x = [data[k].get(st, 0) for k in keys],
                name = legend_map.get(st, st),
                orientation='h',
                marker_color=quote_status_colors.get(st, default_quote_color)
            ))

        fig.update_layout(
            barmode='stack',
            title='Quote Log Activity (Last Hour) by Service Mode',
            xaxis_title='Count',
            yaxis_title='Type + Service Mode',
            margin=dict(l=80, r=20, t=50, b=50),
            height=300  # adjust as you like
        )

        # 4) Return the figure JSON
        return jsonify(json.loads(fig.to_json()))

    except Exception:
        app.logger.exception("Error in quote-log-last-hour")
        return jsonify({ 'error': 'Failed to build quote-log graph' }), 500



if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
