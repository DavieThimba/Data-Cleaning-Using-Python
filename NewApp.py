from flask import Flask, render_template, request, redirect, make_response, jsonify, send_file
from datetime import datetime, timedelta
import mysql.connector
import pandas as pd
import requests
import json
import time
import io
import re
import os

app = Flask(__name__)

directory = r"C:\Users\THIMBA\Desktop\Vynamic files"

def get_db_connection():
    db_connection = {
        'host': '127.0.0.1',
        'user': 'root',
        'password': 'Daviethimba@1',
        'database': 'recon'
    }
    return mysql.connector.connect(**db_connection)


@app.route('/')
def index():
    return render_template('index2.html')

@app.context_processor
def inject_totals():
    try:
        response = requests.get('http://localhost:5000/get_totals')
        totals = response.json()
        print(totals)  
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        totals = None
    return dict(totals=totals)

@app.route('/download_matched', methods=['GET'])
def download_matched():
    user_date = request.args.get('date')
    db_connection = get_db_connection()
    cursor = db_connection.cursor()

    _sql_query = """
    SELECT EOD_DATE, Terminal, FORACID, SOL_ID, Device, ACCT_SHORT_NAME, Cash_Counters_Balance, VALUE_DATE_BAL, RCRE_TIME, Variance, RETRACTS, REMARKS FROM archive 
    WHERE EOD_DATE = %s and merge = 'both'
    """

    cursor.execute(_sql_query)
    matched = cursor.fetchall()
    matched = pd.DataFrame(matched)
    print(matched)
    csv_buffer = io.StringIO()
    # matched = matched.rename(columns={
    #     'SOL_ID': 'solId',
    #     'Device': 'device',
    #     'Terminal': 'terminal',
    #     'Cash Counters Balance': 'cashCountersBalance',
    #     'FORACID': 'foracid',
    #     'EOD_DATE': 'EODDate',
    #     'RCRE_TIME': 'RCRETime',
    #     'VALUE_DATE_BAL': 'valueDateBal',
    #     'Variance': 'variance',
    #     'RETRACTS': 'retracts',
    #     'REMARKS': 'remarks'
    # })

    matched.to_csv(csv_buffer, index=False)
    response = make_response(csv_buffer.getvalue())
    response.headers['Content-Disposition'] = 'attachment; filename=Report.csv'
    response.headers['Content-Type'] = 'text/csv'
    return response

# Get the exceptions from the db
@app.route('/exceptions', methods=['GET'])
def fetch_exceptions_data_from_archive(*args):
    user_date = request.args.get('date')
    db_connection = get_db_connection()
    cursor = db_connection.cursor()
    if not user_date:
        return jsonify({'error': 'Date parameter is required.'}), 400

    _SQL_SELECT_DATE = "SELECT EOD_DATE, Terminal, FORACID, SOL_ID, Device, ACCT_SHORT_NAME, Cash_Counters_Balance, VALUE_DATE_BAL, RCRE_TIME, Variance, RETRACTS, REMARKS FROM archive WHERE EOD_DATE != %s and merge != 'both'"

    max_retries = 3 
    retry_delay = 5  
    retries = 0

    while retries < max_retries:
        cursor.execute(_SQL_SELECT_DATE, (user_date,))
        result = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        exceptions = [dict(zip(columns, row)) for row in result]

        if len(exceptions) > 0:
            renamed_columns = {
                'SOL_ID': 'solId',
                'Device': 'device',
                'Terminal': 'terminal',
                'Cash_Counters_Balance': 'cashCountersBalance',
                'FORACID': 'foracid',
                'EOD_DATE': 'EODDate',
                'RCRE_TIME': 'RCRETime',
                'VALUE_DATE_BAL': 'valueDateBalance',
                'Variance': 'variance',
                'RETRACTS': 'retracts',
                'REMARKS': 'remarks'
            }
            exceptions = [{renamed_columns.get(k, k): v for k, v in row.items()} for row in exceptions]
            cursor.close()
            db_connection.close()
            return jsonify(exceptions)

        if retries < max_retries - 1:
            time.sleep(retry_delay)
            select_files_by_date(directory, user_date)

    return jsonify({'error': 'Data not found.'}), 404

@app.route('/download_exceptions',methods=['GET'])
def download_exceptions():
    user_date = request.args.get('date')
    db_connection = get_db_connection()
    cursor = db_connection.cursor()
 
    _sql_query = """
        SELECT EOD_DATE, Terminal, FORACID, SOL_ID, Device, ACCT_SHORT_NAME, Cash_Counters_Balance, VALUE_DATE_BAL, RCRE_TIME, Variance, RETRACTS, REMARKS FROM archive 
        WHERE date_format(EOD_DATE, "yyyy-MM-dd") = '{}' and merge != 'both'
        """.format(user_date)

    cursor.execute(_sql_query)
    exceptions = cursor.fetchall()
    cursor.close()
    db_connection.close()
    excel_file = io.BytesIO()
    exceptions = pd.DataFrame(exceptions)
    columns = [column[0] for column in cursor.description]
    exceptions = [dict(zip(columns, row)) for row in exceptions]
    exceptions = exceptions[['SOL_ID', 'Device', 'Terminal', 'Cash Counters Balance', 'FORACID', 'EOD_DATE', 'RCRE_TIME', 'VALUE_DATE_BAL', 'Variance', 'RETRACTS', 'REMARKS']]
    exceptions.to_excel(excel_file, index=False, sheet_name='Exceptions')
    excel_file.seek(0) 
    response = make_response(excel_file.getvalue())
    response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    response.headers['Content-Disposition'] = 'attachment; filename=exceptions.xlsx'
    return response

# get the files from the directory
def get_matching_files(directory, pattern):
    file_list = []
    for root, dirs, files in os.walk(directory):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            file_list.append(file_path)

    matching_files = []
    for file_path in file_list:
        match = re.search(pattern, file_path)
        if match:
            matching_files.append(file_path)
    return matching_files


# retracts_pattern = r"Result_\d*\.csv"
@app.route('/select_data', methods=['GET'])
def select_files_by_date(*args):
    atm_pattern = r".*\\ATMS.*\.xlsx"
    df_pattern = r".*\\.*CashStateReport\.xls$"

    atm_files = get_matching_files(directory, atm_pattern)
    df_files = get_matching_files(directory, df_pattern)

    # atm_date_pattern = r"\d{1,2}[-_]\w+[-_]\d{4}"

    selected_date = datetime.strptime(request.args.get('date'), "%Y-%m-%d")

    atm_file = list(filter(lambda file: file.replace('-','').replace('_','').find(selected_date.strftime("%d%b%Y")), atm_files))
    df_file =  list(filter(lambda file: file.find(selected_date.strftime("%Y%m%d")), df_files))

    if (len(atm_file) != 0 and len(df_file) != 0):
       matched, exceptions, variance =  CashReportProcessor(df_file=df_file[0], atm_file=atm_file[0])
       return {
           "matched" : matched.values.tolist(),
           "exceptions": exceptions.values.tolist(),
           "variance": variance.values.tolist()
       }
    else:
        print("I did not find any file")

def CashReportProcessor(df_file, atm_file):
    # atm_file, df_file = select_files_by_date(directory)
    df = pd.read_excel(df_file)
    atm_data = pd.read_excel(atm_file)
    
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, header=True)
    df.columns = df.columns.str.strip()
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    df = df[df['Unnamed: 0'] != 'Currency: ''']
    current_date = atm_data['EOD_DATE'][0] or df['Unnamed: 25'][0]
    currency1 = "Currency: 'KES'"
    currency2 = "Currency: 'TZS'"
    currency3 = "Currency: 'UGX'"
    currency4 = "Currency: ''"
    currency5 = "Currency: 'USD'"
    currency6 = "Currency: 'LAK'"
    df = df[df['Unnamed: 0'] != currency1]
    df = df[df['Unnamed: 0'] != currency2]
    df = df[df['Unnamed: 0'] != currency3]
    df = df[df['Unnamed: 0'] != currency4]
    df = df[df['Unnamed: 0'] != currency5]
    df = df[df['Unnamed: 0'] != currency6]
    df = df[df['Unnamed: 0'] != 'Date :  ']
    df = df[df['Unnamed: 25'] != 'Date :  ']
    df = df[df['Unnamed: 8'] != 'Number Of Devices: ']
    df = df[df['Unnamed: 0'] != 'Cash amount report for the network']
    df = df[df['Unnamed: 4'] != 'Container Name']
    df = df[df['Unnamed: 12'] != 'Denomination']
    df = df[df['Unnamed: 14'] != 'Stock']
    df = df[df['Unnamed: 20'] != 'Turnover']
    df = df[df['Unnamed: 22'] != 'Dispensed Items']
    df = df[df['Unnamed: 24'] != 'Amount']
    df = df[df['Unnamed: 25'] != 'current_date']
    df = df[df['Unnamed: 27'] != 'Out-Of-Stock Forcast']
    pattern = r'^.*Number Of Devices: .*$'
    rows_to_drop1 = df['Unnamed: 0'].astype(str).str.contains(pattern)
    df = df[~rows_to_drop1]
    rows_to_drop2 = df['Unnamed: 0'].astype(str).str.contains('Currency:')
    df = df[~rows_to_drop2]
    rows_to_drop3 = df['Unnamed: 0'].astype(str).str.contains('Date :')
    df = df[~rows_to_drop3]

    df = df[['Unnamed: 0','Unnamed: 2','Unnamed: 4','Unnamed: 7','Unnamed: 13','Unnamed: 19']]
    df = df.dropna(how='all',axis=1)
    df = df.dropna(how='all',axis=0)
    df = df.drop(df.index[[2,3]]) 

    rows_to_drop = []
    for index, row in df.iterrows():
        if re.match(pattern, str(row)):
            rows_to_drop.append(index)
    df.drop(rows_to_drop)
    print(rows_to_drop)

    Devices = []
    Terminals = []

    skip_next_row = False

    for index, row in df.iterrows():
        if skip_next_row:
            skip_next_row = False
            continue

        device = row['Unnamed: 0']
        terminal = row['Unnamed: 2']

        if str(device) != 'nan' and (device, terminal) not in zip(Devices, Terminals):
            if str(terminal) != 'nan' and re.match(r'^EBL.*', str(terminal)):
                Devices.append(device)
                Terminals.append(terminal)
                devices_with = list(zip(Devices, Terminals))

    check = df['Unnamed: 0'].isna()
    Totals = df.loc[check, 'Unnamed: 13']
    Totals = [amount for amount in Totals if str(amount) != 'nan']

    df.reset_index(drop=True, inplace=True)

    Terminals = []
    visited_terminals = set()

    for i, val in enumerate(check):
        if val:
            terminal = df.loc[i-1, 'Unnamed: 2']  # Retrieve terminal from the row above
            if str(terminal) != 'nan' and terminal not in visited_terminals:
                Terminals.append(terminal)
                visited_terminals.add(terminal)

    result = list(zip(Terminals, Totals))

    devices_with_df = pd.DataFrame(devices_with, columns=['Devices', 'Terminal'])
    result_df = pd.DataFrame(result, columns=['Terminal', 'Totals'])

    df = pd.merge(devices_with_df, result_df, on='Terminal', how='left')
    df.rename(columns={'Devices': 'Device', 'Terminals': 'Terminal', 'Totals': 'Cash Counters Balance'}, inplace=True)

    merged = pd.merge(df,atm_data, left_on='Terminal',right_on='ACCT_SHORT_NAME',how='outer',indicator = True)
    merged['Variance'] = merged['Cash Counters Balance'] - abs(merged['VALUE_DATE_BAL'])

    merged['VALUE_DATE_BAL'] = abs(merged['VALUE_DATE_BAL'])

    # merged.loc[merged['Terminal'].isin(retracts_data.iloc[:, 0]), 'RETRACTS'] = retracts_data.iloc[:, 1]

    merged['EOD_DATE'] = pd.to_datetime(merged['EOD_DATE'], format='%d-%b-%y')
    merged['RCRE_TIME'] = pd.to_datetime(merged['RCRE_TIME'], format='%d-%b-%y %H:%M:%S')

    merged['Variance'] = merged['Cash Counters Balance'] - merged['VALUE_DATE_BAL']
    merged['Variance'] = abs(merged['Variance'])
    merged['RETRACTS'] = [None] * len(merged)
    merged['REMARKS'] = [None] * len(merged)

    matched = merged[(merged['_merge'] == 'both') & (merged['EOD_DATE'] == current_date)]
    matched = matched[['SOL_ID','Device','Cash Counters Balance','Terminal','FORACID','EOD_DATE','RCRE_TIME','VALUE_DATE_BAL','Variance','RETRACTS','REMARKS']]

    unmatched = merged[(merged['_merge'] != 'both') & (merged['EOD_DATE'] != current_date)]
    exceptions = unmatched[['SOL_ID', 'Device','Terminal', 'FORACID', 'ACCT_SHORT_NAME','Cash Counters Balance','VALUE_DATE_BAL','EOD_DATE','RCRE_TIME','Variance','RETRACTS','REMARKS']]

    # data in finacle but not in vynamic
    variance = merged[(merged['_merge'] != 'both') & (merged['EOD_DATE'] == current_date)]
    variance = variance[['SOL_ID', 'Device','Terminal', 'FORACID', 'ACCT_SHORT_NAME','Cash Counters Balance','VALUE_DATE_BAL','EOD_DATE','RCRE_TIME','Variance','RETRACTS','REMARKS']]
    variance.to_csv("Variances.csv",index=False)
    
    db_connection = get_db_connection()

    create_table_query = '''
        CREATE TABLE IF NOT EXISTS cashStateReportTable (
            FORACID VARCHAR(13) PRIMARY KEY,
            SOL_ID INT,
            Terminal varchar(10),
            Device TEXT,
            ACCT_SHORT_NAME VARCHAR(10),
            Cash_Counters_Balance DOUBLE,
            VALUE_DATE_BAL DOUBLE,
            EOD_DATE DATETIME,
            RCRE_TIME DATETIME,
            Variance DOUBLE,
            RETRACTS DOUBLE,
            REMARKS VARCHAR(150),
            merge VARCHAR(20)
        );
    '''
    with db_connection.cursor() as cursor:
        cursor.execute(create_table_query)
    db_connection.commit()

    create_table_query = '''
        CREATE TABLE IF NOT EXISTS archive (
            ID INT AUTO_INCREMENT PRIMARY KEY,
            EOD_DATE DATETIME,
            FORACID VARCHAR(13),
            SOL_ID INT,
            Terminal varchar(10),
            Device TEXT,
            ACCT_SHORT_NAME VARCHAR(10),
            Cash_Counters_Balance DOUBLE,
            VALUE_DATE_BAL DOUBLE,
            RCRE_TIME DATETIME,
            Variance DOUBLE,
            RETRACTS DOUBLE,
            REMARKS VARCHAR(150),
            merge VARCHAR(20)
        );
    '''
    with db_connection.cursor() as cursor:
        cursor.execute(create_table_query)
    db_connection.commit()

    _SQL_add_data = "INSERT INTO archive (Device, SOL_ID,Terminal, FORACID, ACCT_SHORT_NAME,Cash_Counters_Balance, VALUE_DATE_BAL, EOD_DATE, RCRE_TIME, Variance, merge, RETRACTS, REMARKS) VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    with db_connection.cursor() as move_to_archive:
        for index,row in merged.iterrows():
                data = (row['Device'], row['SOL_ID'],row['Terminal'], row['FORACID'], row['ACCT_SHORT_NAME'],
                        row['Cash Counters Balance'], row['VALUE_DATE_BAL'], row['EOD_DATE'],
                        row['RCRE_TIME'], row['Variance'], row['_merge'], row['RETRACTS'], row['REMARKS'])
                try:
                    move_to_archive.execute(_SQL_add_data,data)
                except (mysql.connector.errors.IntegrityError, mysql.connector.errors.ProgrammingError) as e:
                    if isinstance(e, mysql.connector.errors.IntegrityError):
                        if e.errno == 1062:
                            # print(f"Duplicate entry for FORACID: {row['FORACID']}")
                            ...
                    elif isinstance(e, mysql.connector.errors.ProgrammingError):
                        if e.errno == 1054 and "Unknown column" in e.msg and "'nan'" in e.msg:
                            # print(f"Error: Unknown column encountered for value 'nan' in FORACID: {row['FORACID']}")
                            ...
                    else:
                        print("Error:", e.msg)
        print("Successfully archived all data.")
        db_connection.commit()           

    _SQL_cashStateReport = "INSERT INTO cashStateReportTable (Device, SOL_ID,Terminal, FORACID, ACCT_SHORT_NAME, Cash_Counters_Balance, VALUE_DATE_BAL, EOD_DATE, RCRE_TIME, Variance, merge,RETRACTS,REMARKS) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s)"
    with db_connection.cursor() as cursor_cashStateReport:
        for index, row in merged.iterrows():
            data = (
                row['Device'], row['SOL_ID'], row['Terminal'], row['FORACID'], row['ACCT_SHORT_NAME'],
                row['Cash Counters Balance'], row['VALUE_DATE_BAL'], row['EOD_DATE'],
                row['RCRE_TIME'], row['Variance'], row['_merge'], row['RETRACTS'], row['REMARKS']
            )
            try:
                cursor_cashStateReport.execute(_SQL_cashStateReport, data)
            except (mysql.connector.errors.IntegrityError, mysql.connector.errors.ProgrammingError) as e:
                if isinstance(e, mysql.connector.errors.IntegrityError):
                    if e.errno == 1062:
                        ...
                        # print(f"Duplicate entry for FORACID: {row['FORACID']}")
                elif isinstance(e, mysql.connector.errors.ProgrammingError):
                    if e.errno == 1054 and "Unknown column" in e.msg and "'nan'" in e.msg:
                        # print(f"Error: Unknown column encountered for value 'nan' in FORACID: {row['FORACID']}")
                        ...
                else:
                    print("Error:", e.msg)
        print("Successfully added data to table.")
        db_connection.commit()

    db_connection.close()
    cursor.close()
    return matched, exceptions, variance

@app.route('/get_totals',methods=['GET'])
def get_totals():
    matched, exceptions, merged = CashReportProcessor()
    matched_count = len(matched)
    exceptions_count = len(exceptions)
    merged_count = len(merged)
    totals = {
            'matched_count': matched_count,
            'exceptions_count': exceptions_count,
            'merged_count': merged_count
        }
    return jsonify(**totals)

@app.route('/totals', methods=['GET'])
def display_totals():
    try:
        response = requests.get('http://localhost:5000/get_totals') 
        totals = response.json()  
        print(totals)
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        totals = None 
    return render_template('totals.html', totals=totals)

if (__name__) == '__main__':
    app.run(debug=True)