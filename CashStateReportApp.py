from flask import Flask, render_template, request,redirect,make_response, jsonify, send_file
from datetime import datetime, timedelta
import mysql.connector
import pandas as pd
import json
import requests
import datetime
import io
import re
import os
import re


app = Flask(__name__)

app.config['DATA_LOCATION'] = 'C:/Users/user/Downloads'

def get_db_connection():
    db_connection = {
        'host': '127.0.0.1',
        'user': 'root',
        'password': '',
        'database': ''
    }
    return mysql.connector.connect(**db_connection)

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

@app.route('/')
def index():
    return render_template('index2.html')

from datetime import datetime, timedelta

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

# @app.route('/select_files')
def select_files_by_date(directory):
    atm_pattern = r".*\\file.*\.xlsx"
    df_pattern = r".*\\.*file2\.xls$"

    atm_files = get_matching_files(directory, atm_pattern)
    df_files = get_matching_files(directory, df_pattern)

    atm_date_pattern = r"\d{1,2}[-_]\w+[-_]\d{4}"
    atm_dates = []

    for file_path in atm_files:
        match = re.search(atm_date_pattern, file_path)
        if match:
            date = match.group()
            date = date.replace('-', '').replace('_', '')  
            atm_dates.append(date)

    df_date_pattern = r"\d{8}"
    df_dates = []
    for file_path in df_files:
        match = re.search(df_date_pattern, file_path)
        if match:
            date = match.group()
            df_dates.append(date)

    user_date = request.args.get('date').upper()
    # user_date = request.form['date']
    selected_date = datetime.strptime(user_date, "%d-%b-%Y")

    matching_index = None
    for index, date in enumerate(atm_dates):
        atm_date = datetime.strptime(date, "%d%b%Y")

        if selected_date.strftime("%d-%b-%Y").upper().replace('-', '') == atm_date.strftime("%d%b%Y").upper():
            matching_index = index
            break

    if matching_index is not None:
        atm_date = datetime.strptime(atm_dates[matching_index], "%d%b%Y")
        df_date = None
        for days_offset in range(-2, 3):
            next_date = atm_date + timedelta(days=days_offset)
            next_date_str = next_date.strftime("%Y%m%d")

            if next_date_str in df_dates:
                df_date = next_date_str
                break

        if df_date is not None:
            selected_atm_file = atm_files[matching_index]
            selected_df_file = None
            for file in df_files:
                if df_date in file:
                    selected_df_file = file
                    break

            if selected_df_file is not None:
                return selected_atm_file, selected_df_file
            else:
                return None, "DF file for the selected ATM date not found."
        else:
            return None, "No DF file found within the two-day range."
    else:
        return "ATM file for the selected date not found.", None

atm_file, df_file = select_files_by_date(directory)

df = pd.read_excel(df_file)
atm_data = pd.read_excel(atm_file)

@app.route('/process', methods=['POST'])
def CashReportProcessor():
    global atm_data, df

    df
    atm_data
    
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
    df = df[df['Unnamed: 0'] != 'Cash amount report for the network']
    df = df[df['Unnamed: 4'] != 'Container Name']
    df = df[df['Unnamed: 12'] != 'Denomination']
    df = df[df['Unnamed: 14'] != 'Stock']
    df = df[df['Unnamed: 20'] != 'Turnover']
    df = df[df['Unnamed: 22'] != 'Dispensed Items']
    df = df[df['Unnamed: 24'] != 'Amount']
    df = df[df['Unnamed: 25'] != 'current_date']
    df = df[df['Unnamed: 27'] != 'Out-Of-Stock Forcast']

    df = df[['Unnamed: 0','Unnamed: 2','Unnamed: 4','Unnamed: 7','Unnamed: 13','Unnamed: 19']]
    df = df.dropna(how='all',axis=1)
    df = df.dropna(how='all',axis=0)
    df = df.drop(df.index[[2,3]]) 

    pattern = r'^.*Number Of Devices: .*$'
    rows_to_drop = []
    for index, row in df.iterrows():
        if re.match(pattern, str(row)):
            rows_to_drop.append(index)
    df.drop(rows_to_drop)
    print(rows_to_drop)

    Devices = []
    for i in df['Unnamed: 0']:
        if i in Devices:
            pass
        else:
            Devices.append(i)
    Devices = [branch for branch in Devices if str(branch) != 'nan' and pattern not in str(branch)]

    Terminal = []
    for i in df['Unnamed: 2']:
        if i in Terminal:
            pass
        else:
            Terminal.append(i)
    Terminal = [i for i in Terminal if str(i) != 'nan' and re.match(r'^EBL.*', i)]

    check = df['Unnamed: 0'].isna()
    Totals = df.loc[check,'Unnamed: 13']
    Drawings = df.loc[check,'Unnamed: 19']
    Totals = [amount for amount in Totals if str(amount) != 'nan']
    Drawings = [i for i in Drawings if str(i) != 'nan']

    df = pd.DataFrame(list(zip(Devices,Totals,Drawings,Terminal)), columns=['Device','Cash Counters Balance','Drawings','Terminal']) 
    mask = df['Device'].astype(str).str.contains(pattern)
    df = df[~mask]
    df.reset_index(drop=True)

    merged = pd.merge(df,atm_data, left_on='Terminal',right_on='ACCT_SHORT_NAME',how='outer',indicator = True)
    merged['Variance'] = merged['Cash Counters Balance'] - abs(merged['VALUE_DATE_BAL'])

    merged['EOD_DATE'] = pd.to_datetime(merged['EOD_DATE'], format='%d-%b-%y')
    merged['RCRE_TIME'] = pd.to_datetime(merged['RCRE_TIME'], format='%d-%b-%y %H:%M:%S')

    merged['Variance'] = merged['Cash Counters Balance'] - abs(merged['VALUE_DATE_BAL'])
    merged['Variance'] = abs(merged['Variance'])
    merged['RETRACTS'] = [None] * len(merged)
    merged['REMARKS'] = [None] * len(merged)

    pattern = r'^Date :'
    temp_column = merged.iloc[:, 0].astype(str).where(~df.iloc[:, 0].astype(str).str.match(pattern), '').shift(-1)
    merged.iloc[:, 0] = temp_column
    merged.reset_index(drop=True, inplace=True)

    matched = merged[(merged['_merge'] == 'both') & (merged['EOD_DATE'] == current_date)]
    matched = matched[['SOL_ID','Device','Cash Counters Balance','Terminal','FORACID','EOD_DATE','RCRE_TIME','VALUE_DATE_BAL','Variance','_merge','RETRACTS','REMARKS']]

    unmatched = merged[(merged['_merge'] != 'both') & (merged['EOD_DATE'] != current_date)]
    exceptions = unmatched[['SOL_ID', 'Device','Terminal', 'FORACID', 'ACCT_SHORT_NAME','Cash Counters Balance','VALUE_DATE_BAL','EOD_DATE','RCRE_TIME','Variance','_merge','RETRACTS','REMARKS']]
    
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

    # _SQL_delete_from_cashStateReportTable = "DELETE FROM cashStateReportTable"
    # with db_connection.cursor() as cursor:
    #     cursor.execute(_SQL_delete_from_cashStateReportTable)
    # print("Successfully deleted from the table.")
    # db_connection.commit() 

    db_connection.close()
    cursor.close()
    return matched, exceptions, merged
processor = CashReportProcessor()

@app.route('/summary', methods=['GET'])
def get_matched():
    selected_date = request.args.get('date')
    if selected_date:
        matched = fetch_data_from_archive(selected_date)
        if not matched.empty:
            matched = matched[matched['EOD_DATE'] == selected_date]
    else:
        matched = processor[0] 
    if not matched.empty:
        matched = matched[['SOL_ID', 'Device','Terminal', 'Cash Counters Balance', 'FORACID', 'EOD_DATE', 'RCRE_TIME',
                           'VALUE_DATE_BAL', 'Variance', 'RETRACTS', 'REMARKS']]
        matched = matched.rename(columns={
            'SOL_ID': 'solId',
            'Device': 'device',
            'Terminal': 'terminal',
            'Cash Counters Balance': 'cashCountersBalance',
            'FORACID': 'foracid',
            'EOD_DATE': 'EODDate',
            'RCRE_TIME': 'RCRETime',
            'VALUE_DATE_BAL': 'valueDateBalance',
            'Variance': 'variance',
            'RETRACTS': 'retracts',
            'REMARKS': 'remarks'
        })
        matched_json = matched.to_json(orient='records')
    else:
        matched_json = "[]"
    
    paginated_data = json.loads(matched_json)
    return paginated_data

def get_exceptions():
    exceptions = processor[1]
    current_date = atm_data['EOD_DATE'][0] or df['Unnamed: 25'][0]
    exceptions = exceptions[(exceptions['_merge'] != 'both')]
    exceptions = exceptions[['SOL_ID','Device','Terminal','Cash Counters Balance','FORACID','EOD_DATE','RCRE_TIME','VALUE_DATE_BAL','Variance','RETRACTS','REMARKS']]
    exceptions = exceptions.rename(columns={
        'SOL_ID':'solId',
        'Device':'device',
        'Terminal': 'terminal',
        'Cash Counters Balance': 'cashCountersBalance',
        'FORACID':'foracid',
        'EOD_DATE':'EODDate',
        'RCRE_TIME':'RCRETime',
        'VALUE_DATE_BAL':'valueDateBalance',
        'Variance': 'variance',
        'RETRACTS': 'retracts',
        'REMARKS': 'remarks'
    })
    exceptions_json = exceptions.to_json(orient='records')
    paginated_data = json.loads(exceptions_json)
    return paginated_data

def get_other_exceptions():
    current_date = atm_data['EOD_DATE'][0] or df['Unnamed: 25'][0]
    exceptions = processor[1]
    other_exceptions = exceptions[(exceptions['_merge'] != 'both') & (exceptions['EOD_DATE'] != current_date)]
    if not other_exceptions.empty:
        other_exceptions_json = other_exceptions.to_json(orient='records')
        return other_exceptions_json
    else:
        message = "No exceptions found."
        return redirect('/display_message?message=' + message)

@app.route('/display_message', methods=['GET'])
def display_message():
    message = request.args.get('message')
    return render_template('index2.html', message=message)

def fetch_matched_data_from_archive():
    user_date = request.args.get('date')
    db_connection = get_db_connection()
    cursor = db_connection.cursor()
    if not user_date:
        return jsonify({'error': 'Date parameter is required.'}), 400

    _SQL_SELECT_DATE = "SELECT EOD_DATE,Terminal, FORACID, SOL_ID, Device, ACCT_SHORT_NAME, Cash_Counters_Balance, VALUE_DATE_BAL, RCRE_TIME, Variance, RETRACTS, REMARKS FROM archive WHERE EOD_DATE = %s and merge = 'both'"
    cursor.execute(_SQL_SELECT_DATE, (user_date,))
    result = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    matched = [dict(zip(columns, row)) for row in result]

    renamed_columns = {
        'SOL_ID':'solId',
        'Device':'device',
        'Terminal': 'terminal',
        'Cash Counters Balance': 'cashCountersBalance',
        'FORACID':'foracid',
        'EOD_DATE':'EODDate',
        'RCRE_TIME':'RCRETime',
        'VALUE_DATE_BAL':'valueDateBalance',
        'Variance': 'variance',
        'RETRACTS': 'retracts',
        'REMARKS': 'remarks'
    }
    matched = [{renamed_columns.get(k, k): v for k, v in row.items()} for row in matched]
    cursor.close()
    db_connection.close()
    return jsonify(matched)

def fetch_data_from_archive():
    selected_date = request.args.get('date')
    db_connection = get_db_connection()
    cursor = db_connection.cursor()
    if not selected_date:
        return jsonify({'error': 'Date parameter is required.'}), 400

    _SQL_SELECT_DATE = "SELECT EOD_DATE, FORACID,Terminal, SOL_ID, Device, ACCT_SHORT_NAME, Cash_Counters_Balance, VALUE_DATE_BAL, RCRE_TIME, Variance, RETRACTS, REMARKS FROM archive WHERE EOD_DATE = %s and merge != 'both'"
    cursor.execute(_SQL_SELECT_DATE, (selected_date,))
    result = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    matched = [dict(zip(columns, row)) for row in result]
    print(matched)

    renamed_columns = {
        'SOL_ID':'solId',
        'Device':'device',
        'Terminal':'terminal',
        'Cash Counters Balance': 'cashCountersBalance',
        'FORACID':'foracid',
        'EOD_DATE':'EODDate',
        'RCRE_TIME':'RCRETime',
        'VALUE_DATE_BAL':'valueDateBalance',
        'Variance': 'variance',
        'RETRACTS': 'retracts',
        'REMARKS': 'remarks'
    }
    matched = [{renamed_columns.get(k, k): v for k, v in row.items()} for row in matched]
    cursor.close()
    db_connection.close()
    return jsonify(matched)

def download_selectBy_date():
    selected_date = request.args.get('date')
    if not selected_date:
        return jsonify({'error': 'Date parameter is required.'}), 400
    matched_data = fetch_data_from_archive(selected_date)
    if not matched_data:
        return jsonify({'error': 'No data found for the selected date.'}), 404

    excel_file = io.BytesIO()
    df = pd.DataFrame(matched_data, columns=['EOD_DATE', 'FORACID','Terminal', 'SOL_ID', 'Device', 'ACCT_SHORT_NAME', 'Cash_Counters_Balance', 'VALUE_DATE_BAL', 'RCRE_TIME', 'Variance', 'RETRACTS', 'REMARKS'])
    df.to_excel(excel_file, index=False, sheet_name='reconSummary')
    excel_file.seek(0)
    
    response = make_response(excel_file.getvalue())
    response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

    formatted_date = datetime.datetime.strptime(selected_date, '%Y-%m-%d').strftime('%Y%m%d')
    filename = f'summary_{formatted_date}.xlsx'
    response.headers['Content-Disposition'] = f'attachment; filename={filename}'
    return response

def get_totals():
    matched = processor[0]
    exceptions = processor[1]
    merged = processor[2]
    matched_count = len(matched)
    exceptions_count = len(exceptions)
    merged_count = len(merged)
    totals = {
            'matched_count': matched_count,
            'exceptions_count': exceptions_count,
            'merged_count': merged_count
        }
    return jsonify(**totals)

def display_totals():
    try:
        response = requests.get('http://localhost:5000/get_totals') 
        totals = response.json()  
        print(totals)
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        totals = None 
    return render_template('totals.html', totals=totals)

def download_matched():
    matched = processor[0]
    csv_buffer = io.StringIO()
    matched = matched.rename(columns={
        'SOL_ID': 'solId',
        'Device': 'device',
        'Terminal': 'terminal',
        'Cash Counters Balance': 'cashCountersBalance',
        'FORACID': 'foracid',
        'EOD_DATE': 'EODDate',
        'RCRE_TIME': 'RCRETime',
        'VALUE_DATE_BAL': 'valueDateBal',
        'Variance': 'variance',
        'RETRACTS': 'retracts',
        'REMARKS': 'remarks'
    })
    # matched = matched[['solId','device','terminal','cashCountersBalance','foracid','EODDate','RCRETime','valueDateBalance','variance','retracts','remarks']]
    matched = matched.drop('_merge', axis=1)
    matched.to_csv(csv_buffer, index=False)
    response = make_response(csv_buffer.getvalue())
    response.headers['Content-Disposition'] = 'attachment; filename=Report.csv'
    response.headers['Content-Type'] = 'text/csv'
    return response

def download_exceptions():
    exceptions = processor[1]
    # current_date = atm_data['EOD_DATE'][0] or df['Unnamed: 25'][0]
    exceptions = exceptions[(exceptions['_merge'] != 'both')]
    excel_file = io.BytesIO()
    exceptions = exceptions[['SOL_ID','Device','Terminal','Cash Counters Balance','FORACID','EOD_DATE','RCRE_TIME','VALUE_DATE_BAL','Variance','RETRACTS','REMARKS']]
    exceptions.to_excel(excel_file, index=False, sheet_name='Exceptions')
    excel_file.seek(0) 
    response = make_response(excel_file.getvalue())
    response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    response.headers['Content-Disposition'] = 'attachment; filename=exceptions.xlsx'
    return response

if (__name__) == '__main__':
    app.run(debug=True)
