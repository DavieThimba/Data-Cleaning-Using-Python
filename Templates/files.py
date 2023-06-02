import os
import re
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

def select_files_by_date(directory):
    atm_pattern = r".*\\ATMS.*\.xlsx"
    df_pattern = r".*\\.*CashStateReport\.xls$"

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

    user_date = input("Enter a date (DD-MONTH-YYYY format): ").upper()
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

directory = r"C:\Users\THIMBA\Desktop\Vynamic files"
atm_file, df_file = select_files_by_date(directory)


