import json
import pandas as pd
import re
import os.path
import sys 

regex_calc = r"(\+|-)+(\d+.?\d?)+(%|€)"
regex_replacement = {'€': '', ',': '.', r'\.': ''}

def calculate_profit(sales_df, categories):
    category_profit = {}
    df_groups = sales_df.groupby(['CATEGORY', 'COST'], as_index=False, sort=False)['QUANTITY'].sum()
    for row in df_groups.index:
        category = df_groups.at[row, 'CATEGORY']
        profit_calculus = categories['*'] if category not in categories.keys() else categories[category]
        check_regex = re.findall(regex_calc, profit_calculus)
        profit_sum = 0
        for refind in check_regex:
            if refind[2] == '%':
                profit_value = (float(refind[1])/100) * df_groups.at[row, 'COST']*df_groups.at[row, 'QUANTITY']
            else:
                profit_value = float(refind[1])*df_groups.at[row, 'QUANTITY']
            profit_sum = profit_sum + profit_value if refind[0] == '+' else profit_sum-profit_value
        category_profit[category] = format(round(profit_sum, 2), '.2f')
    return category_profit

def check_if_file_exists(file):
    if not os.path.exists(file):
        print('File '+file+' doesn\'t exist! Please try again.')
        sys.exit()
    return True

def retrieve_files():
    csv_file = input('Please enter with your CSV file name:\n')+'.csv' 
    check_if_file_exists(csv_file)
    json_file = input('Please enter with your JSON file name:\n')+'.json'
    check_if_file_exists(json_file)
    return csv_file, json_file

def read_category_json(json_file_name):
    with open(json_file_name, encoding="utf-8") as f:
        json_data = json.load(f)
    return json_data['categories']

def reading_csv_file_and_converting_to_dataframe(csv_file_name):
    sales_df = pd.DataFrame()
    for chunk in pd.read_csv(csv_file_name, chunksize=10000, delimiter=';', thousands="."):
        chunk.replace(regex_replacement, regex=True, inplace=True)
        chunk['COST'] = chunk['COST'].astype(float)
        sales_df = pd.concat([sales_df, chunk])
    return sales_df

def main():
    csv_file_name, json_file_name = retrieve_files()
    categories = read_category_json(json_file_name)
    sales_df = reading_csv_file_and_converting_to_dataframe(csv_file_name)
    category_profit = calculate_profit(sales_df, categories)
    for key, value in category_profit.items():
        print(key+':', value)

if __name__ == '__main__':
    main()