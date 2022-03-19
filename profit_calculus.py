import json
import pandas as pd
import re
import os.path
import sys 

# First we create a regex to be able to check how the profit will be calculated from the information given by the json.
regex_calc = r"(\+|-)+(\d+.?\d?)+(%|€)"
# This dictionary helps us to easily replace some chars in the 'sales' csv file
regex_replacement = {'€': '', ',': '.', r'\.': ''}

# This function it's used to calculate the sales profit and store the result into a dictionary where the category is the key and its profit the value.
def calculate_profit(sales_df, categories): # The function receives the csv data stored in a dataframe and the categories dictionary with the profit formula for each category.
    # This dictionary will store the category and its profit at based on the JSON file
    category_profit = {}
    # Using the groupby method we can divide our dataframe based on the category and its cost, the we sum the sales QUANTITY of each category.
    # Note that we use the enclosure sort=False to maintain the order of groups as they appear in the df.
    df_groups = sales_df.groupby(['CATEGORY', 'COST'], as_index=False, sort=False)['QUANTITY'].sum()
    # Then we can iterate through each group to calculates the profits
    for row in df_groups.index:
        # To obtain the values I'm using .at because it's faster than .loc and we can make sure the code still works even if the columns changed their positions.
        category = df_groups.at[row, 'CATEGORY']
        # To calculate the profit it's needed to check if the product category appears in our category dictionary keys to retrieve the profit formula. If there is match we access the dictionary
        # in the category key, if there's no match access the '*' (others) option.
        profit_calculus = categories['*'] if category not in categories.keys() else categories[category]
        # With the profit calculation formula it is necessary to identify the elements of the equation to assist in the calculation.
        # For this we apply the formula in the regular expression regex_calc where the result is a list of tuples with the formula tokens when using the 'findall' method.
        check_regex = re.findall(regex_calc, profit_calculus)
        # Initializing the variable profit_sum with 0 which will stores the final profit.
        profit_sum = 0
        # For each tuple (each part of the formula) we apply our profit calculus
        for refind in check_regex:
            # Here there's two conditions: firt one is when we apply a percentage over the sales.
            if refind[2] == '%':
                profit_value = (float(refind[1])/100) * df_groups.at[row, 'COST']*df_groups.at[row, 'QUANTITY']
            # Second one is when we only need to sum a certain value over each sold quantity.
            else:
                profit_value = float(refind[1])*df_groups.at[row, 'QUANTITY']
            # Then we sum or subtract the value from our profit in order to keep the final profit value.
            profit_sum = profit_sum + profit_value if refind[0] == '+' else profit_sum-profit_value
         # After the loop goes through all categories we can append the final profit into our dictionary.
        category_profit[category] = format(round(profit_sum, 2), '.2f')
    return category_profit

# This function will check if a file exists or not in the system, in order to prevent some error, if the file doesn't exists the code will throw a message and stop immediately.
def check_if_file_exists(file):
    if not os.path.exists(file):
        print('File '+file+' doesn\'t exist! Please try again.')
        sys.exit()
    return True

# This function will ask the user to enter the name of the CSV and JSON files and will call the function that checks if the files exist.
def retrieve_files():
    csv_file = input('Please enter with your CSV file name:\n')+'.csv' 
    check_if_file_exists(csv_file)
    json_file = input('Please enter with your JSON file name:\n')+'.json'
    check_if_file_exists(json_file)
    return csv_file, json_file

# This function read the json file.
def read_category_json(json_file_name):
    # The enclosure 'with' is useful to open, read our json file with 'json.load' then immediately close it because its necessary to to read it once.
    # One thing to note is that we use enconding="utf-8" to read non-ASCII carachters such as the euro sign '€'
    with open(json_file_name, encoding="utf-8") as f:
        json_data = json.load(f)
    # Then we retrieve our dictionary which contains the categories and the formula to obtains the sales price.
    return json_data['categories']

def reading_csv_file_and_converting_to_dataframe(csv_file_name):
    sales_df = pd.DataFrame()
    # Since our sales file can have an huge size. Read the whole file with pandas at once will results in a low perfomance and ultimately an insufficient memory usage with our dataset is bigger than 1GB.
    # So we need to deal with this limitation reducing the memory usage dividing our file into chunks using the parameter chunksize in the pd.read_csv which means the number of rows to be read sequentially.
    for chunk in pd.read_csv(csv_file_name, chunksize=10000, delimiter=';', thousands="."):
        # Having read the chunk we need to handle some conversions on the data so that python can correctly calculate the values.
        # For example, remove the € character and change comma to period. By doing this we can easily convert string values to float.
        # Python can helps us with the replace method passing by argument our regex_replacement dictionary and with the closure regex seted with True it will replace the matching characters acoording to dictionary in the entire Dataframe.
        chunk.replace(regex_replacement, regex=True, inplace=True)
        # Since we're working with a monetary values in the COST column we can save some time converting the whole column to float with the astype method.
        chunk['COST'] = chunk['COST'].astype(float)
        # After having read and worked on the data the chunk we can append it on a dataframe which by the end of the process will have the entire dataset provided by the csv file.
        sales_df = pd.concat([sales_df, chunk]) 
    return sales_df

def main():
    # The code starts by reading the filenames
    csv_file_name, json_file_name = retrieve_files()
    # Then the data is read.
    categories = read_category_json(json_file_name)
    sales_df = reading_csv_file_and_converting_to_dataframe(csv_file_name)
    # After that the profit is calculated.
    category_profit = calculate_profit(sales_df, categories)
    # Then the result is printed.
    for key, value in category_profit.items():
        print(key+':', value)

if __name__ == '__main__':
    main()