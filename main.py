import psycopg2
from psycopg2 import Error
from datetime import datetime
from typing import Final
from db import db_config



def check_database_connection():
    """
    Check the database connection status and update the connection status in db_config dictionary.
    """
    try:
        connection = psycopg2.connect(db_config)
        db_config['connection_status'] = True
        connection.close()
    except (Exception, Error) as error:
        print("Error connecting to the database:", error)
        db_config['connection_status'] = False

# Call the function to check the database connection status
check_database_connection()


FORMULAS: list[str] = ["SUM", "VLOOKUP", "IF", "LOOKUP", "MATCH", "CHOOSE", "CONCATENATE", "DATE", "DAYS", "FIND", "INDEX"]


# Function to execute SQL query and return results
def execute_query(query):
    """
    Execute the given SQL query and return the results.
    
    :param query: SQL query to be executed
    :return: Query results or None if an error occurs
    """
    try:
        connection = psycopg2.connect(db_config)
        cursor = connection.cursor()
        cursor.execute(query)

        # Fetch all rows from the result
        result = cursor.fetchall()

        # Commit the transaction
        connection.commit()

        # Close the cursor and connection
        cursor.close()
        connection.close()

        return result

    except (Exception, Error) as error:
        print("Error executing query:", error)
        return None


# Implementing SUM formula
def calculate_sum(column_name, table_name):
    query = f"SELECT SUM({column_name}) FROM {table_name};"
    result = execute_query(query)
    if result and result[0][0] is not None:
        return result[0][0]
    else:
        return 0


# Implementing VLOOKUP formula
def vlookup(lookup_value, lookup_column, result_column, table_name):
    query = f"SELECT {result_column} FROM {table_name} WHERE {lookup_column}='{lookup_value}';"
    result = execute_query(query)
    if result and result[0][0] is not None:
        return result[0][0]
    else:
        return "Not Found"


# Implementing IF formula
def if_formula(condition, true_value, false_value):
    if condition:
        return true_value
    else:
        return false_value


# Implementing LOOKUP formula
def lookup(lookup_value, lookup_column, result_column, table_name):
    query = f"SELECT {result_column} FROM {table_name} WHERE {lookup_column}='{lookup_value}';"
    result = execute_query(query)
    if result and result[0][0] is not None:
        return result[0][0]
    else:
        return "Not Found"


# Implementing MATCH formula
def match(lookup_value, lookup_column, table_name):
    query = f"SELECT {lookup_column} FROM {table_name} WHERE {lookup_column}='{lookup_value}';"
    result = execute_query(query)
    if result and result[0][0] is not None:
        return result[0][0]
    else:
        return "Not Found"


# Implementing CHOOSE formula
def choose(index, *choices):
    if index >= 1 and index <= len(choices):
        return choices[index - 1]
    else:
        return "Invalid Index"

# Implementing CONCATENATE formula
def concatenate(*args):
    return ''.join(str(arg) for arg in args) 


def date(year, month, day):
    try:
        return datetime(year, month, day).strftime('%Y-%m-%d')
    except ValueError:
        return "Invalid date"


# Implementing DAYS formula (assuming date_format is 'YYYY-MM-DD')
def days(end_date, start_date):
    try:
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        delta = end_date - start_date
        return delta.days
    except ValueError:
        return "Invalid date format"


# Implementing FIND/FINDB formula
def find(substring, text):
    if substring in text:
        return text.index(substring) + 1  # Adjust index to match spreadsheet behavior
    else:
        return "Not Found"


# Implementing INDEX formula
def index(row_num, col_num, data):
    try:
        return data[row_num - 1][col_num - 1]  # Adjust indices to match spreadsheet behavior
    except IndexError:
        return "Index out of range"


# Main function to handle user input
def main():
    if db_config['connection_status']:
        while True:
            print("Available formulas:", ", ".join(FORMULAS))
            formula = input("Enter formula (or 'exit' to quit): ").upper()


            if formula == "SUM":
                column_name = input("Enter column name: ")
                table_name = input("Enter table name: ")
                result = calculate_sum(column_name, table_name)
                print("Result:", result)


            elif formula == "VLOOKUP":
                lookup_value = input("Enter lookup value: ")
                lookup_column = input("Enter lookup column name: ")
                result_column = input("Enter result column name: ")
                table_name = input("Enter table name: ")
                result = vlookup(lookup_value, lookup_column, result_column, table_name)
                print("Result:", result)


            elif formula == "IF":
                condition = input("Enter condition (True/False): ").lower() == 'true'
                true_value = input("Enter value if True: ")
                false_value = input("Enter value if False: ")
                result = if_formula(condition, true_value, false_value)
                print("Result:", result)


            elif formula == "LOOKUP":
                lookup_value = input("Enter lookup value: ")
                lookup_column = input("Enter lookup column name: ")
                result_column = input("Enter result column name: ")
                table_name = input("Enter table name: ")
                result = lookup(lookup_value, lookup_column, result_column, table_name)
                print("Result:", result)


            elif formula == "MATCH":
                lookup_value = input("Enter lookup value: ")
                lookup_column = input("Enter lookup column name: ")
                table_name = input("Enter table name: ")
                result = match(lookup_value, lookup_column, table_name)
                print("Result:", result)


            elif formula == "CHOOSE":
                index = int(input("Enter index: "))
                choices = input("Enter choices separated by commas: ").split(',')
                result = choose(index, *choices)
                print("Result:", result)


            elif formula == "CONCATENATE":
                values = input("Enter values separated by commas: ").split(',')
                result = concatenate(*values)
                print("Result:", result)


            elif formula == "DATE":
                year = int(input("Enter year: "))
                month = int(input("Enter month: "))
                day = int(input("Enter day: "))
                result = date(year, month, day)
                print("Result:", result)


            elif formula == "DAYS":
                end_date = input("Enter first date (YYYY-MM-DD): ")
                start_date = input("Enter second date (YYYY-MM-DD): ")
                result = days(end_date, start_date)
                print("Result:", result)


            elif formula == "FIND":
                substring = input("Enter substring: ")
                text = input("Enter text: ")
                result = find(substring, text)
                print("Result:", result)


            elif formula == "INDEX":
                substring = input("Enter substring: ")
                text = input("Enter text: ")
                result = index(substring, text)
                print("Result:", result)   


            elif formula == "EXIT":
                break


            else:
                print("Invalid formula. Please try again.")


    else:
        print("Database connection is not established. Exiting.")


if __name__ == "__main__":
    main()
