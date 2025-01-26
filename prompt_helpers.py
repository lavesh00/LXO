import subprocess

def execute_sqlplus_command(command):
    """Execute a SQL*Plus command and return the output."""
    try:
        # Attempt to run the SQL*Plus command
        process = subprocess.Popen(
            ["sqlplus", "-S", "/ as sysdba"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        stdout, stderr = process.communicate(command)
        if process.returncode != 0:
            raise Exception(f"SQL*Plus Error: {stderr.strip()}")
        return stdout.strip()
    except Exception as e:
        print(f"Error executing SQL*Plus command: {e}")
        print("Ensure that your ORACLE_HOME is correctly set and SQL*Plus is properly configured.")
        return None

def prompt_user_for_table_creation(execute_sqlplus_command):
    """Prompt the user to create a table dynamically in Oracle."""
    table_name = input("Enter the name of the table to create: ").strip()

    # Validate the number of attributes (columns)
    while True:
        try:
            num_columns = int(input("Enter the number of attributes (columns) for the table: "))
            if num_columns <= 0:
                print("Please enter a positive integer.")
                continue
            break
        except ValueError:
            print("Invalid input. Please enter a valid number.")

    columns = []
    for i in range(num_columns):
        column_name = input(f"Enter the name of column {i + 1}: ").strip()
        data_type = input(f"Enter the data type for column {column_name} (e.g., VARCHAR2(50), NUMBER, DATE): ").strip()
        columns.append(f"{column_name} {data_type}")

    create_table_query = f"CREATE TABLE {table_name} ({', '.join(columns)})"

    # Execute the SQL*Plus command to create the table
    result = execute_sqlplus_command(create_table_query)
    if result:
        print(f"Table '{table_name}' created successfully!")
    else:
        print("Failed to create the table. Please check your SQL*Plus setup or syntax.")

    return table_name
