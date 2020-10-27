# query.py

import cx_Oracle

# Establish the database connection
# connection = cx_Oracle.connect("system", "oracle", "dbhost.example.com/orclpdb1")
connection = cx_Oracle.connect("system", "oracle", "127.0.0.1/xe")


# Obtain a cursor
cursor = connection.cursor()

# Data for binding
managerId = 145
firstName = "Peter"

# Execute the query
sql = """SELECT first_name, last_name
         FROM hr.employees
         WHERE manager_id = :mid AND first_name = :fn"""
cursor.execute(sql, mid = managerId, fn = firstName)

# Loop over the result set
for row in cursor:
    print(row)