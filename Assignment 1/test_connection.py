import mysql.connector
from mysql.connector import Error

def test_mysql_connection():
    try:
        # Replace these with your actual MySQL credentials
        connection = mysql.connector.connect(
            host='localhost',
            user='student',
            password='StrongPassword123',
            database='assignment1'
        )
        
        if connection.is_connected():
            db_info = connection.get_server_info()
            print(f"Connected to MySQL Server version {db_info}")
            
            cursor = connection.cursor()
            cursor.execute("select database();")
            database = cursor.fetchone()
            print(f"Connected to database: {database[0]}")
            
            cursor.close()
            connection.close()
            print("MySQL connection is closed")
            
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")

if __name__ == "__main__":
    test_mysql_connection() 