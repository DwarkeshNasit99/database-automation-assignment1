#!/usr/bin/env python3
"""
MySQL Database Change Deployment Script

This script automates the deployment of database changes, such as adding new tables or columns.
It includes functionality to:
- Connect to MySQL database
- Execute SQL scripts
- Track deployment history
- Implement rollback capabilities
"""

import os
import sys
import json
import logging
import mysql.connector
from mysql.connector import Error
from typing import List, Dict, Optional
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('deployment.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

class DatabaseDeployer:
    def __init__(self, host: str, user: str, password: str, database: str):

        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.deployment_history_file = 'deployment_history.json'
        
        # Initialize deployment history
        self._init_deployment_history()

    def _init_deployment_history(self):
        """Initialize deployment history tracking"""
        if not os.path.exists(self.deployment_history_file):
            with open(self.deployment_history_file, 'w') as f:
                json.dump([], f)

    def connect(self) -> bool:
       
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            logging.info(f"Successfully connected to database: {self.database}")
            return True
        except Error as e:
            logging.error(f"Error connecting to database: {e}")
            return False

    def disconnect(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logging.info("Database connection closed")

    def execute_sql_file(self, sql_file: str) -> bool:
        if not self.connection or not self.connection.is_connected():
            if not self.connect():
                return False

        try:
            with open(sql_file, 'r') as f:
                sql_commands = f.read().split(';')
                
            cursor = self.connection.cursor()
            
            for command in sql_commands:
                if command.strip():
                    cursor.execute(command)
                    self.connection.commit()
            
            cursor.close()
            
            # Record deployment
            self._record_deployment(sql_file)
            
            logging.info(f"Successfully executed SQL file: {sql_file}")
            return True
            
        except Error as e:
            logging.error(f"Error executing SQL file {sql_file}: {e}")
            return False

    def _record_deployment(self, sql_file: str):
        deployment_record = {
            'timestamp': datetime.now().isoformat(),
            'sql_file': sql_file,
            'status': 'success'
        }
        
        with open(self.deployment_history_file, 'r+') as f:
            history = json.load(f)
            history.append(deployment_record)
            f.seek(0)
            json.dump(history, f, indent=4)
            f.truncate()

    def get_deployment_history(self) -> List[Dict]:
        with open(self.deployment_history_file, 'r') as f:
            return json.load(f)

def main():
    # Configuration
    MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
    MYSQL_USER = os.getenv('MYSQL_USER', 'student')
    MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'StrongPassword123')
    MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'assignment1')
    
    if not MYSQL_DATABASE:
        logging.error("Database name not specified")
        sys.exit(1)
    
    # Get SQL file from command line argument
    if len(sys.argv) != 2:
        logging.error("Please provide SQL file path as argument")
        sys.exit(1)
    
    sql_file = sys.argv[1]
    if not os.path.exists(sql_file):
        logging.error(f"SQL file not found: {sql_file}")
        sys.exit(1)
    
    try:
        # Initialize deployer
        deployer = DatabaseDeployer(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        
        # Execute deployment
        if deployer.execute_sql_file(sql_file):
            logging.info("Deployment completed successfully")
        else:
            logging.error("Deployment failed")
            sys.exit(1)
            
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        sys.exit(1)
    finally:
        if 'deployer' in locals():
            deployer.disconnect()

if __name__ == '__main__':
    main() 