#!/usr/bin/env python3
"""
MySQL Database Backup Automation Script

This script automates the process of creating backups for MySQL databases.
It includes functionality to:
- Connect to MySQL database
- Create timestamped backup files
- Handle multiple databases
- Implement error handling
"""

import os
import sys
import datetime
import subprocess
import logging
from typing import List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('backup.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

class MySQLBackup:
    def __init__(self, host: str, user: str, password: str, backup_dir: str):
        self.host = host
        self.user = user
        self.password = password
        self.backup_dir = backup_dir
        
        # Create backup directory if it doesn't exist
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
            logging.info(f"Created backup directory: {backup_dir}")

    def get_databases(self) -> List[str]:
        try:
            cmd = [
                'mysql',
                f'--host={self.host}',
                f'--user={self.user}',
                f'--password={self.password}',
                '-e', 'SHOW DATABASES;'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            databases = result.stdout.strip().split('\n')[1:]  # Skip header
            return [db for db in databases if db not in ['information_schema', 'performance_schema', 'mysql', 'sys']]
        except subprocess.CalledProcessError as e:
            logging.error(f"Error getting databases: {e}")
            return []

    def create_backup(self, database: str) -> Optional[str]:
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = os.path.join(self.backup_dir, f"{database}_{timestamp}.sql")
        
        try:
            cmd = [
                'mysqldump',
                f'--host={self.host}',
                f'--user={self.user}',
                f'--password={self.password}',
                '--single-transaction',
                '--routines',
                '--triggers',
                '--events',
                database
            ]
            
            with open(backup_file, 'w') as f:
                subprocess.run(cmd, stdout=f, check=True)
            
            logging.info(f"Successfully created backup for {database}: {backup_file}")
            return backup_file
        except subprocess.CalledProcessError as e:
            logging.error(f"Error creating backup for {database}: {e}")
            return None

    def backup_all_databases(self) -> List[str]:
        databases = self.get_databases()
        backup_files = []
        
        for database in databases:
            backup_file = self.create_backup(database)
            if backup_file:
                backup_files.append(backup_file)
        
        return backup_files

def main():
    # Configuration
    MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
    MYSQL_USER = os.getenv('MYSQL_USER', 'student')
    MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'StrongPassword123')
    BACKUP_DIR = os.getenv('BACKUP_DIR', 'mysql_backups')
    
    try:
        # Initialize backup handler
        backup_handler = MySQLBackup(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            backup_dir=BACKUP_DIR
        )
        
        # Create backups
        backup_files = backup_handler.backup_all_databases()
        
        if backup_files:
            logging.info(f"Successfully created {len(backup_files)} backups")
        else:
            logging.warning("No backups were created")
            
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main() 