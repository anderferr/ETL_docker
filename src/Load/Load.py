import psycopg2
import json
import os

class Load:
    def __init__(self, host=None, database=None, user=None, password=None, port=None):
        self.conn = psycopg2.connect(
            host=host or os.getenv('RDS_HOST'),
            database=database or os.getenv('RDS_DATABASE', 'postgres'),
            user=user or os.getenv('RDS_USER', 'admin'),
            password=password or os.getenv('RDS_PASSWORD'),
            port=port or os.getenv('RDS_PORT', 5432),
            sslmode='require'
        )
        self.cursor = self.conn.cursor()
        self._create_tables()
    
    def _create_tables(self):
        """Creates tables if they don't exist"""
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS documents (
                id SERIAL PRIMARY KEY,
                filename VARCHAR(255) UNIQUE,
                document_id VARCHAR(255),
                name VARCHAR(255),
                description TEXT
            )
        ''')
        
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS line_items (
                id SERIAL PRIMARY KEY,
                document_id INTEGER REFERENCES documents(id),
                item VARCHAR(255),
                quantity DECIMAL(10,2),
                unit_price DECIMAL(10,2),
                total DECIMAL(10,2)
            )
        ''')
        self.conn.commit()
    
    def load(self, transformed_data):
        """Loads transformed data into PostgreSQL (incremental)"""
        for filename, doc in transformed_data.items():
            self.cursor.execute('SELECT id FROM documents WHERE filename = %s', (filename,))
            existing = self.cursor.fetchone()
            
            if existing:
                doc_id = existing[0]
                self.cursor.execute('''
                    UPDATE documents SET document_id = %s, name = %s, description = %s
                    WHERE id = %s
                ''', (doc['document_id'], doc['name'], doc['description'], doc_id))
                
                self.cursor.execute('DELETE FROM line_items WHERE document_id = %s', (doc_id,))
            else:
                self.cursor.execute('''
                    INSERT INTO documents (filename, document_id, name, description)
                    VALUES (%s, %s, %s, %s) RETURNING id
                ''', (filename, doc['document_id'], doc['name'], doc['description']))
                doc_id = self.cursor.fetchone()[0]
            
            for item in doc['line_items']:
                self.cursor.execute('''
                    INSERT INTO line_items (document_id, item, quantity, unit_price, total)
                    VALUES (%s, %s, %s, %s, %s)
                ''', (doc_id, item['item'], item['quantity'], item['unit_price'], item['total']))
        
        self.conn.commit()
        print(f"Data loaded! {len(transformed_data)} documents processed.")
    
    def close(self):
        """Closes database connection"""
        self.cursor.close()
        self.conn.close()
