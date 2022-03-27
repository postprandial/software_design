from socketserver import BaseRequestHandler, TCPServer
from jsonschema import validate
import time
import json
import sqlite3


class EchoHandler(BaseRequestHandler):
    def handle(self):
        print('Got connection from', self.client_address)
        while True:
            msg = self.request.recv(8192)
            print("Message received:", msg)
            if msg:
                print("if msg was triggered")
                json_payload = json.loads(msg)  # this turns a string into a python dict
                print(json_payload)  # but this doesn't interpret the newline character
                schema = json.loads(open('json/json_book.schema').read())  # dictionary
                print(validate(json_payload, schema))

                conn = sqlite3.connect('books.db')
                c = conn.cursor()
                c.execute("""CREATE TABLE IF NOT EXISTS Books (
                    title TEXT, 
                    author TEXT, 
                    isbn_10 TEXT, 
                    quality TEXT, 
                    language TEXT, 
                    publication_date TEXT, 
                    type TEXT, 
                    purchase_price REAL
                    )""")
                c.execute("INSERT INTO Books VALUES (?,?,?,?,?,?,?,?)",
                          (json_payload['title'],
                           json_payload['author'],
                           json_payload['isbn-10'],
                           json_payload['quality'],
                           json_payload['language'],
                           json_payload['publication_date'],
                           json_payload['type'],
                           json_payload['purchase_price']))
                rows = c.execute("SELECT * FROM Books").fetchall()
                print(f"*****Fetching all rows from Books***** \n {rows}")
            else:
                break


# TODO: Give better structure to the sqlite3 code from above
"""
class Database(EchoHandler):
    def __init__(self):
        pass

    def create_connection(self):
        pass
        
    def create_table(self):
        pass
        
    def insert(self):
        pass
"""

if __name__ == '__main__':
    print("Waiting for connection ...")
    serv = TCPServer(('', 20_001), EchoHandler)
    serv.serve_forever()
