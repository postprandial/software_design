from socketserver import BaseRequestHandler, TCPServer
from jsonschema import validate
import time
import json

class EchoHandler(BaseRequestHandler):
    def handle(self):
        print('Got connection from', self.client_address)
        while True:
            msg = self.request.recv(8192)
            # 8192 refers to the number of received bytes to get from the request in one data block
            if msg:
                print(msg)
                # now validate the message
                json_payload = json.loads(msg)
                f = open('json/json_book.schema')
                schema = json.loads(f.read())
                f.close()
                validate(json_payload, schema)
            else:
                break

if __name__ == '__main__':
    print("Waiting for connection ...")
    serv = TCPServer(('', 20_001), EchoHandler)
    serv.serve_forever()
