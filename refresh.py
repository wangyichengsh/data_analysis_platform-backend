import json

from websocket import create_connection

channel_host = 'ws://localhost:8001'
ws = create_connection(channel_host+'/ws/backend/all/')
message = []
msg = {'message':message}
msg_str = json.dumps(msg)
ws.send(msg_str)
ws.close()
