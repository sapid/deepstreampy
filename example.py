from tornado import gen
from tornado.ioloop import IOLoop
from deepstreampy.client import Client, connect

"""
Example to connect to the DeepStream server and send a message.
"""

DEEPSTREAM_URL = "ws://localhost:6020/deepstream"
EVENT_NAME = "chat"
EVENT_DATA = "hello world"

@gen.coroutine
def send_message():
    print "send_message() - ENTER"
    
    # Connect to server
    client = yield connect(DEEPSTREAM_URL)
    
    # Optional: Login to server
    result = yield client.login({})

    # Emit an event
    client.event.emit(EVENT_NAME, EVENT_DATA)
    
    print "send_message() - EXIT"

if __name__ == '__main__':
    IOLoop.current().run_sync(send_message)
