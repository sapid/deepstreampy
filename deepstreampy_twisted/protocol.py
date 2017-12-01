#!/usr/bin/env python
from autobahn.twisted.websocket import WebSocketClientProtocol, WebSocketClientFactory
from twisted.internet.protocol import ReconnectingClientFactory
from deepstreampy_twisted import constants
from deepstreampy_twisted.message import message_parser, message_builder

class DeepstreamProtocol(WebSocketClientProtocol):
    # Protocols are used once per session; they are not re-used. 
    # If reconnection occurs, a new protocol is created by the factory in use.
    def onConnect(self, response):
        # TODO: Anything to do?
        # Options:
        #  Check or set cookies or other HTTP headers
        #  Verify IP address
        # TODO: Verify we're speaking Deepstream, a subprotocol of WebSocket, essentially
        print("Connected to Server: {}".format(response.peer))
        self.factory._set_state(constants.connection_state.AWAITING_CONNECTION)
        self.debug("onConnect response: {}".format(response))
    def onOpen(self):
        # TODO: If we have auth information stored, negotiate authentication again.
        # TODO: If auth is open, we don't need auth information to login. 
        #         How can we check if auth is open? Attempt to login anyway and catch exceptions?
        #         Per https://deepstream.io/info/protocol/all-messages/ it may not be possible to query for state.
        # TODO: Switch flag to start flushing message queue as well?
            self.debug("Connection opened.")
            self.factory._set_state(constants.connection_state.AUTHENTICATING)
            self.authenticate()
        # TODO: Initialize heartbeat
        # For looping calls see http://twistedmatrix.com/documents/13.2.0/core/howto/time.html
        # l = twisted.internet.loopingCall(func_to_call)
        # l.start(self.factory._heartbeat_time) # TODO default should be?
        # self.factory._heartbeat_looper = l
        # TODO: Reset retry timer in reconnectfactory.
    def onMessage(self, payload, isBinary):
        if isBinary:
            raise NotImplementedError
        self.debug(payload)
        # TODO: Do we need to catch decoding errors?
        #text = payload.decode('UTF-8', errors='strict') # Unnecessary?
        full_buffer = self.factory._message_buffer + payload
        split_buffer = full_buffer.rsplit(constants.message.MESSAGE_SEPERATOR, 1)
        if len(split_buffer) > 1:
            self.factory._message_buffer = split_buffer[1]
        raw_messages = split_buffer[0]
        # TODO Urgent: Uncomment below once message_parser is fixed
        #parsed_messages = message_parser.parse(raw_messages, self._client)
        #for msg in parsed_messages:
        #    if msg is None:
        #        continue

    def heartbeat(self):
        raise NotImplementedError
        # TODO: Check if we've missed any.
    def authenticate(self):
        # TODO: Write this
        self.factory._set_state(constants.connection_state.AWAITING_AUTHENTICATION)
    def _auth_response_handler(self, data):
        pass
    def onClose(self, wasClean, code, reason):
        # TODO: Anything to do?
        self.factory._set_state(constants.connection_state.CLOSED)
        print("WebSocket connection closed: {}".format(reason))
        # if self.factory._heartbeat_looper:
        #   self.factory._heartbeat_looper.cancel()
        #   self.factory._heartbeat_looper = None
    def sendData(self, data):
        # TODO: Batch messages
        self.transport.write(data)
        self.debug("Sent " + str(data))
    def debug(self, message):
        if not self.factory.debug:
            return
        print(message.replace(chr(31), '|').replace(chr(30), '+'))

class DeepstreamFactory(WebSocketClientFactory, ReconnectingClientFactory):
    # Factories store any stateful information a protocol might need.
    # This way, if reconnection occurs, that state information is still available to the new protocol.
    # In the Twisted paradigm, the factory is initialized then handed off to the reactor which initiates the connection.
    # TODO note: Important for message queue: self.(connection?).
    handlers_needed = [constants.topic.CONNECTION,
                       constants.topic.AUTH,
                       constants.topic.EVENT,
                       constants.topic.PRIVATE,
                       constants.topic.RPC,
                       constants.topic.ERROR,
                       constants.topic.PRESENCE,
                       constants.topic.RECORD,
                       ]
    def __init__(self, url, *args, **kwargs):
        self.url = url
        self._state = constants.connection_state.CLOSED
        kwargs['url'] = url
        self.debug = kwargs.pop('debug', False)
        self._heartbeat_looper = None
        self._message_buffer = ''
        self.authParams = kwargs.pop('authParams', None)
        self.authToken = None
        super(DeepstreamFactory, self).__init__(*args, **kwargs)
    def _set_state(self, state):
        # This state keeps track of the connection with Deepstream per the
        # Deepstream spec. This state is distinct from the state
        # handled by ReconnectingClientFactory.
        self._state = state
    def setAuth(self, authParams):
        # TODO: Write docstring
        self.authParams = authParams
    def startedConnecting(self, connector):
        self._set_state(constants.connection_state.AWAITING_CONNECTION)
    # TODO: def clientConnectionFailed and def clientConnectionLost are inherited from ReconnectingClientFactory.
    #       Can we add _set_state updates to these?
    #       Should be able to just call retry with super, also.
    #       TODO: Retry timer doesn't reset on connect.


# The following code should only be used for testing and developing this library.
if __name__ == '__main__':
    import sys
    from twisted.python import log
    from twisted.internet import reactor
    from twisted.internet.protocol import Factory
    from autobahn.twisted.websocket import WebSocketClientFactory,WebSocketClientProtocol

    log.startLogging(sys.stdout)
    factory = DeepstreamFactory(u"ws://localhost:6020/deepstream", debug=True)
    # TODO: Set auth information for factory
    factory.protocol = DeepstreamProtocol
    reactor.connectTCP("127.0.0.1", 6020, factory)
    reactor.run()

