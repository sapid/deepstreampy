from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy_twisted.constants import actions as action_constants
from deepstreampy_twisted.constants import event as event_constants
from deepstreampy_twisted.constants import connection_state

from pyee import EventEmitter
from tornado import concurrent

from functools import partial
from collections import namedtuple

import sys
import time
import random

num_types = ((int, long, float, complex) if sys.version_info < (3,) else
             (int, float, complex))
str_types = (str, unicode) if sys.version_info < (3,) else (str,)


class SingleNotifier(object):

    def __init__(self, client, connection, topic, action, timeout_duration):
        self._client = client
        self._connection = connection
        self._topic = topic
        self._action = action
        self._timeout_duration = timeout_duration

        self._requests = dict()

        self._resubscribe_notifier = ResubscribeNotifier(client,
                                                         self._resend_requests)

    def has_request(self, name):
        return name in self._requests

    def request(self, name, callback):
        if name not in self._requests:
            self._requests[name] = []
            future = self._connection.send_message(
                self._topic, self._action, [name])
        else:
            future = concurrent.Future()
            future.set_result()

        response_timeout = self._client.io_loop.call_later(
            self._timeout_duration, partial(self._on_response_timeout, name))
        self._requests[name].append({'timeout': response_timeout,
                                     'callback': callback})

        return future

    def receive(self, name, error, data):
        entries = self._requests[name]
        for entry in entries:
            self._client.io_loop.remove_timeout(entry['timeout'])
            entry['callback'](error, data)
        del self._requests[name]

    def _on_response_timeout(self, name):
        msg = ('No response received in time for '
               '{0}|{1}|{2}').format(self._topic, self._action, name)
        self._client._on_error(self._topic,
                               event_constants.RESPONSE_TIMEOUT,
                               msg)

    def _resend_requests(self):
        for request in self._requests:
            self._connection.send_message(
                self._topic, self._action, [self._requests[request]])

CallbackResponse = namedtuple('CallbackResponse', 'accept reject')


class Listener(object):

    def __init__(self, listener_type, pattern, callback, options, client,
                 connection):
        self._type = listener_type
        self._callback = callback
        self._pattern = pattern
        self._options = options
        self._client = client
        self._connection = connection
        self._send_future = None

        subscription_timeout = options.get("subscriptionTimeout", 15)
        self._ack_timeout = connection._io_loop.call_later(subscription_timeout,
                                                           self._on_ack_timeout)
        self._resubscribe_notifier = ResubscribeNotifier(client,
                                                         self._send_listen)
        self._send_listen()
        self._destroy_pending = False

    def send_destroy(self):
        self._destroy_pending = True
        future = self._connection.send_message(
            self._type, action_constants.UNLISTEN, [self._pattern])
        self._resubscribe_notifier.destroy()
        return future

    def destroy(self):
        self._callback = None
        self._pattern = None
        self._client = None
        self._connection = None

    def accept(self, name):
        return self._connection.send_message(
            self._type, action_constants.LISTEN_ACCEPT, [self._pattern, name])

    def reject(self, name):
        return self._connection.send_message(
            self._type, action_constants.LISTEN_REJECT, [self._pattern, name])

    def _create_callback_response(self, message):
        return CallbackResponse(accept=partial(self.accept, message['data'][1]),
                                reject=partial(self.reject, message['data'][1]))

    def _on_message(self, message):
        action = message['action']
        data = message['data']
        if action == action_constants.ACK:
            self._connection._io_loop.remove_timeout(self._ack_timeout)
        elif action == action_constants.SUBSCRIPTION_FOR_PATTERN_FOUND:
            # TODO: Show deprecated message
            self._callback(
                data[1], True, self._create_callback_response(message))
        elif action == action_constants.SUBSCRIPTION_FOR_PATTERN_REMOVED:
            self._callback(data[1], False)
        else:
            is_found = (message['action'] ==
                        action_constants.SUBSCRIPTION_FOR_PATTERN_FOUND)
            self._callback(message['data'][1], is_found)

    def _send_listen(self):
        self._send_future = self._connection.send_message(
            self._type, action_constants.LISTEN, [self._pattern])

    def _on_ack_timeout(self):
        self._client._on_error(self._type, event_constants.ACK_TIMEOUT,
                               ('No ACK message received in time for ' +
                                self._pattern))

    @property
    def destroy_pending(self):
        return self._destroy_pending

    @property
    def send_future(self):
        return self._send_future


class ResubscribeNotifier(object):
    """
    Makes sure that all functionality is resubscribed on reconnect. Subscription
    is called when the connection drops - which seems counterintuitive, but in
    fact just means that the re-subscription message will be added to the queue
    of messages that need re-sending as soon as the connection is
    re-established.

    Resubscribe logic should only occur once per connection loss.
    """

    def __init__(self, client, resubscribe):
        """
        Args:
            client: The deepstream client
            resubscribe: callable to call to allow resubscribing
        """
        self._client = client
        self._resubscribe = resubscribe

        self._is_reconnecting = False
        self._client.on(event_constants.CONNECTION_STATE_CHANGED,
                        self._handle_connection_state_changes)

    def destroy(self):
        self._client.remove_listener(event_constants.CONNECTION_STATE_CHANGED,
                                     self._handle_connection_state_changes)
        self._client = None

    def _handle_connection_state_changes(self, state):
        if state == connection_state.RECONNECTING and not self._is_reconnecting:
            self._is_reconnecting = True
        elif state == connection_state.OPEN and self._is_reconnecting:
            self._is_reconnecting = False
            self._resubscribe()


class AckTimeoutRegistry(EventEmitter):

    def __init__(self, client, topic, timeout_duration):
        super(AckTimeoutRegistry, self).__init__()
        self._client = client
        self._topic = topic
        self._timeout_duration = timeout_duration
        self._register = {}

    def add(self, name, action=None):
        unique_name = (action or "") + name

        self.remove(name, action)
        self._client.io_loop.call_later(self._timeout_duration,
                                        partial(self._on_timeout,
                                                unique_name,
                                                name))
        self._register[unique_name] = None

    def remove(self, name, action=None):
        unique_name = (action or "") + name
        if unique_name in self._register:
            self.clear({'data': [action, name]})

    def clear(self, message):
        if len(message['data']) > 1:
            name = message['data'][1]
        else:
            name = ""

        unique_name = (message['data'][0] or "") + name
        timeout = self._register.get(unique_name, self._register.get(name))

        if timeout:
            self._client.io_loop.remove_timeout(timeout)
        else:
            self._client._on_error(self._topic,
                                   event_constants.UNSOLICITED_MESSAGE,
                                   message.get('raw', ''))

    def _on_timeout(self, unique_name, name):
        del self._register[unique_name]
        msg = "No ACK message received in time for " + name
        self._client._on_error(self._topic, event_constants.ACK_TIMEOUT, msg)
        self.emit('timeout', name)


def _pad_list(l, index, value):
    l.extend([value] * (index - len(l)))


class Undefined(object):

    def __repr__(self):
        return 'Undefined'

Undefined = Undefined()


def itoa(num, radix):
    """
    Convert int to a sts representation in an arbitrary base, up to 36.
    """
    result = ""
    while num > 0:
        result = "0123456789abcdefghijklmnopqrstuvwxyz"[num % radix] + result
        num //= radix
    return result


def get_uid():
    timestamp = itoa(int(time.time() * 1000), 36)
    random_str = itoa(int(random.random() * 10000000000000000), 36)
    return "{0}-{1}".format(timestamp, random_str)
