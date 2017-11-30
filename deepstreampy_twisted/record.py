from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy.constants import topic as topic_constants
from deepstreampy.constants import actions as action_constants
from deepstreampy.constants import event as event_constants
from deepstreampy.constants import connection_state
from deepstreampy.message import message_parser, message_builder
from deepstreampy.utils import ResubscribeNotifier, SingleNotifier, Listener
from deepstreampy.utils import str_types
from deepstreampy.constants import merge_strategies
from deepstreampy import jsonpath

from pyee import EventEmitter
from tornado import gen, concurrent

import json
from functools import partial
from copy import deepcopy

ALL_EVENT = 'ALL_EVENT'
ENTRY_ADDED_EVENT = 'ENTRY_ADDED_EVENT'
ENTRY_REMOVED_EVENT = 'ENTRY_REMOVED_EVENT'
ENTRY_MOVED_EVENT = 'ENTRY_MOVED_EVENT'


class Record(EventEmitter, object):

    def __init__(self, name, record_options, connection, options, client):
        super(Record, self).__init__()
        self.name = name
        self.usages = 0
        self._connection = connection
        self._client = client
        self._options = options

        self._has_provider = False
        self._record_options = record_options
        self._is_ready = False
        self._is_destroyed = False
        self._data = {}
        self._version = None
        self._old_value = None
        self._old_path_values = None
        self._queued_method_calls = list()
        self._write_callbacks = {}
        self.merge_strategy = merge_strategies.remote_wins

        self._emitter = EventEmitter()

        if 'merge_strategy' in options:
            self.merge_strategy = options['merge_strategy']

        self._resubscribe_notifier = ResubscribeNotifier(client,
                                                         self._send_read)
        record_read_ack_timeout = options.get("recordReadAckTimeout", 15)
        self._read_ack_timeout = client.io_loop.call_later(
            record_read_ack_timeout,
            partial(self._on_timeout, event_constants.ACK_TIMEOUT))

        record_read_timeout = options.get("recordReadTimeout", 15)
        self._read_timeout = client.io_loop.call_later(
            record_read_timeout,
            partial(self._on_timeout, event_constants.RESPONSE_TIMEOUT))

        self._record_delete_timeout = options.get("recordDeleteTimeout", 15)

        self._delete_ack_timeout = None
        self._discard_timeout = None

        self._read_future = self._send_read()

    def get(self, path=None):
        """
        Returns a copy of either the entire dataset of the record or, if called
        with a path - the value of that path within the record's dataset.

        Returning a copy rather than the actual value helps to prevent the
        record getting out of sync due to unintentional changes to its data.

        Args:
            path (str, optional): a JSON path
        """
        deep_copy = self._options.get('recordDeepCopy', False)
        return jsonpath.get(self._data, path, deep_copy)

    def set(self, data, path=None, callback=None):
        """
        Sets the value of either the entire dataset or of a specific path within
        the record and submits the changes to the server.

        If the new data is equal to the current data, nothing happens.

        Args:
            data: the new value of the data
            path (str, optional): a JSON path
            callback (callable)
        """
        if path is None and not isinstance(data, (dict, list)):
            raise ValueError(
                "Invalid record data {0}: Record data must be a dict or list.")

        if self._check_destroyed('set'):
            return

        if not self._is_ready:
            self._queued_method_calls.append(partial(self.set, data, path))
            return

        old_value = self._data
        deep_copy = self._options.get('recordDeepCopy', True)
        new_value = jsonpath.set(old_value, path, data, deep_copy)

        if new_value == old_value:
            return

        config = {}
        if callback:
            config['writeSuccess'] = True
            self._set_up_callback(self.version, callback)
            state = self._client.connection_state
            if state in (connection_state.CLOSED,
                         connection_state.RECONNECTING):
                callback('Connection error: error updating record as '
                         'connection was closed')

        self._send_update(path, data, config)
        self._apply_change(new_value)

    def subscribe(self, callback, path=None, trigger_now=False):
        """
        Subscribe to changes to the record's dataset.

        When called with a path it will only subscribe to updates to that path,
        rather than the entire record.

        Args:
            callback (callable)
            path (str, optional): a JSON path to subscribe for
            trigger_now (bool): specifies whether the callback should be invoked
                immediately with the current value
        """
        if self._check_destroyed('subscribe'):
            return

        self._emitter.on(path or ALL_EVENT, callback)

        if trigger_now and self._is_ready:
            if path:
                callback(jsonpath.get(self._data, path, True))
            else:
                callback(self._data)

    def unsubscribe(self, callback, path=None):
        """
        Remove a subscription that was previously made.

        Args:
            callback (callable)
            path (str, optional): the JSON path to unsibscribe for
        """
        if self._check_destroyed('unsubscribe'):
            return

        event = path or ALL_EVENT
        self._emitter.remove_listener(event, callback)

    def discard(self):
        """
        Remove all change listeners and notify the server that the client is no
        longer interested in updates for this record.
        """
        future = concurrent.Future()

        if self._check_destroyed('discard'):
            return

        def ready_callback(record):
            self.usages -= 1

            if self.usages <= 0:
                self.emit('destroyPending')
                self._discard_timeout = self._client.io_loop.call_later(
                    1, partial(self._on_timeout, event_constants.ACK_TIMEOUT))

                send_future = self._connection.send_message(
                    topic_constants.RECORD,
                    action_constants.UNSUBSCRIBE,
                    [self.name])
                send_future.add_done_callback(
                    lambda f: future.set_result(f.result()))

        self.when_ready(ready_callback)
        return future

    def delete(self):
        """
        Delete the record on the server.
        """
        future = concurrent.Future()

        if self._check_destroyed('delete'):
            return

        def ready_callback(record):
            self.emit('destroyPending')
            self._delete_ack_timeout = self._client.io_loop.call_later(
                self._record_delete_timeout,
                partial(self._on_timeout, event_constants.DELETE_TIMEOUT))

            send_future = self._connection.send_message(
                topic_constants.RECORD, action_constants.DELETE, [self.name])
            send_future.add_done_callback(
                lambda f: future.set_result(f.result()))

        self.when_ready(ready_callback)
        return future

    def when_ready(self, callback):
        if self._is_ready:
            callback(self)
        else:
            self.once('ready', partial(callback, self))

    def _set_up_callback(self, current_version, callback):
        new_version = (current_version or 0) + 1
        self._write_callbacks[new_version] = callback

    def _on_message(self, message):
        action = message['action']
        if action == action_constants.READ:
            if self.version is None:
                self._client.io_loop.remove_timeout(self._read_timeout)
                self._on_read(message)
            else:
                self._apply_update(message)

        elif action == action_constants.ACK:
            self._process_ack_message(message)

        elif action in (action_constants.UPDATE, action_constants.PATCH):
            self._apply_update(message)

        elif action == action_constants.WRITE_ACKNOWLEDGEMENT:
            versions = json.loads(message['data'][1])
            for version in versions:
                if version in self._write_callbacks:
                    callback = self._write_callbacks[version]
                    callback(message_parser.convert_typed(message['data'][2],
                                                          self._client))
                    del self._write_callbacks[version]

        elif message['data'][0] == event_constants.VERSION_EXISTS:
            self._recover_record(message['data'][2],
                                 json.loads(message['data'][3]),
                                 message)

        elif action == event_constants.MESSAGE_DENIED:
            self._clear_timeouts()

        elif action == action_constants.SUBSCRIPTION_HAS_PROVIDER:
            has_provider = message_parser.convert_typed(message['data'][1],
                                                        self._client)
            self._has_provider = has_provider
            self.emit('hasProviderChanged', has_provider)

    def _recover_record(self, remote_version, remote_data, message):
        if self.merge_strategy:
            self.merge_strategy(
                self, remote_data, remote_version, partial(
                    self._on_record_recovered,
                    remote_version,
                    remote_data,
                    message))
        else:
            self.emit('error',
                      event_constants.VERSION_EXISTS,
                      'received update for {0} but version is {1}'.format(
                          remote_version, self.version))

    def _on_record_recovered(
            self, remote_version, remote_data, message, error, data):
        if not error:
            old_version = self.version
            self._version = int(remote_version)

            old_value = self._data

            new_value = jsonpath.set(old_value, None, data, True)

            if data == remote_data:
                self._apply_change(data)

                callback = self._write_callbacks.get(self.version, None)
                if callback:
                    callback(None)
                    del self._write_callbacks[remote_version]

                return

            config = message['data'][4] if len(message['data']) >= 5 else None
            if config and json.loads(config)['writeSuccess']:
                callback = self._write_callbacks[old_version]
                del self._write_callbacks[old_version]
                self._set_up_callback(self.version, callback)

            self._send_update(None, data, config)
            self._apply_change(new_value)
        else:
            self.emit('error', event_constants.VERSION_EXISTS,
                      'received update for {0} but version is {1}'.format(
                          remote_version, self.version))

    def _process_ack_message(self, message):
        acknowledge_action = message['data'][0]

        if acknowledge_action == action_constants.SUBSCRIBE:
            self._client.io_loop.remove_timeout(self._read_ack_timeout)

        elif acknowledge_action == action_constants.DELETE:
            self.emit('delete')
            self._destroy()

        elif acknowledge_action == action_constants.UNSUBSCRIBE:
            self.emit('discard')
            self._destroy()

    def _apply_update(self, message):
        version = int(message['data'][1])
        if message['action'] == action_constants.PATCH:
            data = message_parser.convert_typed(
                message['data'][3], self._client)
        else:
            data = json.loads(message['data'][2])

        if self.version is None:
            self._version = version
        elif self.version + 1 != version:
            if message['action'] == action_constants.PATCH:
                self._connection.send_message(topic_constants.RECORD,
                                              action_constants.SNAPSHOT,
                                              [self.name])
            else:
                self._recover_record(version, data, message)

            return

        self._begin_change()
        self._version = version
        if message['action'] == action_constants.PATCH:
            jsonpath.set(self._data, message['data'][2], data, False)
        else:
            self._data = data

        self._complete_change()

    def _send_update(self, path, data, config):
        self._version += 1
        if not path:
            if config:
                msg_data = [self.name, self.version, data, config]
            else:
                msg_data = [self.name, self.version, data]
            self._connection.send_message(topic_constants.RECORD,
                                          action_constants.UPDATE,
                                          msg_data)
        else:
            if config:
                msg_data = [self.name, self.version, path,
                            message_builder.typed(data), config]
            else:
                msg_data = [self.name, self.version, path,
                            message_builder.typed(data)]
            self._connection.send_message(topic_constants.RECORD,
                                          action_constants.PATCH,
                                          msg_data)

    def _apply_change(self, new_data):
        if self.is_destroyed:
            return

        old_data = self._data
        self._data = new_data

        if not self._emitter._events:
            return

        paths = self._emitter._events.keys()
        for path in paths:
            if path == 'new_listener':
                continue

            if path == 'ALL_EVENT' and new_data != old_data:
                self._emitter.emit(ALL_EVENT, new_data)
                continue

            new_value = jsonpath.get(new_data, path, False)
            old_value = jsonpath.get(old_data, path, False)

            if new_value != old_value:
                self._emitter.emit(path, self.get(path))

    def _on_read(self, message):
        self._begin_change()
        self._version = int(message['data'][1])
        self._data = json.loads(message['data'][2])
        self._complete_change()
        self._set_ready()

    def _set_ready(self):
        self._is_ready = True
        for call in self._queued_method_calls:
            call()
        self._queued_method_calls = []
        self.emit('ready')

    def _send_read(self):
        """
        Sends the read message, either initially at record creation or after a
        lost connection has been re-established.
        """
        return self._connection.send_message(
            topic_constants.RECORD, action_constants.CREATEORREAD, [self.name])

    def _get_path(self, path):
        return jsonpath.get(self._data, path, True)

    def _begin_change(self):
        if not self._emitter._events:
            return

        # Hacky way of getting active listeners, except a special one
        paths = [event for event
                 in self._emitter._events.keys()
                 if event != 'new_listener']

        self._old_path_values = dict()

        if self._emitter.listeners(ALL_EVENT):
            self._old_value = deepcopy(self.get())

        for path in paths:
            if path != ALL_EVENT:
                self._old_path_values[path] = jsonpath.get(
                    self._data, path, True)

    def _complete_change(self):
        if (self._emitter.listeners(ALL_EVENT) and
                self._old_value != self._data):
            self._emitter.emit(ALL_EVENT, self.get())

        self._old_value = None

        if not self._old_path_values:
            return

        for path in self._old_path_values:
            current_value = jsonpath.get(self._data, path, True)

            if current_value != self._old_path_values[path]:
                self._emitter.emit(path, current_value)

        self._old_path_values = None

    def _clear_timeouts(self):
        if self._read_ack_timeout:
            self._client.io_loop.remove_timeout(self._read_ack_timeout)
        if self._discard_timeout:
            self._client.io_loop.remove_timeout(self._discard_timeout)
        if self._delete_ack_timeout:
            self._client.io_loop.remove_timeout(self._delete_ack_timeout)

    def _check_destroyed(self, method_name):
        if self._is_destroyed:
            self.emit(
                'error',
                "Can't invoke {0}. Record {1} is already destroyed".format(
                    method_name, self.name))
            return True

        return False

    def _on_timeout(self, timeout_type):
        self._clear_timeouts()
        self.emit('error', timeout_type)

    def _destroy(self):
        self._clear_timeouts()
        self._emitter.remove_all_listeners()
        self._resubscribe_notifier.destroy()
        self._is_destroyed = True
        self._is_ready = False
        self._client = None
        self._connection = None

    @property
    def has_provider(self):
        return self._has_provider

    @property
    def is_destroyed(self):
        return self._is_destroyed

    @property
    def is_ready(self):
        return self._is_ready

    @property
    def read_future(self):
        return self._read_future

    @property
    def version(self):
        return self._version


class List(Record):

    def __init__(self, name, list_options, connection, options, client):
        super(List, self).__init__(name, list_options, connection, options,
                                   client)
        self._before_structure = None
        self._has_add_listener = None
        self._has_move_listener = None

    def get(self):
        """
        Return the list of entries or an empty array if the list hasn't been
        populated yet.
        """
        entries = super(List, self).get()

        if not isinstance(entries, list):
            return []

        return entries

    def set(self, entries):
        """
        Update the list with a new set of entries.

        Args:
            entries (list): the new entries
        """
        error_msg = 'entries must be a list of record names'

        if not isinstance(entries, list):
            raise ValueError(error_msg)

        for entry in entries:
            if not isinstance(entry, str_types):
                raise ValueError(error_msg)

        if not self.is_ready:
            self._queued_method_calls.append(partial(self.set, entries))
        else:
            self._before_change()
            super(List, self).set(entries)
            self._after_change()

    def remove_entry(self, entry):
        """
        Remove the entry from the list.

        Args:
            entry (str): the entry to remove
        """
        if not self.is_ready:
            self._queued_method_calls.append(partial(self.remove_entry, entry))

        current_entries = deepcopy(super(List, self).get())
        current_entries.remove(entry)

        self.set(current_entries)

    def remove_at(self, index):
        """
        Remove the entry at the specified index.

        Args:
            index (int): the index of the entry to remove
        """
        if not self.is_ready:
            self._queued_method_calls.append(
                partial(self.remove_entry_at, index))

        current_entries = deepcopy(super(List, self).get())
        del current_entries[index]
        self.set(current_entries)

    def add_entry(self, entry, index=None):
        """
        Add an entry to the list.

        Args:
            entry (str): the entry to add
            index (int): the index at which to add the entry
        """
        if not self.is_ready:
            self._queued_method_calls.append(
                partial(self.add_entry, entry, index))

        entries = deepcopy(self.get())
        if index is not None:
            entries.insert(index, entry)
        else:
            entries.append(entry)

        self.set(entries)

    def subscribe(self, callback):
        """
        Proxies the underlying Record's subscribe method.
        """
        super(List, self).subscribe(callback)

    def unsubscribe(self, callback):
        """
        Proxies the underlying Record's unsubscribe method.
        """
        super(List, self).unsubscribe(callback)

    def _apply_update(self, message):
        if message['action'] == action_constants.PATCH:
            raise ValueError('PATCH is not supported for Lists')

        if message['data'][2][0] != '[':
            message['data'][2] = '[]'

        self._before_change()
        super(List, self)._apply_update(message)
        self._after_change()

    def _before_change(self):
        self._has_add_listener = len(self.listeners(ENTRY_ADDED_EVENT)) > 0
        self._has_remove_listener = len(self.listeners(ENTRY_REMOVED_EVENT)) > 0
        self._has_move_listener = len(self.listeners(ENTRY_MOVED_EVENT)) > 0

        if (self._has_add_listener or
                self._has_remove_listener or
                self._has_move_listener):
            self._before_structure = self._get_structure()
        else:
            self._before_structure = None

    def _after_change(self):
        if self._before_structure is None:
            return

        after = self._get_structure()
        before = self._before_structure

        if self._has_remove_listener:
            for entry in before:
                if (entry not in after or
                        len(after[entry]) < len(before[entry])):
                    for n in before[entry]:
                        if entry not in after or n not in after[entry]:
                            self.emit(ENTRY_REMOVED_EVENT, entry, n)

        if self._has_add_listener or self._has_move_listener:
            for entry in after:
                if entry not in before:
                    for n in after[entry]:
                        self.emit(ENTRY_ADDED_EVENT, entry, n)
                elif before[entry] != after[entry]:
                    added = len(before[entry]) != len(after[entry])
                    for n in after[entry]:
                        if added and n not in before[entry]:
                            self.emit(ENTRY_ADDED_EVENT, entry, n)
                        elif not added:
                            self.emit(ENTRY_MOVED_EVENT, entry, n)

    def _get_structure(self):
        structure = {}
        entries = super(List, self).get()

        for i, entry in enumerate(entries):
            if entry in structure:
                structure[entry].append(i)
            else:
                structure[entry] = [i]

        return structure

    @property
    def is_empty(self):
        return len(self.get()) == 0


class RecordHandler(EventEmitter, object):

    def __init__(self, connection, client, **options):
        super(RecordHandler, self).__init__()
        self._options = options
        self._connection = connection
        self._client = client
        self._records = {}
        self._lists = {}
        self._listeners = {}
        self._destroy_emitter = EventEmitter()

        record_read_timeout = options.get("recordReadTimeout", 15)

        self._has_registry = SingleNotifier(client,
                                            connection,
                                            topic_constants.RECORD,
                                            action_constants.HAS,
                                            record_read_timeout)

        self._snapshot_registry = SingleNotifier(client,
                                                 connection,
                                                 topic_constants.RECORD,
                                                 action_constants.SNAPSHOT,
                                                 record_read_timeout)

    @gen.coroutine
    def get_record(self, name, record_options=None):
        """
        Return an existing record or create a new one.

        Args:
            name (str): the unique name of the record
            record_options (dict): a dict of parameters for this particular
                record
        """
        if name in self._records:
            record = self._records[name]
        else:
            record = Record(name, record_options, self._connection,
                            self._options, self._client)
            record.on('error', partial(self._on_record_error, name))
            record.on('destroyPending', partial(self._on_destroy_pending, name))
            record.on('delete', partial(self._remove_record, name))
            record.on('discard', partial(self._remove_record, name))

            self._records[name] = record

            record.usages += 1

        yield record.read_future
        raise gen.Return(record)

    @gen.coroutine
    def get_list(self, name, list_options=None):
        """
        Return an exising list or create a new one.

        Args:
            name (str): the unique name of the list
            list_options (dict): a dict of parameters for this particular list
        """
        if name in self._lists:
            _list = self._lists[name]
        else:
            _list = List(name, list_options, self._connection, self._options,
                         self._client)

            self._lists[name] = _list

        if name not in self._records:
            self._records[name] = _list

            _list.on('error', partial(self._on_record_error, name))
            _list.on('destroyPending', partial(self._on_destroy_pending, name))
            _list.on('delete', partial(self._remove_record, name))
            _list.on('discard', partial(self._remove_record, name))

        self._records[name].usages += 1

        yield _list.read_future
        raise gen.Return(_list)

    def get_anonymous_record(self):
        """
        Return an anonymous record.
        """
        future = concurrent.Future()
        future.set_result(AnonymousRecord(self))
        return future

    def listen(self, pattern, callback):
        """
        Listen for record subscriptions made by this or other clients. This is
        useful to create "active" data providers, e.g. providers that only
        provide data for a particular record if a user is actually interested in
        it.

        Args:
            pattern (str): A combination of alpha numeric characters and
                wildcards(*)
            callback (callable):
        """
        if pattern in self._listeners:
            self._client._on_error(topic_constants.RECORD,
                                   event_constants.LISTENER_EXISTS, pattern)
            future = concurrent.Future()
            future.set_result(None)
        else:
            listener = Listener(topic_constants.RECORD,
                                pattern,
                                callback,
                                self._options,
                                self._client,
                                self._connection)
            self._listeners[pattern] = listener
            future = listener.send_future

        return future

    def unlisten(self, pattern):
        """
        Remove a listener that was previously registered with `listen`.

        Args:
            pattern (str): A combination of alpha numeric characters and
                wildcards(*)
        """
        if pattern in self._listeners:
            listener = self._listeners[pattern]
            if not listener.destroy_pending:
                listener.send_destroy()
                future = concurrent.Future()
                future.set_result(None)
            else:
                future = listener.destroy()
                del self._listeners[pattern]
        else:
            self._client._on_error(topic_constants.RECORD,
                                   event_constants.NOT_LISTENING,
                                   pattern)
            future = concurrent.Future()
            future.set_result(None)

        return future

    def snapshot(self, name, callback):
        """
        Retrieve the current record data without subscribing to changes.

        Args:
            name (str): the unique name of the record
            callback (callable):
        """
        if name in self._records and self._records[name].is_ready:
            callback(None, self._records[name].get())
            future = concurrent.Future()
            future.set_result(None)
        else:
            future = self._snapshot_registry.request(name, callback)

        return future

    def has(self, name, callback):
        """
        Check whether the record exists.

        Args:
            name (str): the unique name of the record
            callback (callable):
        """
        if name in self._records:
            callback(None, True)
            future = concurrent.Future()
            future.set_result(None)
        else:
            future = self._has_registry.request(name, callback)

        return future

    def handle(self, message):
        action = message['action']
        data = message['data']

        if (action == action_constants.ERROR and
                data[0] not in (event_constants.VERSION_EXISTS,
                                action_constants.SNAPSHOT,
                                action_constants.HAS)):
            message['processedError'] = True
            self._client._on_error(topic_constants.RECORD,
                                   message['data'][0], message['data'][1])
            return

        if action in (action_constants.ACK, action_constants.ERROR):
            name = data[1]
            if data[0] in (action_constants.DELETE,
                           action_constants.UNSUBSCRIBE):
                self._destroy_emitter.emit('destroy_ack_' + name, message)

                if (message['data'][0] == action_constants.DELETE and
                        name in self._records):
                    self._records[name]._on_message(message)

                return

            if data[0] in (action_constants.SNAPSHOT, action_constants.HAS):
                message['processedError'] = True
                error = message['data'][2]
                self._snapshot_registry.receive(name, error, None)
                return
        else:
            name = message['data'][0]

        processed = False

        if name in self._records:
            processed = True
            self._records[name]._on_message(message)

        if (action == action_constants.READ and
                self._snapshot_registry.has_request(name)):
            processed = True
            self._snapshot_registry.receive(name,
                                            None,
                                            json.loads(data[2]))

        if (action == action_constants.HAS and
                self._has_registry.has_request(name)):
            processed = True
            record_exists = message_parser.convert_typed(data[1], self._client)
            self._has_registry.receive(name, None, record_exists)
        listener = self._listeners.get(name, None)
        if (action == action_constants.ACK and
                data[0] == action_constants.UNLISTEN and
                listener and listener.destroy_pending):
            processed = True
            listener.destroy()
            del self._listeners[name]
            del listener
        elif listener:
            processed = True
            listener._on_message(message)
        elif action in (action_constants.SUBSCRIPTION_FOR_PATTERN_REMOVED,
                        action_constants.SUBSCRIPTION_HAS_PROVIDER):
            processed = True

        if not processed:
            self._client._on_error(topic_constants.RECORD,
                                   event_constants.UNSOLICITED_MESSAGE,
                                   name)

    def _on_record_error(self, record_name, error, message=None):
        self._client._on_error(topic_constants.RECORD, error, record_name)

    def _on_destroy_pending(self, record_name):
        on_message = self._records[record_name]._on_message
        self._destroy_emitter.once('destroy_ack_' + record_name, on_message)
        self._remove_record(record_name)

    def _remove_record(self, record_name):
        if record_name in self._records:
            del self._records[record_name]
        elif record_name in self._lists:
            del self._lists[record_name]


class AnonymousRecord(EventEmitter, object):

    def __init__(self, record_handler):
        super(AnonymousRecord, self).__init__()
        self._record_handler = record_handler
        self._name = None
        self._record = None
        self._subscriptions = []
        self._proxy_method('delete')
        self._proxy_method('set')
        self._proxy_method('discard')

    def get(self, path=None):
        """
        Proxies the actual record's get method.

        Args:
            path (str, optional): a JSON path. If not provided, the entire
                record is returned
        """
        if self._record is None:
            return None

        return self._record.get(path)

    def subscribe(self, callback, path=None):
        """
        Proxies the actual record's subscribe method.

        Args:
            callback (callable):
            path (str): a JSON path. If not provided, the subscription is for
                the entire record.
        """
        self._subscriptions.append((callback, path))

        if self._record is not None:
            self._record.subscribe(callback, path, True)

    def unsubscribe(self, callback, path=None):
        """
        Proxies the actual record's unsubscribe method.

        Args:
            callback (callable):
            path (str): a JSON path. If not provided, the subscription is for
                the entire record.
        """
        self._subscriptions.remove((callback, path))

        if self._record is not None:
            self._record.unsubscribe(callback, path)

    def _on_record_get(self, record):
        self._record = record

    @property
    def name(self):
        return self._name

    @name.setter
    @gen.coroutine
    def name(self, value):
        self._name = value

        if self._record is not None and not self._record.is_destroyed:
            for subscription in self._subscriptions:
                self._record.unsubscribe(*subscription)

            self._record.discard()

        record_future = self._record_handler.get_record(value)

        self._record = yield record_future

        for subscription in self._subscriptions:
            self._record.subscribe(*subscription, trigger_now=True)

        self._record.when_ready(partial(self.emit, "ready"))
        self.emit("nameChanged", value)

    def _proxy_method(self, method_name):
        method = partial(self._call_method_on_record, method_name)
        setattr(self, method_name, method)

    def _call_method_on_record(self, method_name, *args, **kwargs):
        if self._record is None:
            raise AttributeError(
                "Can't invoke {}. AnonymousRecord not initialised. "
                "Set `name` first.")

        getattr(self._record, method_name)(*args, **kwargs)
