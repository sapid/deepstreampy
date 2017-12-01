# WARNING

This is a WIP, being adapted from the forked source (a Tornado adapter) to work with Twisted. It DOES NOT WORK right now.

Current state: `src/protocol.py` initiates a connection

Outstanding priority issues:
* message_parser expects a client object to send errors to.
* heartbeat isn't handled yet; requires message_builder to be fixed first.
* auth needs to get sent.
* handler router initialization needs to be finished in factory init.

# deepstreampy
A Python client for deepstream.io for Twisted.

# Readme TODO: Build status, coverage status
* Travis-ci?
* Coveralls.io?
