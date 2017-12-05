# WARNING

This is a WIP, being adapted from the forked source (a Tornado adapter) to work with Twisted. It DOES NOT WORK right now.

Current state: `src/protocol.py` initiates a connection and authenticates.

THIS BRANCH MAY BE FORCE-PUSHED AT ANY TIME.

I intend to clean up the commit history and the structure of this repository.

Outstanding priority issues:
* handler router initialization needs to be finished in factory init.
* server still dropping connection :\ 

# deepstreampy
A Python client for deepstream.io for Twisted.

# Readme TODO: Build status, coverage status
* Travis-ci?
* Coveralls.io?
