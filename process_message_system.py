#!/usr/bin/python3
""" process_message_system.py : A module for erlang-style interprocess comms.

	===========================
	CompSci 340 - Assignment 1
	2016-08-19
	===========================
	Author:	Cam Smith
	UPI:	csmi928
	ID:		706899195
	===========================

	Imports

	Globals:
		-ANY
		-path

	Classes:
		-Message:
			-__init__
		-MessageProc:
			-__init__
			-start
			-main
			-cleanup
			-extract_from_pipe
			-give
			-open_target
			-receive
		-TimeOut:
			-__init__

"""

import atexit
import os
import pickle
import select
import sys
import threading
import time
import queue

ANY = None
path = "/tmp/cam_pipe"


class Message:
	""" Message is just a simple data structure class"""
	def __init__(self, message_type=None, action=None, guard=lambda: True):
		""" __init__ just takes care of simple assignments. """
		self.message_type = message_type
		self.action = action
		self.guard = guard


class MessageProc:

	def __init__(self):
		""" __init__ just establishes some instance variables to sensible
			defaults. """
		self._pid = os.getpid()
		self._path = path
		self._ischild = False
		self._childpid = None
		self._inpipe = None
		self._consumers = []
		self._messages = []
		self._queue = queue.Queue()
		self._receiving = False 


	def start(self, *args):
		""" Start is where our processes will fork. The parent simply returns
			the child's pid, which is also set to an instance variable.

			the child will cleanup some instance variables relating to pids
			and status as a child; then call main() on itself. """
		self._childpid = os.fork()

		if not self._childpid:
			self._ischild = True
			self._childpid = None
			self._pid = os.getpid()

			self.main(*args)
			sys.exit()	# once child process has run main, it can be terminated

		return self._childpid


	def main(self):
		""" main has the job of creating our _inpipe for this process and of
			spawning the hellbeast which will monitor it. main also registers
			the atexit function to be run on termination.

			Main is expected to be extended by users of this module. """
		self._path = path + str(self._pid)
		os.mkfifo(self._path)

		gather_thread = threading.Thread(target=self.extract_from_pipe, daemon=True)
		gather_thread.start()

		atexit.register(self.cleanup)

		return


	def cleanup(self):
		""" Removes the pipe created by this process. Will be registered via 
			atexit, so is run by this process automatically at sys.exit()"""
		try:
			os.unlink(path + str(os.getpid())) # re-make path
		except Exception as e:
			pass


	def extract_from_pipe(self):
		""" Opens the pipe that was created by this process and continually tries to
			extract a pickled object out of that pipe, should be a daemon thread
			
			This uses code written by Robert Sheehan which was provided in lecture 9 """
		with open(self._path,'rb') as pipe_rd:
			self._receiving = True
			while True:
				try:
					message= pickle.load(pipe_rd)
					self._queue.put(message)
				except EOFError: # no writer open yet
					time.sleep(0.001) # mercy for the cpu


	def give(self, consumer, message_type, data=None):
		""" Sends a message to a specified pipe, relies on open_target(),
			will silently return if writing fails, or open_target() fails. """
		target_pipe = self.open_target(consumer)
		if target_pipe == None:
			return # that pipe is forever closed

		message = pickle.dumps((message_type, data,))

		os.write(target_pipe, message)
		os.close(target_pipe) # never know when we do last write, so lets tidy up


	def open_target(self, consumer):
		""" Attempts to open a target pipe, returns file descriptor if successful,will
			wait for pipe to exist on first open, else it returns None. """
		target_path = path + str(consumer)

		# Wait till pipe is created at least once
		while consumer not in self._consumers: 		#hang around till pipe is created
			if os.path.exists(target_path):
				self._consumers.append(consumer) 	# we found the pipe
			time.sleep(0.01) # to avoid CPU hammering

		try:
			target_pipe = os.open(target_path, os.O_RDWR)
			return target_pipe
		except:
			return None # this will happen if a once-existing pipe suddenly
						# doesn't exist anymore


	def receive(self, *args):
		""" receive, the beast. receive is passed a pointer to an array of
			arguments of indefinite length. Out of *args receive must gather
			everything which is a Message object, and if present the first
			Timeout object which is present.

			Generally speaking, receive matches objects gathered by the 
			gatherer thread against Messages passed in *args, and if there is
			a match of the message type then an action is run, breaking receive
			from its loop.

			receive also has the task of checking if a Message object has a Guard,
			which is a lambda defining a predicate; and of executing a TimeOut
			action if no messages are recieved within a certain duration. """
		timeout = None
		start_time = None
		queries = []

		## Gather Messages and upto one TimeOut ##
		for argument in args:
			if type(argument).__name__ == "Message":
				queries.append(argument)
			elif type(argument).__name__ == "TimeOut":
				if timeout == None:
					timeout = argument
		## args has been parsed ##

		if timeout != None:
			start_time = time.time()

		while True:

			## move from queue _messages ##
			while not self._queue.empty():
				self._messages.append(self._queue.get())

			## iterate over messages looking for match 
			for message in self._messages:
				for query in queries:
					if query.message_type == message[0] or query.message_type == ANY:
						if query.guard():
							self._messages.remove(message)
							if query.action:
								if message[1] == None:		# if we have no data
									return query.action()   # sans-argument
								else:
									return query.action(message[1])	# otherwise we call with
							return

			## evaluate our timeout
			if timeout != None:
				if time.time() - start_time >= timeout._duration:
					return timeout._action()


class TimeOut:
	""" TimeOut is a simple data structure class. """
	def __init__(self, duration=0,action=lambda: None):
		""" __init__ handles basic assignments. """
		self._duration = duration
		self._action = action
