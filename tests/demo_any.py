#!/usr/bin/python3
from process_message_system import *
import sys

class Any(MessageProc):

	def main(self):
		super().main()
		self.receive(
		  Message(
				'one',
				action=self.a),
			Message(
				'four',
				action=self.b),
			Message(
				ANY,
				action=self.c))

	def a(self):
		print('a')

	def b(self):
		print('b')

	def c(self):
		print('c')

if __name__=='__main__':
	me = MessageProc()
	me.main()
	example = Any().start()
	me.give(example, 'hi')
