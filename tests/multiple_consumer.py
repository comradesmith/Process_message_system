#!/usr/bin/python3
from process_message_system import *
import sys
import os
import time

class Buffer(MessageProc):

    def main(self, main_proc):
        super().main()
        self.main_proc = main_proc
        buffer_space = []

        while True:
            self.receive(
                Message(
                    'put',
                    action=lambda data: buffer_space.append(data)),
                Message(
                    'get',
                    guard=lambda: len(buffer_space) > 0,
                    action=lambda consumer: self.give(consumer, 'data', buffer_space.pop(0))),
                Message(
                    'stop',
                    guard=lambda: len(buffer_space) == 0,
                    action=self.finish))

    def finish(self):
        self.give(self.main_proc, 'finished')
        sys.exit()

class Consumer(MessageProc):

    def main(self, which, main_proc, buffer):
        super().main()
        self.which = which
        self.main_proc = main_proc
        self.count = 0
        while True:
            self.give(buffer, 'get', os.getpid())
            self.receive(
                Message(
                    'stop',
                    action=self.finish),
                Message(
                    ANY,
                    action=self.handle_input))

    def handle_input(self, data):
        print('{} from consumer {}'.format(data, self.which))
        self.count += 1

    def finish(self):
        self.give(self.main_proc, 'completed', self.count)
        sys.exit()

def add_to_total(n):
    global total
    total += n

if __name__=='__main__': # really do need this
    me = MessageProc()
    me.main()
    main_proc = os.getpid()
    buffer = Buffer().start(main_proc)

    consumers = []
    for i in range(10):
        consumer = Consumer().start(i, main_proc, buffer)
        consumers.append(consumer)

    for i in range(1000):
        me.give(buffer, 'put', i*i)
    me.give(buffer, 'stop')
    me.receive(
        Message(
            'finished'))

    total = 0

  
    for consumer in consumers:
        me.give(consumer, 'stop')
        me.receive(
            Message(
                'completed',
                action=add_to_total))

    print('The total number processed was', total)
