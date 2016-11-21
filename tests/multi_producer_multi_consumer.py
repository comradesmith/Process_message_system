#!/usr/bin/python3
from process_message_system import *

import time

class Buffer(MessageProc):

    def main(self, main_proc):
        '''Handle all data transfers from the producers to the consumers.
        
        When a producer has completed it sends a 'stop' message to the buffer.
        After every 'stop' message is received, the buffer begins waiting for 2
        seconds, to see if any more data arrives.
        If none does and all data has been consumed, it terminates, after
        sending 'stop' messages to any waiting consumers.
        '''
        super().main()
        self.buffer_space = []
        active = self.normal
        while active != 'stop':
            active = active()
        self.give(main_proc, 'buffer_finished')

    def put(self, data):
        self.buffer_space.append(data)
        return self.normal

    def get(self, consumer):
        self.give(consumer, 'data', self.buffer_space.pop(0))
        return self.normal

    def normal(self):
        return self.receive(
            Message(
                'put',
                action=self.put),
            Message(
                'get',
                guard=lambda: len(self.buffer_space) > 0,
                action=self.get),
            Message(
                'stop',
                guard=lambda: len(self.buffer_space) == 0,
                action=lambda: self.after_stop))

    def after_stop(self):
        print('after_stop called')
        return self.receive(
            TimeOut(
                2,
                action=lambda: self.signal_consumers),
            Message(
                'put',
                action=self.put))

    def signal_consumers(self):
        return self.receive(
            Message(
                'get',
                action=self.goodbye),
            TimeOut(
                0,
                action=lambda: 'stop'))

    def goodbye(self, consumer):
        self.give(consumer, 'stop')
        return self.signal_consumers

class Producer(MessageProc):

    def main(self, buffer):
        super().main()
        for i in range(1, 1001):
            self.give(buffer, 'put', i)
        self.give(buffer, 'stop')

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
        self.count += 1

    def finish(self):
        print('Consumer', self.which, 'received', self.count, 'values.')
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

    for i in range(10):
        Producer().start(buffer)

    time.sleep(1)
    Producer().start(buffer)

    me.receive(
        Message(
            'buffer_finished'))
    total = 0
    for consumer in consumers:
        me.receive(
            Message(
                'completed',
                action=add_to_total))

    print('The total number processed was', total)
