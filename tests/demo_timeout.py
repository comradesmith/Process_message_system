#!/usr/bin/python3
from process_message_system import *

class Flusher(MessageProc):

    def main(self):
        super().main()
        print('before start message')
        self.receive(
            Message(
                'start',
                action=self.flush))
        print('after start message')
        self.receive(
            Message(
                ANY,
                action=lambda data:print('The first thing in the queue after the flush is', data)))

    def flush(self, *args):
        self.receive(
            Message(
                ANY,
                action=self.flush), # recursively call the flush method
            TimeOut(
                0, # old val 0
                action=lambda: None)) # when no more messages return

if __name__=='__main__':
    me = MessageProc()
    me.main()
    flusher = Flusher().start()
    # put a few things in the queue
    me.give(flusher, 'something', 'wow')
    me.give(flusher, 'something else', 'gosh')
    me.give(flusher, 'HHGTTG', 42)

    # then start the flush method
    me.give(flusher, 'start')

    time.sleep(1) #old val 1  # to give the flusher time to throw away existing messages

    me.give(flusher, 'last', ['this array', 1, 2, 3, 8])
