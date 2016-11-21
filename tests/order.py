#!/usr/bin/python3
from process_message_system import *

class Ordered(MessageProc):

    def main(self):
        super().main()
        self.receive(
            Message(
                'go',
                action=lambda: print("GO!")))
        for i in range(2):
            self.receive(
                Message(
                    'two',
                    action=lambda: print('two')),
                Message(
                    'one',
                    action=lambda: print('one')))
        for i in range(2):
            self.receive(
                Message(
                    'four',
                    action=lambda: print('four')),
                Message(
                    'three',
                    action=lambda: print('three')))

if __name__=='__main__':
    me = MessageProc()
    me.main()
    ordered = Ordered().start()
    me.give(ordered, 'one')
    me.give(ordered, 'three')
    me.give(ordered, 'two')
    me.give(ordered, 'four')

    me.give(ordered, 'go')
