#!/usr/bin/python3

from process_message_system import *
import sys

class Return(MessageProc):

    def main(self):
        super().main()
        value = self.receive(
            Message(
                'two',
                action=lambda: time.sleep(10)),
            Message(
                'hi',
                action=lambda: 2 * 2))
       	print(value)


if __name__=='__main__': # really do need this
    me = MessageProc()
    me.main()
    example = Return().start()
    me.give(example, 'hi')
