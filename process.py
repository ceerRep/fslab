#! /usr/bin/env python3

import sys

if __name__ == '__main__':
    print("__asm__(\\")
    for line in sys.stdin:
        line = '\t"' + line.strip().encode('unicode-escape').decode().replace('"', '\\"') + '\\n\\t"\\'
        print(line)
    print(");")
