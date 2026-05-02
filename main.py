"""
main.py

Command-line entry point for the program.

Usage examples:
    python main.py COUNT 4 test01.txt
    python main.py CYCLE 3 test02.txt

Arguments:
    1. Operation type: COUNT or CYCLE
    2. Number of worker/mapper processes
    3. Input file path
"""

from FindCitations import FindCitations
from FindCyclicReference import FindCyclicReference
import sys


if __name__ == '__main__':
    # Read command-line arguments in the order required by the assignment.
    operation = sys.argv[1]
    processCount = int(sys.argv[2])
    file = sys.argv[3]

    # Select the correct MapReduce subclass based on the requested operation.
    if operation == "CYCLE":
        mp = FindCyclicReference(processCount)
        mp.start(file)

    elif operation == "COUNT":
        mp = FindCitations(processCount)
        mp.start(file)

    else:
        print("UNSUPPORTED OPERATION USE CYCLE OR COUNT")
