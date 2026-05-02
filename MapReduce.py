"""
MapReduce.py

This module defines the reusable MapReduce framework.
The framework uses Python multiprocessing to create separate producer,
consumer, and result collector processes. These processes communicate
through ZeroMQ PUSH/PULL sockets.

Subclasses only need to implement Map() and Reduce(). The framework handles:
1. Reading the input file.
2. Splitting the input into nearly equal parts.
3. Sending each part to exactly one worker.
4. Collecting partial results from all workers.
5. Calling Reduce() to build the final output.
"""

import os
import zmq
from multiprocessing import Process, freeze_support
from abc import ABC, abstractmethod


class MapReduce(ABC):
    # Kept from the original implementation. The actual multiprocessing entry
    # point is protected in main.py with if __name__ == '__main__'.
    __name__ = "__main__"

    def __init__(self, numWorkers):
        """
        Store the number of worker/mapper processes that will be created.

        Each worker receives one partition of the input list and returns one
        partial dictionary result.
        """
        self.numWorkers = numWorkers

    def start(self, fileName):
        """
        Start the complete MapReduce execution flow.

        The method reads the input file, creates all worker processes, starts
        the producer and result collector, and waits until every process ends.
        Clients are expected to call only this method; Producer, Consumer, and
        ResultCollector are internal framework methods.
        """
        # Read the whole file into memory. Each line becomes one MapReduce input
        # record, such as "paperA\tpaperB\n" for the citation dataset.
        currentFile = open(fileName, 'r')
        lines = currentFile.readlines()
        currentFile.close()

        allProcesses = []

        # Debug output showing worker indexes before process creation.
        for i in range(0, self.numWorkers):
            print(i)

        # Create all consumer/mapper processes first. This allows them to connect
        # to the ZeroMQ sockets before the producer sends work.
        for i in range(0, self.numWorkers):
            print(i)
            p = Process(target=self._Consumer, args=())
            p.start()
            allProcesses.append(p)

        # Create the producer process. It divides the input lines and pushes one
        # partition to each consumer.
        p = Process(target=self._Producer, args=(lines,))
        p.start()
        allProcesses.append(p)

        # Create the result collector/reducer process. It waits for all partial
        # results, calls Reduce(), and writes the final result to a file.
        p = Process(target=self._ResultCollector, args=())
        p.start()
        allProcesses.append(p)

        # Block the main process until all child processes finish.
        for process in allProcesses:
            process.join()

        print("Message from start all processes have finished")

        return 1

    def _Producer(self, lineList: list):
        """
        Split the input list and send one partition to each Consumer.

        ZeroMQ sockets used here:
        - PUSH socket on port 5560 sends input partitions to workers.
        - PULL socket on port 5559 receives readiness messages from workers.
        """
        context = zmq.Context()

        # Work distribution channel: Producer PUSH -> Consumer PULL.
        list_sender = context.socket(zmq.PUSH)
        list_sender.bind("tcp://127.0.0.1:5560")

        # Readiness channel: Consumer PUSH -> Producer PULL.
        # This prevents the producer from sending work before consumers are ready.
        info_Channel = context.socket(zmq.PULL)
        info_Channel.bind("tcp://127.0.0.1:5559")

        # Wait until every consumer sends an "I am ready" message.
        for i in range(0, self.numWorkers):
            info_Channel.recv_json()

        # Divide the input into almost equal chunks. The divmod-based slicing
        # guarantees that partition sizes differ by at most one element.
        k, m = divmod(len(lineList), self.numWorkers)
        linesDivied = (
            lineList[i * k + min(i, m):(i + 1) * k + min(i + 1, m)]
            for i in range(self.numWorkers)
        )

        # Send one chunk to each worker as a JSON-serializable dictionary.
        for line in linesDivied:
            print(line)
            list_sender.send_json({'list': line})

        return 1

    def _Consumer(self):
        """
        Receive one input partition, run Map(), and send the partial result.

        Each consumer is a mapper process. The returned value from Map() is
        expected to be a dictionary that can be serialized as JSON.
        """
        print('process id of consumer:', os.getpid())

        context = zmq.Context()

        # Receive work from the producer.
        list_receiver = context.socket(zmq.PULL)
        list_receiver.connect("tcp://127.0.0.1:5560")

        # Notify the producer that this consumer is connected and ready.
        info_Channel = context.socket(zmq.PUSH)
        info_Channel.connect("tcp://127.0.0.1:5559")

        # Send the mapper's partial result to the result collector.
        result_sender = context.socket(zmq.PUSH)
        result_sender.connect("tcp://127.0.0.1:5561")

        info_Channel.send_json({'message': "I am ready"})

        # Pull exactly one partition from the producer.
        result = list_receiver.recv_json()
        ownList = result['list']

        # Delegate job-specific mapping logic to the subclass implementation.
        localResult = self.Map(ownList)

        # Send the partial dictionary to the reducer/result collector.
        result_sender.send_json({'result': localResult})

        return 1

    def _ResultCollector(self):
        """
        Collect partial dictionaries from all consumers and run Reduce().

        The final result is written to output.txt in the current working
        directory. The PA specification mentions results.txt, but this submitted
        implementation uses output.txt.
        """
        print('process id of RESULT COLLECTOR:', os.getpid())

        context = zmq.Context()

        # Result channel: Consumer PUSH -> ResultCollector PULL.
        result_sender = context.socket(zmq.PULL)
        result_sender.bind("tcp://127.0.0.1:5561")

        allResults = []

        # Receive one partial result from each worker process.
        for i in range(0, self.numWorkers):
            currentResult = result_sender.recv_json()
            currentResult = currentResult['result']
            allResults.append(currentResult)

        # Delegate job-specific reduce logic to the subclass implementation.
        finalResult = self.Reduce(allResults)

        # Store the final dictionary/string representation in an output file.
        finalResultString = str(finalResult)
        print(finalResultString)
        f = open("results.txt", "w")
        f.write(finalResultString)
        f.close()

        return 1

    @abstractmethod
    def Map(self):
        """
        Abstract map function.

        Subclasses must override this method and return a partial result
        dictionary for their assigned input partition.
        """
        pass

    @abstractmethod
    def Reduce(self):
        """
        Abstract reduce function.

        Subclasses must override this method and merge all partial dictionaries
        into the final result.
        """
        pass
