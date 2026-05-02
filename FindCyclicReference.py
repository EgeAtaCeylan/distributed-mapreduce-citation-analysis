"""
FindCyclicReference.py

Concrete MapReduce job for detecting cyclic references of length 2.

A two-paper cycle exists when:
    paper A cites paper B
    paper B cites paper A

The result dictionary stores each detected pair once with value 1.
"""

from MapReduce import MapReduce


class FindCyclicReference(MapReduce):
    def Map(self, ownList) -> dict:
        """
        Convert directed citation edges into normalized pair keys.

        For every edge A -> B, this mapper creates a pair key containing both
        paper IDs. If the reverse edge B -> A exists anywhere in the dataset,
        the same normalized key will appear twice after all mapper results are
        merged. A count greater than 1 therefore means a length-2 cycle exists.
        """
        localResult = {}

        for line in ownList:
            # Split the citation edge into source/citing paper and target/cited paper.
            info = line.split()
            sitedArticle = info[1]
            siterArticle = info[0]

            # Normalize the pair key so A->B and B->A generate the same key.
            # The IDs are compared as strings in this implementation, but the
            # resulting key is still consistent for detecting reciprocal edges.
            if siterArticle > sitedArticle:
                currentTuple = "(" + str(sitedArticle) + ", " + str(siterArticle) + ")"
            else:
                currentTuple = "(" + str(siterArticle) + ", " + str(sitedArticle) + ")"

            # Count how many times this unordered pair appears in this partition.
            if currentTuple in localResult:
                localResult[currentTuple] = localResult[currentTuple] + 1
            else:
                localResult[currentTuple] = 1

        return localResult

    def Reduce(self, results):
        """
        Merge pair counts and keep only pairs that appear more than once.

        Since the assignment states that duplicate edges do not exist, a pair
        count greater than 1 means both A->B and B->A were present.
        """
        # Merge all mapper dictionaries into one global pair-count dictionary.
        finalDict = results[0]

        for i in range(1, len(results)):
            currentDict = results[i]

            for currentTuple in currentDict:
                if currentTuple in finalDict:
                    finalDict[currentTuple] = finalDict[currentTuple] + currentDict[currentTuple]
                else:
                    finalDict[currentTuple] = currentDict[currentTuple]

        # Store only cyclic pairs. The required output value is always 1.
        cycleDict = {}

        for currentTuple in finalDict:
            if finalDict[currentTuple] > 1:
                cycleDict[currentTuple] = 1

        return cycleDict
