"""
FindCitations.py

Concrete MapReduce job for counting paper citations.

Input format:
    citing_paper_id<TAB>cited_paper_id

For every edge A -> B, paper A cites paper B. The goal is to count incoming
edges for each paper, meaning the number of times each paper is cited.
"""

from MapReduce import MapReduce


class FindCitations(MapReduce):
    def Map(self, ownList):
        """
        Count citations inside one worker's input partition.

        ownList contains lines from the citation file. Each line has two IDs:
        the first ID is the citing paper and the second ID is the cited paper.
        This mapper builds a local dictionary:
            cited_paper_id -> citation_count_in_this_partition
        """
        localResult = {}

        for line in ownList:
            # Split by whitespace so both tabs and spaces are handled.
            info = line.split()

            # The second column is the paper receiving the citation.
            sitedArticle = info[1]

            # The first column is the paper that makes the citation.
            # It is parsed for clarity, although it is not needed for counting.
            siterArticele = info[0]

            # Increase the local citation count for the cited paper.
            if sitedArticle in localResult:
                localResult[sitedArticle] = localResult[sitedArticle] + 1
            else:
                localResult[sitedArticle] = 1

        return localResult

    def Reduce(self, results):
        """
        Merge citation-count dictionaries from all mapper workers.

        results is a list of dictionaries. Each dictionary contains local counts
        for one input partition. The reducer sums counts for matching paper IDs.
        """
        # Start from the first mapper result and merge the remaining dictionaries
        # into it to avoid creating an unnecessary extra dictionary.
        finalDict = results[0]

        for i in range(1, len(results)):
            currentDict = results[i]

            for key in currentDict:
                if key in finalDict:
                    finalDict[key] = finalDict[key] + currentDict[key]
                else:
                    finalDict[key] = currentDict[key]

        return finalDict
