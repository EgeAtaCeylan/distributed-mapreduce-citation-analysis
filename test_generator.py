"""
test_generator.py

Helper script for creating a smaller random test file from the large
Cit-HepPh citation dataset.

The original dataset is large, so this script samples a fixed number of random
line numbers and writes those selected edges into a smaller test file.
"""

from random import randint


# Number of random citation edges to copy from Cit-HepPh.txt.
numLines = 500
lineNums = []

# Generate unique random line numbers from the dataset range.
for i in range(numLines):
    newNum = randint(5, 421582)
    while newNum in lineNums:
        newNum = randint(5, 421582)
    lineNums.append(newNum)

# Stop reading the large dataset once the largest sampled line is passed.
maxElt = max(lineNums)

f = open("test_file" + str(numLines) + ".txt", "w")

myData = []
count = 0

# Copy only the sampled lines into the generated test file.
with open("Cit-HepPh.txt") as fp:
    for i, line in enumerate(fp):
        if i > maxElt:
            break

        for j in range(numLines):
            if i == lineNums[j]:
                count = count + 1
                print(count)
                f.write(line)

                # Store parsed integer edge data for optional debugging/inspection.
                elts = line.rstrip().split("\t")
                myData.append((int(elts[0]), int(elts[1])))

f.close()
