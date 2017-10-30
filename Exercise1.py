from mrjob.job import MRJob
import re

# Gets all alphanumeric character and the underscore, equivalent to the set [a-zA-Z0-9_]
regex = re.compile(r"[\w']+")

class MRWordFreqCount(MRJob):

   #For every word in each line we create (horse, 1), the word and count 1
   def mapper(self, _, line):
       for word in regex.findall(line):
           yield word.lower(), 1

   # For every word we sum the counts together (horse, 3), the text has 3 hi in it
   def reducer(self, word, counts):
       yield word, sum(counts)

if __name__ == '__main__':
   MRWordFreqCount.run()