from mrjob.job import MRJob
import re

class Euler_tour(MRJob):
   oddEdges = 0

   def mapper(self,key,line):
       for edge in line.split():
           yield edge,1

   def reducer(self, key, values):
       #count all the edges from the same node
       v = [v for v in values]
	#If the node has odd number of edges we add it to oddEdges
       if sum(v)%2!=0:
           Euler_tour.oddEdges += 1
       yield key, sum(v)

if __name__ == '__main__':

   Euler_tour.run()

   if (Euler_tour.oddEdges>2):
       print "The graph doesn't have Euler Tour"