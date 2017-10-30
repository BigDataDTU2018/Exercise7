from mrjob.job import MRJob
import re

class Patients(MRJob):
    def mapper(self, _, line):
        Patient = line.split(',')[0]
        for month in line.split(',')[1:]:
            yield int(month),Patient
    def reducer(self,month,Patient):
            yield str(month),int(len(list(Patient)))

if __name__ == '__main__':

    Patients.run()