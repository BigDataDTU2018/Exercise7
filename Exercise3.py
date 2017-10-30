from mrjob.job import MRJob
from mrjob.step import MRStep

class Patients(MRJob):

    # As we have 2 reducer we have to define the steps order we want
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer_find_max_month)
        ]

    # Maps the name of the patient for each month he has checked in
    def mapper(self, _, line):
        Patient = line.split(',')[0]
        for month in line.split(',')[1:]:
            yield month, Patient

    def reducer(self,month,Patient):
        # num_occurrences is so we can easily use Python's max() function.
        yield None, (int(len(list(Patient))), "times in month number " + month)

    # Gives us the key of the highest value
    def reducer_find_max_month(self, _, month_count_pairs):
        yield max(month_count_pairs)

if __name__ == '__main__':

    Patients.run()