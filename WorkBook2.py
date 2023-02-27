# second py file to perform some task on dataset
# avilable at:  https://archive.ics.uci.edu/ml/datasets/Contraceptive+Method+Choice

#https://github.com/astan54321/PA3/blob/44628868dcc7f00feec9e4c4bdb9391558391ac7/problem2_3.py

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

DATA_RE = re.compile(r"[\w.-]+")


class MRProb2_3(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_avgWifeAge,
                   reducer=self.reducer_get_avg)
        ]
    def mapper_get_avgWifeAge(self, _, line):
        # yield each wifes age
        # the first column is the wife's age

        data = DATA_RE.findall(line)
        wife_A = float(data[0])
        yield ("Wife's Age", wife_A)
    def reducer_get_avg(self, key, values):
        # get max of the petal widths
        size, total = 0, 0
        for val in values:
            size += 1
            total += val
        yield ("Wife's Average Age is: ", round(total,1) / size)
if __name__ == '__main__':
    MRProb2_3.run()
