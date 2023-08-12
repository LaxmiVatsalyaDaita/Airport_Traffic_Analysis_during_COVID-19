from mrjob.job import MRJob
from mrjob.step import MRStep

class MRmyjob(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper1, reducer=self.reducer1)
        ]
    def mapper1(self, _, line):
        #Split the line with tab separated fields
        data=line.split(',')

        country = data[9].strip()
        pob = int(data[4].strip())
        date = data[1].strip()
        month=date[5:7]
        airport = data[3].strip()

        yield (month,country), pob
            

    def reducer1(self, key, list_of_values):
        count = 0
        total = 0.0

        for x in list_of_values:
            total = total+x
            count=count+1

        avgLen = ("%.2f" % (total/count))
        yield key, avgLen



if __name__ == '__main__':
    MRmyjob.run()
