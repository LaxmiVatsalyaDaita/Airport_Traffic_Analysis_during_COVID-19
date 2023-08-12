from mrjob.job import MRJob
from mrjob.step import MRStep

class MRmyjob(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer)
        ]
    def mapper(self, _, line):
        #Split the line with tab separated fields
        data=line.split(',')

        country = data[9].strip()
        pob = int(data[4].strip())
        date = data[1].strip()
        month=int(date[5:7])
        airport = data[3].strip()

        if month<=3:
            yield ("Q1",country), pob
        elif month<=6:
            yield ("Q2",country), pob
        elif month<=9:
            yield ("Q3",country), pob
        else:
            yield ("Q4",country), pob
            

    def reducer(self, key, list_of_values):
        count = 0
        total = 0.0

        for x in list_of_values:
            total = total+x
            count=count+1

        avgLen = ("%.2f" % (total/count))
        yield key, avgLen



if __name__ == '__main__':
    MRmyjob.run()


        