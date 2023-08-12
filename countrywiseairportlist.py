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
        month=date[5:7]
        airport = data[3].strip()
        state = data[7].strip()

        yield (country,airport,state), 1

    def reducer(self, key, list_of_values):
        yield key[0], (key[2],key[1],sum(list_of_values))


if __name__ == '__main__':
    MRmyjob.run()


        