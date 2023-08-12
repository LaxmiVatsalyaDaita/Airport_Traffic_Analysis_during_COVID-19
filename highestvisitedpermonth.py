from mrjob.job import MRJob
from mrjob.step import MRStep

class MRmyjob(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper1, reducer=self.reducer1),
            MRStep(reducer=self.reducer2)
        ]
    def mapper1(self, _, line):
        #Split the line with tab separated fields
        data=line.split(',')

        country = data[9].strip()
        pob = int(data[4].strip())
        date = data[1].strip()
       
        airport = data[3].strip()
        month=date[5:7]

        yield (month,airport), 1

    def reducer1(self, key, list_of_values):
        yield key[0], (sum(list_of_values), key[1])
        
    def reducer2(self, key, list_of_values):
        yield key, max(list_of_values)

if __name__ == '__main__':
    MRmyjob.run()


        