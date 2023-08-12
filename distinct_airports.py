
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRmyjob(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,reducer=self.reducer)
        ]

    def mapper (self, _, line):
        data=line.split(',')
    
        am = data[0].strip()
        date = data[1].strip()
        version = data[2].strip()
        airportName = data[3].strip()
        pob = data[4].strip()    
        centroid = data[5].strip()
        city = data[6].strip()
        state = data[7].strip()
        iso = data[8].strip()
        country = data[9].strip()
        geography = data[10].strip()
    
        yield airportName, None
    
    def reducer(self, key, list_of_values):
        yield key, None
        

if __name__=='__main__':
    MRmyjob.run()   

    

