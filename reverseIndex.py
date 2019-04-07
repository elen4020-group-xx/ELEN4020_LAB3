from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import os
import time
WORD_RE = re.compile(r"[\w']+")

class CustomProtocol(object):###Custom input protocol class that is used to workaround the lack of direct file access    
    lineCount=0#static line counter workaround

    def read(self, line):
        decoded=line.decode('utf_8')
        CustomProtocol.lineCount=CustomProtocol.lineCount+1
        return (CustomProtocol.lineCount),decoded

    def write(self, key, value):
        return '%s\t%s' % (key, value)



class ReverseIndex(MRJob):
    INPUT_PROTOCOL=CustomProtocol

    def steps(self):##Multi-step 1) discard stop words and recombine. 2)map words to line numbers
        return [
            MRStep(mapper=self.mapper_discard_stop, reducer=self.reducer_discard_stop),
            MRStep(mapper=self.mapper,
                    reducer=self.reducer)
        ]

    def mapper_discard_stop(self,key, line):
        stopWords=['to','at','the','we','am','in']
        for word in WORD_RE.findall(line):
            if(word.lower() not in stopWords):
                yield key, word.lower()


    def reducer_discard_stop(self, key, words):
        yield (key,' '.join(words))#remake a list without stopwords
        ##prevents the devolution to a single mapper in the next step

    def mapper(self, key, line):
        for word in WORD_RE.findall(line):
            yield word, key


    def reducer(self, word, lineNos):
        unique = set(lineNos)
        if(len(unique)>1):
            outStr=''.join(str(unique))
        else:
            outStr=str(unique)
        yield word, outStr



if __name__ == '__main__':
    startTime=time.time()
    ReverseIndex.run()
    print("Time taken:", time.time()-startTime )