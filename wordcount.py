from mrjob.job import MRJob
from mrjob.step import MRStep
from reverseIndex import CustomProtocol
import re
import os
import time

WORD_RE = re.compile(r"[\w']+")


class MRWordFreqCount(MRJob):
    #INPUT_PROTOCOL=CustomProtocol


    def steps(self):
        return [
            MRStep(mapper=self.mapper_discard_stop, 
            reducer=self.reducer_discard_stop),
            MRStep(mapper=self.mapper,
                    combiner=self.combiner,
                    reducer=self.reducer)
        ]

    def mapper_discard_stop(self, key, line):
        stopWords=['to','at','the','we','am','in']
        for word in WORD_RE.findall(line):
            if(word.lower() not in stopWords):
                yield key, word.lower()

    def reducer_discard_stop(self, key, words):
        yield (key,' '.join(words))#even though line numbers are not important, this prevents a devolution to a single map in the next step

    def mapper(self, _, line):#line no is no longer relevant here
        for word in WORD_RE.findall(line):
            yield word,1

    def combiner(self, word, counts):
        yield word, sum(counts)

    def reducer(self, word, counts):
        yield word, sum(counts)


if __name__ == '__main__':
    startTime= time.time() 
    MRWordFreqCount.run()
    print("Time taken:", time.time()-startTime )