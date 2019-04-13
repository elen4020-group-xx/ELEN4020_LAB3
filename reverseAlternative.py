from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import os
import time
import heapq
from utility import stopWords
WORD_RE = re.compile(r"[\w']+")


class ReverseIndex(MRJob):
    def steps(self):##Multi-step 1) discard stop words and recombine. 2)map words to line numbers
        return [
            MRStep(mapper=self.mapper_discard_stop, reducer=self.reducer_discard_stop),
            MRStep(mapper=self.mapper,
                    reducer=self.reducer),
            MRStep(reducer=self.topKReducer)
        ]

    def mapper_discard_stop(self,_, line):
        wordList = WORD_RE.findall(line)
        for word in wordList:
            if(word.lower() not in stopWords):
                yield int(wordList[0]), word.lower()


    def reducer_discard_stop(self, key, words):
        yield (key,' '.join(words))#remake a list without stopwords
        ##prevents the devolution to a single mapper in the next step

    def mapper(self, key, line):
        for word in WORD_RE.findall(line):
            yield word, key


    def reducer(self, word, lineNos):
        unique = set(lineNos)
        if(len(unique)>1):
            outStr=str(unique)
        else:
            outStr=str(unique)
 
        yield None, (word,outStr)

    def topKReducer(self, _, word_line_pairs):
        yield ('Top'+str(50),heapq.nlargest(50, word_line_pairs,key=lambda tup: tup[1].count(',')))





if __name__ == '__main__':
    startTime=time.time()
    ReverseIndex.run()
    print("Time taken:", time.time()-startTime )