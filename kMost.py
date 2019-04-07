from mrjob.job import MRJob
from mrjob.step import MRStep
from reverseIndex import CustomProtocol
import re
import time
import heapq
import os
WORD_RE = re.compile(r"[\w']+")


class KMost(MRJob):
    INPUT_PROTOCOL=CustomProtocol

    def steps(self):##Multi-step 1) discard stop words and recombine. 2)count words. 3) Find K largest words
        return [
            MRStep(mapper=self.mapper_discard_stop, reducer=self.reducer_discard_stop),
            MRStep(mapper=self.mapper_count_words,
                    combiner=self.combiner_count_words,
                    reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_kmax)
        ]

    def mapper_discard_stop(self, key, line):
        stopWords=['to','at','the','we','am','in']
        for word in WORD_RE.findall(line):
            if(word.lower() not in stopWords):
                yield key, word.lower()

    def reducer_discard_stop(self, key, words):
        yield (key,' '.join(words))#remake a delimited list
        ##even though line numbers are not important, this prevents a devolution to a single map in the next step

    def mapper_count_words(self, _, line):
        for word in WORD_RE.findall(line):
            yield word.lower(), 1

    def combiner_count_words(self, word, counts):
        yield word, sum(counts)

    def reducer_count_words(self, word, counts):
        yield None, (word,sum(counts))

    def reducer_find_kmax(self, _, word_count_pairs):#using heapq builtin for n largest
        yield ('Top'+str(self.options.maxNo),heapq.nlargest(self.options.maxNo, word_count_pairs,key=lambda tup: tup[1]))
    
    def configure_args(self):## extra CL arg for K value
        super(KMost, self).configure_args()
        self.add_passthru_arg(
        '--maxNo', type=int, default=10, help='...')

if __name__ == '__main__':
    startTime=time.time()
    KMost.run()
    print("Time taken:", time.time()-startTime )