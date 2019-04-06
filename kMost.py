from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import heapq
import os
WORD_RE = re.compile(r"[\w']+")


class KMost(MRJob):

    
    def steps(self):##Multi-step 1) discard stop words and recombine. 2)count words. 3) Find K largest words
        return [
            MRStep(mapper=self.mapper_discard_stop, reducer=self.reducer_discard_stop),
            MRStep(mapper=self.mapper_count_words,
                    combiner=self.combiner_count_words,
                    reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_kmax)
        ]

    def mapper_discard_stop(self, _, line):
        stopWords=['to','at','the','we','am','in']
        for word in WORD_RE.findall(line):
            if(word not in stopWords):
                yield None, word.lower()

    def reducer_discard_stop(self, _, words):
        yield (None,' '.join(words))#remake a delimited list

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
    KMost.run()