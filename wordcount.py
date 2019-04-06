from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import os
WORD_RE = re.compile(r"[\w']+")


class MRWordFreqCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_discard_stop, 
            reducer=self.reducer_discard_stop),
            MRStep(mapper=self.mapper,
                    combiner=self.combiner,
                    reducer=self.reducer)
        ]

    def mapper_discard_stop(self, _, line):
        stopWords=['to','at','the','we','am','in']
        for word in WORD_RE.findall(line):
            if(word not in stopWords):
                yield None, word.lower()

    def reducer_discard_stop(self, _, words):
        yield (None,' '.join(words))

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield word.lower(), 1

    def combiner(self, word, counts):
        yield word, sum(counts)

    def reducer(self, word, counts):
        yield word, sum(counts)


if __name__ == '__main__':
    MRWordFreqCount.run()