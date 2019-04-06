from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import os
WORD_RE = re.compile(r"[\w']+")


class ReverseIndex(MRJob):

    def steps(self):##Multi-step 1) discard stop words and recombine. 2)
        return [
            MRStep(mapper=self.mapper_discard_stop, reducer=self.reducer_discard_stop),
            MRStep(mapper=self.mapper,
                    reducer=self.reducer)
        ]

    def mapper_discard_stop(self, _, line):
        stopWords=['to','at','the','we','am','in']
        for word in WORD_RE.findall(line):
            if(word not in stopWords):
                yield None, word.lower()

    def reducer_discard_stop(self, _, words):
        yield (None,' '.join(words))#remake a delimited list

    def mapper(self, _, line):
        fileName = os.environ['map_input_file']
        for word in WORD_RE.findall(line):
            yield word.lower(), fileName


    def reducer(self, word, fileNames):
        unique = list(set(fileNames))
        yield word, ','.join(unique)


if __name__ == '__main__':
    ReverseIndex.run()