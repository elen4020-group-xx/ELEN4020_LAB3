from mrjob.job import MRJob
import re
import os
WORD_RE = re.compile(r"[\w']+")


class MRWordFreqCount(MRJob):

    def mapper(self, _, line):
        fileName = os.environ['map_input_file']
        for word in WORD_RE.findall(line):
            yield word.lower(), fileName


    def reducer(self, word, fileNames):
        unique = list(set(fileNames))
        yield word, ','.join(unique)


if __name__ == '__main__':
    MRWordFreqCount.run()