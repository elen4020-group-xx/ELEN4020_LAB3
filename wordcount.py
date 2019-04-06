import mrs

class MrsProgram(mrs.MapReduce):
    def map(self,key, value):
        words = value.split()
        for word in words:
            yield (word,1)

    def reduce(kself,key, values):
        yield sum(values)

if __name__ == '__main__':
    mrs.main(MrsProgram)