import mrs

class Mrsstage1(mrs.MapReduce):
    def map(self,key, value):
        words = value.split()
        for word in words:
            yield (word.lower(),1)

    def reduce(self,key, values):
        yield sum(values)


if __name__ == '__main__':
    mrs.main(Mrsstage1)