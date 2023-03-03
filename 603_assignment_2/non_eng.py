from mrjob.job import MRJob
import enchant

# create a dictionary for US English words
d = enchant.Dict("en_US")

class NonEnglishWordsCount(MRJob):
    
    # the mapper function reads each line and splits it into word
    def mapper(self, _, line):
        for word in line.split():
            if not d.check(word):
                yield word.lower(), 1
    
    # the reducer function receives a word and a list of counts for that word
    def reducer(self, word, counts):
        yield word, sum(counts)
        
if __name__ == '__main__':
    NonEnglishWordsCount.run()
