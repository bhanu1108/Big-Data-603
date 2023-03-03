# Date of Birth : 08-11-2000
# Import necessary libraries
from mrjob.job import MRJob
import re

# Regular expression for finding words in a string
WORD_PATTERN = re.compile(r"\w+")

# Define a class for counting words
class UniqueWordCount(MRJob):
    
    # define the mapper function that reads each line.
    def mapper(self, _, line):
        for word in WORD_PATTERN.findall(line):
            # Yield each word
            yield word.lower(), 1
    
    # define reducer function that receives a word and a list of counts for that word.
    def reducer(self, word, values):
        # Sum the counts for each word and emit the result
        total_count = sum(values)
        yield word, total_count
        
# Run  class 
if __name__ == '__main__':
    UniqueWordCount.run()
