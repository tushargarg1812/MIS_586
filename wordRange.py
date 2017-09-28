#This code gives a range of words in a file based on its number of occurances.

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[\w']+")

class MRMostUsedWord(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words),
            MRStep(mapper=self.mapper_filter_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_sort_word)
        ]

    def mapper_get_words(self, _, line):
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)

    def mapper_filter_words(self, word, counts):
        if word.startswith("co"):
            yield word, counts

    def combiner_count_words(self, word, counts):
        # optimization: sum the words we've seen so far
        yield (word, sum(counts))

    def reducer_count_words(self, word, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.
        yield None, (word, sum(counts))

    # discard the key; it is just None
    def reducer_sort_word(self, _, word_count_pairs):
        L = sorted(word_count_pairs, key = lambda k: k[1] , reverse = True)
        return L[4:10]

if __name__ == '__main__':
    MRMostUsedWord.run()
