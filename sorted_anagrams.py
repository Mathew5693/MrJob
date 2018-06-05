"""
Taken from
https://pythonhosted.org/mrjob/guides/quickstart.html#writing-your-first-job,
last access 11/13/2017
"""

# To test locally, from Terminal:
#    python sorted_anagrams.py --conf-path=mrjob.conf words_alpha.txt > sorted_anagrams.txt

# To run on AWS Elastic Map Reduce (EMR), from Terminal:
# You can first run a cluster that can be reused via:
#    mrjob create-cluster --conf-path=mrjob.conf
#    Watch for the cluster id starting with j-
#    Next run your job on the cluster using:
#        python sorted_anagrams.py -r emr --cluster-id=j-<CLUSTER ID> --conf-path=mrjob.conf words_alpha.txt > sorted_anagrams.txt
# When finished with the cluster you created, you can terminate it using:
#    mrjob terminate-cluster --conf-path=mrjob.conf j-<CLUSTER ID>

from mrjob.job import MRJob, MRStep


class SortedAnagrams(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_anagram,
                   reducer=self.reducer_anagram),
            MRStep(reducer=self.reducer_sorter)
        ]

    def mapper_anagram(self, _, word):
        """
        Given a word, return a key (a string) with the letters of the word sorted, and a value that is the word
        For example, given food, it will return 'dfoo', 'food'
        :param word: the word
        :return: return a key (a string) with the letters of the word sorted, and a value that is the word
        """
        yield ''.join(sorted(word)), word

    def reducer_anagram(self, sorted_letters, anagrams):
        # Returning None as key will ensure that
        # all (len, anagrams) pairs will be sent
        # to the same reducer
        anagram_list = list(anagrams)
        yield None, (len(anagram_list), anagram_list)

    def reducer_sorter(self, _, len_anagrams_pairs):
        # print(list(len_anagrams_pairs))
        for ap in sorted(len_anagrams_pairs, reverse=True):
            yield ap

if __name__ == '__main__':
    SortedAnagrams.run()
