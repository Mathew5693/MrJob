"""
Taken from
https://pythonhosted.org/mrjob/guides/quickstart.html#writing-your-first-job,
last access 11/13/2017
"""

# To test locally, from Terminal:
#    python anagrams.py --conf-path=mrjob.conf words_alpha.txt > anagrams.txt

# To run on AWS Elastic Map Reduce (EMR), from Terminal:
# You can first run a cluster that can be reused via:
#    mrjob create-cluster --conf-path=mrjob.conf
#    Watch for the cluster id starting with j-
#    Next run your job on the cluster using:
#        python anagrams.py -r emr --cluster-id=j-<CLUSTER ID> --conf-path=mrjob.conf words_alpha.txt > anagrams.txt
# When finished with the cluster you created, you can terminate it using:
#    mrjob terminate-cluster --conf-path=mrjob.conf j-<CLUSTER ID>

from mrjob.job import MRJob


class Anagrams(MRJob):

    def mapper(self, _, word):
        """
        Given a word, return a key (a string) with the letters of the word sorted, and a value that is the word
        For example, given food, it will return 'dfoo', 'food'
        :param word: the word
        :return: return a key (a string) with the letters of the word sorted, and a value that is the word
        """
        yield ''.join(sorted(word)), word

    def reducer(self, sorted_letters, anagrams):
        yield sorted_letters, list(anagrams)


if __name__ == '__main__':
    Anagrams.run()
