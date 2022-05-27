import sys
from pyspark import SparkContext
import mmh3

k = 7


def is_key_in_bloom(key, bloom_filter):

    for i in range(k):

        position = mmh3.hash(key, i, signed=False) % len(bloom_filters)
        value = bloom_filter[position]
        # if we have 0 as output we are sure the key is not in the bloom filter and we can return
        if value == 0:
            return 0

    return 1


if __name__ == "__main__":

    master = "local"

    if len(sys.argv) == 2:
        master = sys.argv[1]

    sc = SparkContext(master, "BloomFilter")
    bloom_filters = sc.textFile("data/bloom_filters.txt")

    with open("data/data_parsed.txt") as file:
        lines = file.readlines()
        false_positives = 0
        tot_positives = 0
        for line in lines:
            movie_id, rating = line.strip("()").split(",")
            cur_positives = 0
            for bloom in bloom_filters:
                cur_positives += is_key_in_bloom(movie_id, bloom)

            tot_positives += cur_positives

            # we know for sure that the number of true positive is 1
            cur_false_positive = cur_positives - 1
            false_positives += cur_false_positive

    print(false_positives / tot_positives * 100)
