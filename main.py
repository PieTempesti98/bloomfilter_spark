import sys

import mmh3
from pyspark import SparkContext

import bloom_filter
import keycount
import math


def compute_k(fpr):
    return math.ceil(- math.log(fpr) / math.log(2))


p = 0
k = 0
m = None
partitions = 4  # number of mappers

master = "local"

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Please specify a false positive rate")
        sys.exit(-1)

    try:
        p = float(sys.argv[1])
    except ValueError:
        print("The false positive rate must be a float")

    # find k in function of p
    k = compute_k(p)

    sc = SparkContext(master, "BloomFilter")

    # import data   4 is the number of partitions
    text = sc.textFile("data/data.txt", partitions)

    # broadcast the starting variables for the bloom filter creation

    p_bloom = sc.broadcast(p)
    k_bloom = sc.broadcast(k)

    # cache partition RDD since it's going to be reused later
    text.persist()

    # Obtain the parsed dataset and the array of bloom filters' lengths
    text, m = keycount.compute_m(text, sc, p_bloom.value)

    m_bloom = sc.broadcast(m)

    # perform the mapping and the reducing of each partition
    map_output = text.map(lambda x: bloom_filter.mapper(x, m_bloom.value[int(x[1]) - 1], k_bloom.value))
    aggregate_positions = map_output.reduceByKey(lambda x, y: x + y)

    # aggregate the results for each split
    aggregate_positions = aggregate_positions.reduceByKey(lambda x, y: x + y)
    aggregate_positions = aggregate_positions.sortByKey(ascending=True)

    # compute each bloom filter
    bloom_filters = aggregate_positions.map(lambda x: bloom_filter.bloom_build(x, m_bloom.value[int(x[0]) - 1]))
    bloom_filters.saveAsTextFile("data/bloom_filters.txt")

    # testing

    print("\n\n***** test phase *****")
    filters = bloom_filters.values().collect()

    # print('\n\n')
    #
    # print("bloom filters " + str(filters))
    #
    # print('\n\n')
    false_positives = [0] * 10
    true_negatives = [0] * 10
    for row in text.collect():
        for i in range(len(filters)):
            positive = True
            for j in range(k):
                position = mmh3.hash(row[0], j, signed=False) % len(filters[i])
                if not filters[i][position] and i != row[1] - 1:  # true negative for the i-th filter
                    true_negatives[i] += 1
                    positive = False
                    break
            if positive and i != row[1] - 1:  # false positive for the i-th filter
                false_positives[i] += 1

    # compute the false positive rates
    fp_rates = []
    index = 1
    for fp, tn in zip(false_positives, true_negatives):
        fp_rates.append((index, float(fp) / (float(fp + tn))))
        index += 1

    print("\n\n***** results *****\n\n")
    for row in fp_rates:
        print("false positive rate of " + str(row[0]) + ": " + str(row[1]) + '\n')

    # save as text file
    fp_rdd = sc.parallelize(fp_rates)
    fp_rdd.saveAsTextFile("data/fprates")
