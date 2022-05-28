import sys
from pyspark import SparkContext

import bloom_filter
import keycount

p = 0.01
k = 7
m = None
partitions = 4  # number of mappers


if __name__ == "__main__":

    master = "local"

    if len(sys.argv) == 2:
        master = sys.argv[1]

    sc = SparkContext(master, "BloomFilter")

    # import data
    text = sc.textFile("data/data.txt")

    # Obtain the parsed dataset and the array of bloom filters' lengths
    text, m = keycount.compute_m(text, sc)

    # split the input in partitions
    count = text.count()
    weights = [count/partitions] * partitions
    splits = text.randomSplit(weights)

    split_positions = []

    # perform the mapping and the reducing of each partition
    for split in splits:
        parsed_text = split.map(bloom_filter.parse_input_lines)
        map_output = parsed_text.map(lambda x: bloom_filter.mapper(x, m[x[0]], k))
        aggregate_positions = map_output.reduceByKey(lambda x, y: x + y)
        split_positions.append(aggregate_positions)

    # aggregate the results for each split
    aggregate_positions = split_positions[0]
    for i in range(1, len(split_positions)):
        aggregate_positions.union(split_positions[i])
    aggregate_positions = aggregate_positions.reduceByKey(lambda x, y: x + y)
    aggregate_positions = aggregate_positions.sortByKey(ascending=True)

    # compute each bloom filter
    bloom_filters = aggregate_positions.map(bloom_filter.bloom_build)

    print(bloom_filters.collect())
    bloom_filters.saveAsTextFile("data/bloom_filters.txt")
