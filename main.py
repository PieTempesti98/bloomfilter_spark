import sys
from pyspark import SparkContext
import mmh3
import math
import numpy as np

p = 0.01
k = 7
n = [2538, 6620, 17874, 43500, 102083, 218713, 369807, 352641, 112845, 16559]


def m(n):
    return round(- (n * math.log(p)) / (math.log(2) ** 2))


def parse_input_lines(line):
    """

    :param line:  < movie_id avg_rating num_votes>
    :return: tuple-> (movie_id, avg_rating)
    """

    splitted_line = line.split("\t")
    movie_id = splitted_line[0]
    # round rating to the nearest integer
    avg_rating = int(round(float(splitted_line[1])))
    return movie_id, avg_rating


def mapper(lines):
    """

    :param lines: < movie_id avg_rating>
    lines[0] -> movie_id
    lines[1] -> avg_rating
    :return: tuple -> (movie_id , a list of positions in the bloom filter)
    """

    key = lines[1]
    value = []
    for i in range(k):
        # we use a family of murmur hash map to compute the hash of the movie_id which is our key
        # we change the seed to each hash function
        # we need in output an integer ranged from 0 to m-1 therefore we need to calculate the module
        position = mmh3.hash(lines[0], i, signed=False) % m(n[key - 1])
        value.append(position)

    return key, value


def bloom_build(reducer_output):
    """

    :param reducer_output: ( avg_rating, list of all hash values )
    :return: newly built bloom filter
    """
    key = reducer_output[0]
    positions = reducer_output[1]
    bloom_filter = np.zeros(m(n[key - 1]))

    for position in positions:
        bloom_filter[position] = 1

    return key, bloom_filter


if __name__ == "__main__":

    master = "local"

    if len(sys.argv) == 2:
        master = sys.argv[1]

    sc = SparkContext(master, "BloomFilter")
    text = sc.textFile("data/data.txt")

    parsed_text = text.map(parse_input_lines)
    map_output = parsed_text.map(mapper)

    aggregate_positions = map_output.reduceByKey(lambda x, y: x + y)
    bloom_filters = aggregate_positions.map(bloom_build)

    print(bloom_filters.collect())
    bloom_filters.saveAsTextFile("data/bloom_filters.txt")
