from pyspark import SparkContext, SparkConf
import math
import sys
import os


def find_m(n, p):
    """
    :param p: false positive rate
    :param n: number of keys in input to a bloom filter
    :return: dimension of the bloom filter
    """
    return n[0], math.ceil(- (float(n[1]) * math.log(p)) / (math.log(2) ** 2))


def parse_input_lines(line):
    """
    :param line:  < movie_id avg_rating num_votes>
    :return: tuple-> (movie_id, avg_rating)
    """
    splitted_line = line.split("\t")
    movie_id = splitted_line[0]
    # round rating to the nearest integer
    rating = float(splitted_line[1])
    if rating - math.floor(rating) < 0.5:
        avg_rating = round(rating)
    else:
        avg_rating = math.ceil(rating)
    return movie_id, avg_rating


def compute_m(text, p):

    """

    :param text: input data
    :param p: number of fpr
    :return: parsed input data, m
            record of parsed input data ->    <movie_id, rounded avg_counter>
            m -> array of 10 integers where 10 is number of possible ratings
    """
    parsed_input = text.map(parse_input_lines)

    ones = parsed_input.map(lambda w: (w[1], 1))
    counts = ones.reduceByKey(lambda x, y: x + y)
    print("******* lens *******\n\n\n" + str(counts.collect()) + "\n\n\n*******************")
    lens = counts.map(lambda n: find_m(n, p))

    # get an array built this way:
    # a[i] = mi
    # where i is the i-th bloom filter (we sort the array by key)
    # m is the size of the i-th bloom filter
    lens = lens.sortByKey(ascending=True)
    print("******* lens *******\n\n\n" + str(lens.collect()) + "\n\n\n*******************")

    m_values = lens.values().collect()
    return parsed_input, m_values


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


def mapper(lines, m, k):
    """

    :param lines: < movie_id avg_rating>
    :param m: int, length of the filter
    :param k: int, number of hash functions
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
        position = mmh3.hash(lines[0], i) % m
        value.append(position)
    # print("mapped the movie no." + lines[0])
    return key, value


def bloom_build(reducer_output, m):
    """

    :param reducer_output: ( avg_rating, list of all hash values )
    :param m: dimension of the bloom filter
    :return: newly built bloom filter
    """
    key = reducer_output[0]
    positions = reducer_output[1]
    bloom_filter = [False] * m

    for position in positions:
        bloom_filter[position] = True

    return key, bloom_filter

def compute_k(fpr):
    return math.ceil(- math.log(fpr) / math.log(2))


p = 0
k = 0
m = None
partitions = 4  # number of mappers

master = "yarn"

if __name__ == "__main__":

    os.environ['PYSPARK_PYTHON'] = './environment/bin/python'
    if len(sys.argv) != 4:
        print("Please specify the inputs as <input file> <output path> <false positive rate>")
        sys.exit(-1)

    try:
        p = float(sys.argv[3])
    except ValueError:
        print("The false positive rate must be a float")
        sys.exit(-1)

    if p <= 0 or p >= 1:
        print("The false positive rate must be between 0 and 1")
        sys.exit(-1)

    try:
        input_path = str(sys.argv[1])
    except ValueError:
        print("specify a valid input path")
        sys.exit(-1)

    try:
        output_path = str(sys.argv[2])
    except ValueError:
        print("specify a valid output path")
        sys.exit(-1)

    # find k in function of p
    k = compute_k(p)

    config = SparkConf()
    config.set('spark.archives', 'pyspark_venv.tar.gz#environment')
    sc = SparkContext(master, "BloomFilter", conf=config)
    import mmh3
    # import data   4 is the number of partitions
    text = sc.textFile(input_path, partitions)

    # broadcast the starting variables for the bloom filter creation

    p_bloom = sc.broadcast(p)
    k_bloom = sc.broadcast(k)

    # cache partition RDD since it's going to be reused later
    text.persist()

    # Obtain the parsed dataset and the array of bloom filters' lengths
    text, m = compute_m(text, p_bloom.value)

    m_bloom = sc.broadcast(m)

    # perform the mapping and the reducing of each partition
    map_output = text.map(lambda x: mapper(x, m_bloom.value[int(x[1]) - 1], k_bloom.value))
    aggregate_positions = map_output.reduceByKey(lambda x, y: x + y)

    # aggregate the results for each split
    aggregate_positions = aggregate_positions.sortByKey(ascending=True)

    # compute each bloom filter
    bloom_filters = aggregate_positions.map(lambda x: bloom_build(x, m_bloom.value[int(x[0]) - 1]))
    bloom_filters.saveAsTextFile(output_path + "/bloom_filters")

    # testing

    print("\n\n***** test phase *****")
    filters = bloom_filters.values().collect()

    false_positives = [0] * 10
    true_negatives = [0] * 10
    for row in text.collect():
        for i in range(len(filters)):
            positive = True
            for j in range(k):
                position = mmh3.hash(row[0], j) % len(filters[i])
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
        fp_rates.append((index, fp, float(fp) / (float(fp + tn))))
        index += 1

    print("\n\n***** results *****\n\n")
    for row in fp_rates:
        print("vote " + str(row[0]) + " --> false positives: " + str(row[1]) + ", false positive rate: " + str(row[2])
              + '\n')

    # save as text file
    fp_rdd = sc.parallelize(fp_rates)
    fp_rdd.saveAsTextFile(output_path + "/fp_rates")
