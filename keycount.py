import math

p = 0.01


def find_m(n):
    """

    :param n: number of keys in input to a bloom filter
    :return: dimension of the bloom filter
    """
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


def compute_m(text, sc):
    parsed_input = text.map(parse_input_lines)

    # export the already cleaned dataset
    parsed_input.saveAsTextFile("data/data_parsed.txt")

    ones = parsed_input.map(lambda w: (w, 1))
    counts = ones.reduceByKey(lambda x, y: x + y)
    lens = counts.map(lambda n: find_m(n))
    counts.saveAsTextFile("data/lens.txt")

    # get an array built this way:
    # a[i] = mi
    # where i is the i-th bloom filter (we sort the array by key)
    # m is the size of the i-th array
    lens.sortByKey(ascending=True)

    # broadcast data so it is accessible by all workers
    values = lens.values()
    return parsed_input, values
