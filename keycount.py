import math


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
