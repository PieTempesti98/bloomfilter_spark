import mmh3


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
        position = mmh3.hash(lines[0], i, signed=False) % m
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

