# Bloom Filter - Spark

Implementation of a Bloom Filter using the Spark framework in Python. All the documentation for this project can be found [here](./documentation)

## How to run the algorithm

` spark-submit bloomfilter_spark/main.py <input path> <output path> <false positive rate>`

## The input file

We have tested the bloom filter with the *title.ratings* IMDb dataset (available [here](https://datasets.imdbws.com/title.ratings.tsv.gz)).
The dataset is a `.tsv` file, with an header line containing the structure of the data: 

`tconst averageRating   numVotes`

We transform this file in a `.txt` file without the header.

<pre><code>tt0000001	5.7	1882
tt0000002	5.9	250
tt0000003	6.5	1663
tt0000004	5.8	163
tt0000005	6.2	2487
tt0000006	5.2	166
tt0000007	5.4	773
tt0000008	5.4	2024
tt0000009	5.3	194
tt0000010	6.9	6803
tt0000011	5.3	346
tt0000012	7.4	11692
tt0000013	5.7	1801
...             ...     ...
</code></pre>

In the setup phase, in which we compute m for each bloom filter, we also pre-process this dataset, removing the `numVotes` column and rounding the values in the `averageRating` column.
You can find the `data.txt` file (and a reduced version of the same file used for testing purposes `data1.txt`) in [this folder](./data).

## Outputs
The algorithm generates two output folders: `bloom_filters`, with a bloom filter for each vote, and `fp_rates`, with the number of false positives and the false positive rate computed for each bloom filter.

You can find these folders inside the `<output path>` specified as input parameter; for example, if you specify `output` as output path, the output structure will be:

<pre><code>output
| 
|_ bloom_filters
|
|_ fp_rates
</code></pre>