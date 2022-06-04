# Bloom Filter - Spark

Implementation of a Bloom Filter using the Spark framework in Python. All the documentation for this project can be found [here](./documentation)

## How to run the algorithm

#### Note: execute all the commands below inside the *bloomfilter_spark* folder

### Pre-installation

In order to run the algorithm on a cluster, you have to install the same python environment on all the cluster's machines.

These are the steps to create a virtual environment with all the needed packages:

1. *Only on debian/ubuntu* install python venv
    
    `sudo apt-get install python3-venv`

2. create a virtual environment and activate it

   <pre><code>python -m venv pyspark_venv
   source pyspark_venv/bin/activate
   </code></pre>

3. install all the needed dependencies using *pip*

    `pip3 install pyspark mmh3`

4. zip the virtual environment in a *.tar.gz* archive

    `venv-pack -o pyspark_venv.tar.gz`

### Spark submission

<pre><code>export PYSPARK_PYTHON=./environment/bin/python
spark-submit --archives pyspark_venv.tar.gz#environment main.py [input path] [output path] [false positive rate] 
</code></pre>

#### spark-submit example
`spark-submit --archives pyspark_venv.tar.gz#environment main.py data/data.txt output 0.01`

##### Note: remember to delete the output folder before  the next execution using `hadoop fs -rm -r [output path]` 

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

### Output printed by the application
This is an example of the output printed by the execution of the application, using 0.01 as p value
<pre><code>***** results *****


vote 1 --> false positives: 12481, false positive rate: 0.01002832292147922

vote 2 --> false positives: 12872, false positive rate: 0.010385712383864707

vote 3 --> false positives: 12228, false positive rate: 0.009938215314994612

vote 4 --> false positives: 11999, false positive rate: 0.01002203360667924

vote 5 --> false positives: 11349, false positive rate: 0.009860438606627667

vote 6 --> false positives: 9953, false positive rate: 0.009960091585208669

vote 7 --> false positives: 9192, false positive rate: 0.010166252289397545

vote 8 --> false positives: 8837, false positive rate: 0.010116007262187402

vote 9 --> false positives: 11843, false positive rate: 0.010266826525048092

vote 10 --> false positives: 12337, false positive rate: 0.010021908963666214</code></pre>