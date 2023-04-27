import sys
import time
from pyspark import SparkContext
from itertools import combinations
# from pyspark import Partitioner

# Recursive ECLAT function
def eclat_run(prefix, tidsets, sup, frequent_itemsets):
    for i, (item, tidset) in enumerate(tidsets):
        new_prefix = prefix | {item}
        support = len(tidset)
        if support >= sup:
            frequent_itemsets.append((new_prefix, support))
            new_tidsets = [(item2, tidset & tidset2) for j, (item2, tidset2) in enumerate(tidsets[i + 1:])]
            eclat_run(new_prefix, new_tidsets, sup, frequent_itemsets)


# class CustomPartitioner():
#     def __init__(self, num_partitions):
#         self.num_partitions = num_partitions

#     def numPartitions(self):
#         return self.num_partitions

#     def getPartition(self, key):
#         return hash(key) % self.num_partitions

# def eclat_run(tidsets_rdd, sup):
#     def generate_candidates(items):
#         for itemset in combinations(items, 2):
#             union_itemset = itemset[0] | itemset[1]
#             if len(union_itemset) == len(itemset[0]) + 1:
#                 yield union_itemset

#     def eclat_partition(tidsets_partition):
#         frequent_itemsets = []

#         for tidset in tidsets_partition:
#             itemset, tids = tidset
#             support = len(tids)
#             if support >= sup:
#                 frequent_itemsets.append((itemset, support))

#         return frequent_itemsets

#     frequent_itemsets_rdd = tidsets_rdd
#     k = 1

#     while True:
#         # Generate candidate itemsets of size k+1
#         candidate_itemsets = frequent_itemsets_rdd.map(lambda x: (x[0], x[0])).distinct().groupByKey().flatMap(generate_candidates).distinct()

#         # Calculate support for candidate itemsets
#         candidate_tidsets = candidate_itemsets.map(lambda itemset: (itemset, set.intersection(*[tidsets_rdd.lookup(item)[0] for item in itemset])))

#         # Filter frequent itemsets
#         frequent_itemsets_rdd = candidate_tidsets.filter(lambda x: len(x[1]) >= sup)

#         # Check for termination condition
#         if frequent_itemsets_rdd.isEmpty():
#             break

#         k += 1

#     return frequent_itemsets_rdd.map(lambda x: (x[0], len(x[1])))


def eclat(sc, f_input, min_sup):
    # Load data and create transactions RDD
    data = sc.textFile('hdfs://master:9000/' + f_input, 4)
    transactions = data.map(lambda line: set(line.strip().split(" ")))

    # Compute support for each item
    item_support = transactions.flatMap(lambda transaction: [(item, 1) for item in transaction]) \
                                .reduceByKey(lambda a, b: a + b)

    # Filter items based on the minimum support threshold
    n_samples = data.count()
    # min_sup to frequency
    sup = n_samples * min_sup
    frequent_items = item_support.filter(lambda x: x[1] >= sup)

    # Create equivalence classes and initialize tidsets
    def create_equivalence_class(transaction, index):
        return [(item, {index}) for item in transaction]

    indexed_transactions = transactions.zipWithIndex()
    equivalence_classes = indexed_transactions.flatMap(lambda x: create_equivalence_class(x[0], x[1])) \
                                            .reduceByKey(lambda a, b: a | b)
    frequent_tidsets = frequent_items.join(equivalence_classes).map(lambda x: (x[0], x[1][1]))

    # Initialize the list of frequent itemsets and run ECLAT
    frequent_itemsets = []
    eclat_run(set(), frequent_tidsets.collect(), sup, frequent_itemsets)
    # tidsets_rdd = sc.parallelize(frequent_tidsets.collect())
    # frequent_itemsets = eclat_run(tidsets_rdd, sup).collect()
    return frequent_itemsets


if __name__ == "__main__":
    # Main
    sc = SparkContext(appName="ECLAT")
    if len(sys.argv) == 3:
        sys.argv.append("0")

    stime = time.perf_counter()
    frequent_itemsets = eclat(sc, sys.argv[1], float(sys.argv[2]))
    # Print the discovered frequent itemsets
    # for itemset, support in frequent_itemsets:
    #     print(f"Itemset: {itemset}, Support: {support}")
    print("Time Cost: {}, Dataset: {}, Min_Support: {}, Replicate_id: {} ENDLINE".format(
        time.perf_counter() - stime, sys.argv[1], float(sys.argv[2]), sys.argv[3]))
    # sc.show_profiles()