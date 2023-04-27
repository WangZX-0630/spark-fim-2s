import sys
import time
from pyspark import SparkContext
from itertools import combinations


def apriori(sc, f_input, min_sup):
    # Load the transaction dataset as an RDD
    data = sc.textFile("hdfs://master:9000/" + f_input, 4)
    transactions = data.map(lambda line: sorted([int(item) for item in line.strip().split(' ')]))

    # Create an RDD of singleton itemsets and their counts
    sup = transactions.count() * min_sup
    item_counts = transactions.flatMap(lambda t: [(item, 1) for item in t]) \
                            .reduceByKey(lambda a, b: a + b) \
                            .filter(lambda ic: ic[1] >= sup)

    # Generate candidate itemsets from the previous frequent itemsets
    def generate_candidate_itemsets(frequent_itemsets, k):
        items = sorted(set(item for itemset in frequent_itemsets for item in itemset))
        return list(combinations(items, k))

    # Calculate the support of candidate itemsets
    def support(candidate_itemset, transactions_broadcast):
        transactions = transactions_broadcast.value
        return len([t for t in transactions if set(candidate_itemset).issubset(t)])


    # Iteratively generate candidate itemsets, calculate their support, and prune itemsets with low support
    frequent_itemsets = item_counts.map(lambda ic: ([ic[0]], ic[1]))
    transactions_broadcast = sc.broadcast(transactions.collect())

    k = 2
    while True:
        candidate_itemsets = generate_candidate_itemsets(frequent_itemsets.keys().collect(), k)
        
        if not candidate_itemsets:
            break

        candidate_itemset_counts = sc.parallelize(candidate_itemsets) \
                                    .map(lambda ci: (ci, support(ci, transactions_broadcast))) \
                                    .filter(lambda cic: cic[1] >= sup)

        if candidate_itemset_counts.isEmpty():
            break

        frequent_itemsets = frequent_itemsets.union(candidate_itemset_counts)
        k += 1

    return frequent_itemsets


if __name__ == "__main__":
    # Main
    sc = SparkContext(appName="Apriori")
    if len(sys.argv) == 3:
        sys.argv.append("0")

    stime = time.perf_counter()
    frequent_itemsets = apriori(sc, sys.argv[1], float(sys.argv[2]))
    # Print the discovered frequent itemsets
    # for itemset, support in frequent_itemsets:
    #     print(f"Itemset: {itemset}, Support: {support}")
    print("Time Cost: {}, Dataset: {}, sup: {}, Replicate_id: {} ENDLINE".format(
        time.perf_counter() - stime, sys.argv[1], float(sys.argv[2]), sys.argv[3]))
    # sc.show_profiles()