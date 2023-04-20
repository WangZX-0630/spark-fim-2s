from pyspark import SparkContext
import time
import sys

# FPNode class

class FPNode:
    def __init__(self, item, count, parent):
        self.item = item
        self.count = count
        self.parent = parent
        self.children = {}
        self.next = None

    def increment(self, count):
        self.count += count

    def display(self, depth=0):
        print('  ' * depth, self.item, '(', self.count, ')')
        for child in self.children.values():
            child.display(depth + 1)

# Functions

def insert_tree(transaction, tree, header_table, count):
    if transaction:
        item = transaction[0]
        remaining_transaction = transaction[1:]

        # If item is already in tree.children, increment its count
        if item in tree.children:
            tree.children[item].increment(count)
        # Else, create a new node and add it to tree.children
        else:
            tree.children[item] = FPNode(item, count, tree)
            # If item is not in header_table, add it
            if header_table[item] is None:
                header_table[item] = tree.children[item]
            # Else, add it to the end of the linked list
            else:
                node = header_table[item]
                while node.next is not None:
                    node = node.next
                node.next = tree.children[item]
        # Recurse on remaining items in the transaction
        insert_tree(remaining_transaction,
                    tree.children[item], header_table, count)


def construct_fp_tree(transactions, sorted_frequent_items_bc, min_support):
    # Build FP-Tree, starting with root node
    root = FPNode(None, 0, None)
    header_table = {item: None for item, _ in sorted_frequent_items_bc.value.items()}
    # For each transaction, sort it by frequency of items in header_table and insert items into the tree
    
    for transaction, count in transactions:
        # sorted_transaction = [item for item in sorted(
        #     transaction, key=lambda x: sorted_frequent_items_bc.value[x], reverse=True) if item in header_table]
        # Get frequent items in transaction
        frequent_items = [item for item in transaction if item in header_table]
        # Sort frequent items by descending frequency count
        sorted_transaction = sorted(frequent_items, key=lambda x: sorted_frequent_items_bc.value[x], reverse=True)
        insert_tree(sorted_transaction, root, header_table, count)

    return root, header_table

def construct_conditional_fp_tree(transactions, sorted_frequent_items_bc, min_support):
    # Build conditional FP-Tree, starting with root node
    root = FPNode(None, 0, None)
    header_table = {item: None for item, _ in sorted_frequent_items_bc.value.items()}
    # For each transaction, sort it by frequency of items in header_table and insert items into the tree
    for transaction, count in transactions:
        # Get frequent items in transaction
        frequent_items = [item for item in transaction if item in header_table]
        # Sort frequent items by descending frequency count
        sorted_transaction = sorted(frequent_items, key=lambda x: sorted_frequent_items_bc.value[x], reverse=True)
        insert_tree(sorted_transaction, root, header_table, count)

    return root, header_table


def conditional_tree(item, header_table):
    # Find all paths from the nodes in header table to the root
    paths = []
    node = header_table[item]

    while node is not None:
        path = []
        parent = node.parent

        while parent.item is not None:
            path.append(parent.item)
            parent = parent.parent

        if path:
            paths.append((path[::-1], node.count))

        node = node.next

    return paths


def fp_growth_run(sc, header_table, min_support, prefix, frequent_itemsets, sorted_frequent_items_bc):
    # Sort header table by descending frequency
    items = [item for item, node in header_table.items() if node is not None]
    # If there are no items, return
    if not items:
        return
    # Add frequent itemsets to the list
    for item in items:
        new_prefix = prefix.copy()
        new_prefix.add(item)
        frequent_itemsets.append((new_prefix, header_table[item].count))

        # paths = conditional_tree(item, header_table)
        # conditional transactions are the paths with their counts repeated
        conditional_transactions = conditional_tree(item, header_table)
        
        conditional_frequent_items_bc = sc.broadcast(dict([(item, index) for index, (
            item, _) in enumerate(sorted_frequent_items_bc.value.items()) if item in new_prefix]))
        conditional_trees, conditional_header_table = construct_conditional_fp_tree(
            conditional_transactions, conditional_frequent_items_bc, min_support)

        fp_growth_run(sc, conditional_header_table, min_support,
                  new_prefix, frequent_itemsets, sorted_frequent_items_bc)


def fp_growth(sc, f_input, min_sup):
    # Read input data and split transactions
    input_data = sc.textFile('hdfs://master:9000/' + f_input, 2)
    transactions = input_data.map(lambda line: line.strip().split(' '))

    n_samples = input_data.count()
    # min_sup to frequency
    min_support = n_samples * min_sup

    # Compute item frequencies and filter out infrequent items
    item_frequencies = transactions.flatMap(lambda transaction: transaction).map(
        lambda item: (item, 1)).reduceByKey(lambda a, b: a + b)
    frequent_items = item_frequencies.filter(
        lambda item_freq: item_freq[1] >= min_support)

    # Sort frequent items by descending frequency
    sorted_frequent_items = frequent_items.sortBy(
        lambda item_freq: (-item_freq[1], item_freq[0])).collect()

    # Create a broadcast variable for the sorted frequent items
    sorted_frequent_items_bc = sc.broadcast(
        dict([(item, index) for index, (item, _) in enumerate(sorted_frequent_items)]))

    # Construct FP-Tree
    transactions_filtered_sorted = transactions.map(lambda transaction: [item for item in transaction if item in dict(
        sorted_frequent_items_bc.value)]).filter(lambda transaction: len(transaction) > 0)
    transaction_counts = transactions_filtered_sorted.map(lambda transaction: (
        tuple(transaction), 1)).reduceByKey(lambda a, b: a + b).collect()
    fp_tree, header_table = construct_fp_tree(
        transaction_counts, sorted_frequent_items_bc, min_support)

    # Mine frequent itemsets using FP-Growth
    frequent_itemsets = []
    fp_growth_run(sc, header_table, min_support, set(), frequent_itemsets, sorted_frequent_items_bc)

    return frequent_itemsets


if __name__ == "__main__":
    # Main
    sc = SparkContext(appName="FP-Growth")
    if len(sys.argv) == 3:
        sys.argv.append("0")

    stime = time.perf_counter()
    frequent_itemsets = fp_growth(sc, sys.argv[1], float(sys.argv[2]))
    # total_count = sum([count for _, count in frequent_itemsets])
    # print(frequent_itemsets, total_count)
    print("Time Cost: {}, Dataset: {}, Min_Support: {}, Replicate_id: {} ENDLINE".format(
        time.perf_counter() - stime, sys.argv[1], float(sys.argv[2]), sys.argv[3]))
