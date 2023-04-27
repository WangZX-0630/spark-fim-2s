import os 

def read_transactions(file_name):
    with open(file_name, 'r') as f:
        return [set(line.strip().split()) for line in f.readlines()]

def calculate_dataset_characteristics(transactions):
    num_transactions = len(transactions)
    all_items = set(item for transaction in transactions for item in transaction)
    num_items = len(all_items)
    avg_itemset_size = sum(len(transaction) for transaction in transactions) / num_transactions
    total_possible_itemsets = num_transactions * num_items
    density = sum(len(transaction) for transaction in transactions) / total_possible_itemsets

    return {
        "Density": density,
        "Number of Transactions": num_transactions,
        "Number of Items": num_items,
        "Average Itemset Size": avg_itemset_size
    }


if __name__ == '__main__':
    dataset_dir = "/Users/wangzixian/docker/spark-fim-3s/share/data/"
    for file in os.listdir(dataset_dir):
        dataset_file = os.path.join(dataset_dir, file)
        
        transactions = read_transactions(dataset_file)
        result = calculate_dataset_characteristics(transactions)
        print(str(dataset_file), str(result))
