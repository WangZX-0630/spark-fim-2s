import subprocess
import os

# Define the command and arguments
command = "spark-submit"
master_option = "--master"
master_url = "spark://master:7077"
python_files = [
    # "share/fp-growth.py",
    # "share/apriori_2.py",
    "share/apriori.py",
    # "share/fp-growth-lib.py",
    # "share/eclat.py"
]
dataset_files = [
    # "chess.dat",
    # "retail.dat",
    "mushroom.dat",
    # "kosarak.dat",
    # "movieItem.dat"
]
min_supports = [0.3, 0.1, 0.03, 0.01]
replicate_times = 5
result_file = "share/result.txt"

if __name__ == "__main__":
    # Main
    if not os.path.exists(result_file):
        os.mknod(result_file)
    # f = open(result_file, "w")
    f = open(result_file, "a+")
    f.write("========================================\n")
    f.write("New Test\n\n")
    avg_results = []
    f.write("Replicated Results:\n")

    for python_file in python_files:
        method = python_file.split("/")[-1].split(".")[0]
        for dataset_file in dataset_files:
            for min_support in min_supports:
                avg_time_cost = 0.0
                for i in range(replicate_times):
                    # Create a list containing the command and its arguments
                    cmd = [command, master_option, master_url, python_file, dataset_file, str(min_support), str(i)]

                    # Run the command using subprocess
                    try:
                        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
                        print("Output:\n", result.stdout)
                        print("Error:\n", result.stderr)
                        # Find the Time Cost: ... Line in the result.stdout
                        output_line = "Time Cost: " + result.stdout.split("Time Cost: ")[1]. \
                            split("ENDLINE")[0] + "Method: " + method
                        f.write(output_line + "\n")
                        avg_time_cost += float(output_line.split("Time Cost: ")[1].split(",")[0]) / replicate_times

                    except subprocess.CalledProcessError as e:
                        print("An error occurred while running the spark-submit command:")
                        print("Command: ", e.cmd)
                        print("Return code: ", e.returncode)
                        print("Output:\n", e.stdout)
                        print("Error:\n", e.stderr)
                
                avg_results.append("Average Time Cost: {}, Dataset: {}, Min_Support: {}, Method: {}".format(
                    avg_time_cost, dataset_file, min_support, method))
    f.write("\n----------------------------------------\n")
    f.write("Average Results:\n")

    for i in range(len(avg_results)):
        f.write(avg_results[i] + "\n")
    f.write("\n")
    f.close()

