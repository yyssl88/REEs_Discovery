import pandas as pd
import argparse

def main():
    parser = argparse.ArgumentParser(description="Learn Predicate Association")
    parser.add_argument('-data_name', '--data_name', type=str, default="airports")
    parser.add_argument('-N', '--N', type=int, default=1000)

    args = parser.parse_args()
    arg_dict = args.__dict__

    data_name = arg_dict["data_name"]
    N = arg_dict["N"]

    sequence_file = "./model/sequence_" + data_name + ".txt"
    RL_model_path = "./model/" + data_name + "_N" + str(N) + "/model.ckpt"

    sequence_file_new = "./model/sequence_" + data_name + "_new.txt"
    with open(sequence_file, "r") as f:
        pnum = int(f.readline())
        with open(sequence_file_new, "w") as f_new:
            f_new.write(str(pnum))
            f_new.write("\n")
            f_new.close()
        f.close()

    names = ["Psel", "next_p", "reward"]
    data = pd.read_csv(sequence_file, sep=",", index_col=False, names=names, dtype=str, skiprows=[0])
    neg_data = data[data["reward"].str.contains("-")].sample(frac=1)
    pos_data = data[~data["reward"].str.contains("-")].sample(frac=1)
    size = min(neg_data.shape[0], pos_data.shape[0])
    training_data = pd.concat([pos_data[:size], neg_data[:size]], axis=0)

    training_data.to_csv(sequence_file_new, mode="a", header=False, index=False)

if __name__ == "__main__":
    main()
