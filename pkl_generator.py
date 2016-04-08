import pandas as pd


def generate_pkl(n_rows):
    # Generate a .pkl file containing the `n_rows` first lines of the netflow.csv dataset.

    netflow_df = pd.read_csv("netflow.csv", nrows=n_rows)
    netflow_df.to_pickle("netflow_" + str(n_rows) + ".pkl")

generate_pkl(n_rows=5000000)