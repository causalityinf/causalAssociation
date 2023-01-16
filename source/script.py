import os
from glob import glob
import pandas as pd
from pprint import pprint

# nst_files = glob(os.path.join("results", "hits_at_k", "Synthetic 2", "gen_*", "*nst*"))
nst_files = glob(os.path.join("results", "hits_at_k", "Synthetic 2", "gen_*", "*cirm*"))

dfs = []

for nst_file in nst_files:
    df = pd.read_csv(nst_file)
    dfs.append(df)

big_df = pd.concat(dfs, ignore_index=True)
big_df.to_csv(
    os.path.join(
        # "results", "hits_at_k", "Synthetic 2", "concat_synthetic_2_nst_cirb_circ.csv"
        "results",
        "hits_at_k",
        "Synthetic 2",
        "concat_synthetic_2_cirm.csv",
    ),
    index=False,
)
print("MIN")
# print(big_df[["nst", "cirb", "circ"]].min())
print(big_df[["cirm 1 (avg)", "cirm 1 (max)", "cirm 2 (avg)", "cirm 2 (max)"]].min())
print()

print("MAX")
# print(big_df[["nst", "cirb", "circ"]].max())
print(big_df[["cirm 1 (avg)", "cirm 1 (max)", "cirm 2 (avg)", "cirm 2 (max)"]].max())
print()

print("FIRST QUANTILE")
# print(big_df[["nst", "cirb", "circ"]].quantile(q=0.25))
print(
    big_df[["cirm 1 (avg)", "cirm 1 (max)", "cirm 2 (avg)", "cirm 2 (max)"]].quantile(
        q=0.25
    )
)
print()

print("THIRD QUANTILE")
# print(big_df[["nst", "cirb", "circ"]].quantile(q=0.75))
print(
    big_df[["cirm 1 (avg)", "cirm 1 (max)", "cirm 2 (avg)", "cirm 2 (max)"]].quantile(
        q=0.75
    )
)
print()

print("MEDIAN")
# print(big_df[["nst", "cirb", "circ"]].median())
print(big_df[["cirm 1 (avg)", "cirm 1 (max)", "cirm 2 (avg)", "cirm 2 (max)"]].median())
