import pandas as pd
import numpy as np
from utils import plot_to_file

CHUNKSIZE = 10 ** 3

# dfs = pd.read_csv("netflow_100000.csv", chunksize=CHUNKSIZE, iterator=True)
#
# df = pd.concat(dfs, ignore_index=True)
# print("")

ibyts = np.array([])
obyts = np.array([])
ipkts = np.array([])
opkts = np.array([])
durations = np.array([])

for df in pd.read_csv("netflow_2000000.csv", chunksize=CHUNKSIZE, iterator=True):
    ibyts = np.append(ipkts, [ df[['ibyt']].mean() ])
    obyts = np.append(opkts, [ df[['obyt']].mean() ])
    ipkts = np.append(ipkts, [df[['ipkt']].mean()])
    opkts = np.append(opkts, [df[['opkt']].mean()])
    durations = np.append(durations, df[['td']].mean())

# ---- Average packet size

ipkt_size = ibyts/ipkts
ipkt_size = ipkt_size[~np.isnan(ipkt_size)]
if ipkt_size.size > 0:
    print("Average packet size (input): {0:.2f}\n".format(ipkt_size.mean()))

opkt_size = obyts/opkts
opkt_size = opkt_size[~np.isnan(opkt_size)]
if opkt_size.size > 0:
    print("Average packet size (output): {0:.2f}\n".format(opkt_size.mean()))

# ---- CCDF
# -- Compute

# Flow duration
durations_values, durations_base = np.histogram(durations, bins=40)
durations_values = durations_values / len(durations)
durations_cumulative = np.cumsum(durations_values)

# Number of bytes (in)
ibyts_values, ibyts_base = np.histogram(ibyts, bins=40)
ibyts_values = ibyts_values / len(ibyts)
ibyts_cumulative = np.cumsum(ibyts_values)

# Number of bytes (out)
obyts_values, obyts_base = np.histogram(obyts, bins=40)
obyts_values = obyts_values / len(obyts)
obyts_cumulative = np.cumsum(obyts_values)

# Number of packets (in)
ipkts_values, ipkts_base = np.histogram(ipkts, bins=40)
ipkts_values = ipkts_values / len(ipkts)
ipkts_cumulative = np.cumsum(ipkts_values)

# Number of packets (out)
opkts_values, opkts_base = np.histogram(opkts, bins=40)
opkts_values = opkts_values / len(opkts)
opkts_cumulative = np.cumsum(opkts_values)

# -- Draw

# Flow duration
plot_to_file(file_name="ccdf_durations", title="CCDF of flow duration", x=durations_base[:-1], y=1-durations_cumulative, xlabel="Duration", ylabel="Complementary Cumulative Probability Distribution")
plot_to_file(file_name="ccdf_durations_log", title="CCDF of flow duration", x=durations_base[:-1], y=1-durations_cumulative, xlabel="Duration", ylabel="Complementary Cumulative Probability Distribution", scale='log')

# Number of bytes (in)
plot_to_file(file_name="ccdf_ibyts", title="CCDF of number of bytes (in)", x=ibyts_base[:-1], y=1-ibyts_cumulative, xlabel="Number of bytes (in)", ylabel="Complementary Cumulative Probability Distribution")
plot_to_file(file_name="ccdf_ibyts_log", title="CCDF of number of bytes (in)", x=ibyts_base[:-1], y=1-ibyts_cumulative, xlabel="Number of bytes (in)", ylabel="Complementary Cumulative Probability Distribution", scale='log')

# Number of bytes (out)
plot_to_file(file_name="ccdf_obyts", title="CCDF of number of bytes (out)", x=obyts_base[:-1], y=1-obyts_cumulative, xlabel="Number of bytes (out)", ylabel="Complementary Cumulative Probability Distribution")
plot_to_file(file_name="ccdf_obyts_log", title="CCDF of number of bytes (out)", x=obyts_base[:-1], y=1-obyts_cumulative, xlabel="Number of bytes (out)", ylabel="Complementary Cumulative Probability Distribution", scale='log')

# Number of packets (in)
plot_to_file(file_name="ccdf_ipkts", title="CCDF of number of packets (in)", x=ipkts_base[:-1], y=1-ipkts_cumulative, xlabel="Number of packets (in)", ylabel="Complementary Cumulative Probability Distribution")
plot_to_file(file_name="ccdf_ipkts_log", title="CCDF of number of packets (in)", x=ipkts_base[:-1], y=1-ipkts_cumulative, xlabel="Number of packets (in)", ylabel="Complementary Cumulative Probability Distribution", scale='log')

# Number of packets (out)
plot_to_file(file_name="ccdf_opkts", title="CCDF of number of packets (out)", x=opkts_base[:-1], y=1-opkts_cumulative, xlabel="Number of packets (out)", ylabel="Complementary Cumulative Probability Distribution")
plot_to_file(file_name="ccdf_opkts_log", title="CCDF of number of packets (out)", x=opkts_base[:-1], y=1-opkts_cumulative, xlabel="Number of packets (out)", ylabel="Complementary Cumulative Probability Distribution", scale='log')
