import pandas as pd
import numpy as np

from utils import *
from ipaddress import *

import time

start_time = time.time()

CHUNKSIZE = 10 ** 5

# ibyts = np.array([])
# ipkts = np.array([])
# durations = np.array([])

ibyts_pd = pd.Series()
ipkts_pd = pd.Series()
durations_pd = pd.Series()

port_traffic_sender = pd.DataFrame({'sp': [-1], 'ibyt': [-1]}, index=[0]) # Init with row of port -1 to set the columns as integers
port_traffic_receiver = pd.DataFrame({'dp': [-1], 'ibyt': [-1]}, index=[0])

traffic_by_ip = pd.DataFrame({'sa': [-1], 'ibyt': [-1]}, index=[0])

traffic_by_prefix_source = pd.DataFrame({'source_netw': [-1], 'ibyt': [-1], 'ipkt': [-1]}, index=[0])
traffic_by_prefix_dest = pd.DataFrame({'dest_netw': [-1], 'ibyt': [-1], 'ipkt': [-1]}, index=[0])


i = 0
df_save = pd.DataFrame()
for df in pd.read_csv("netflow_100000.csv", chunksize=CHUNKSIZE, iterator=True):
    i += 1
    print("Chunk number {}".format(i))
    df = df.dropna()

    # ibyts = np.append(ibyts, [ df[['ibyt']].mean() ])
    # ipkts = np.append(ipkts, [df[['ipkt']].mean()])
    # durations = np.append(durations, df[['td']].mean())

    ibyts_pd = pd.concat([ibyts_pd, df[['ibyt']].ix[:,0]])
    ipkts_pd = pd.concat([ipkts_pd, df[['ipkt']].ix[:,0]])
    durations_pd = pd.concat([durations_pd, df[['td']].ix[:,0]])

    # Compute traffic by port
    port_traffic_sender = pd.concat([port_traffic_sender, df[['sp', 'ibyt']]])
    gb_sender = port_traffic_sender.groupby('sp')
    port_traffic_sender = gb_sender.sum().reset_index()

    port_traffic_receiver = pd.concat([port_traffic_receiver, df[['dp', 'ibyt']]])
    gb_receiver = port_traffic_receiver.groupby('dp')
    port_traffic_receiver = gb_receiver.sum().reset_index()

    # Compute traffic by IP
    # traffic_by_ip = pd.concat([traffic_by_ip, df[['sa', 'ibyt']]])
    # gb_ip = traffic_by_ip.groupby('sa')
    # traffic_by_ip = gb_ip.sum().reset_index()

    df['source_netw'] = df['sa'].apply(lambda x: ip_address(x))
    df['dest_netw'] = df['da'].apply(lambda x: ip_address(x))

    df['source_netw'] = df['source_netw'].apply(lambda x: str(ip_interface(str(x) + '/24').network) if isinstance(x, IPv4Address) else str(ip_interface(str(x) + '/32').network))
    df['dest_netw'] = df['dest_netw'].apply(lambda x: str(ip_interface(str(x) + '/24').network) if isinstance(x, IPv4Address) else str(ip_interface(str(x) + '/32').network))

    traffic_by_prefix_source = pd.concat([traffic_by_prefix_source, df[['source_netw', 'ibyt', 'ipkt']]])
    gb_netw_source = traffic_by_prefix_source.groupby('source_netw')
    traffic_by_prefix_source = gb_netw_source.sum().reset_index()

    traffic_by_prefix_dest = pd.concat([traffic_by_prefix_dest, df[['dest_netw', 'ibyt', 'ipkt']]])
    gb_netw_source = traffic_by_prefix_dest.groupby('dest_netw')
    traffic_by_prefix_dest = gb_netw_source.sum().reset_index()

# -- Post-processing

port_traffic_sender = port_traffic_sender[port_traffic_sender.sp != -1]
port_traffic_sender['bytes_tot'] = port_traffic_sender['ibyt']

port_traffic_receiver = port_traffic_receiver[port_traffic_receiver.dp != -1]
port_traffic_receiver['bytes_tot'] = port_traffic_receiver['ibyt']


# IP
# traffic_by_ip = traffic_by_ip[traffic_by_ip['sa'] != -1]
# traffic_by_ip['sa'] = traffic_by_ip['sa'].apply(lambda x: ip_address(x))
# traffic_by_ipv4 = traffic_by_ip[traffic_by_ip['sa'].apply(lambda x: isinstance(x, IPv4Address))]
# traffic_by_ipv6 = traffic_by_ip[traffic_by_ip['sa'].apply(lambda x: isinstance(x, IPv6Address))]
#
# traffic_by_ipv4_sorted = traffic_by_ipv4.sort_values(by=['sa'])
# traffic_by_ipv6_sorted = traffic_by_ipv6.sort_values(by=['sa'])
#
# for i in range(0, len(traffic_by_ipv4_sorted)):
#     begin = traffic_by_ipv4_sorted.iloc[i]['sa']
#
#     j = 1
#     end = traffic_by_ipv4_sorted.iloc[i+1]['sa']
#     while dist_ips(begin, end) < 254:
#         j += 1
#         end = traffic_by_ipv4_sorted.iloc[i + j]['sa']
#
#     end = traffic_by_ipv4_sorted.iloc[i+j-1]['sa']
#
#     longest_prefix_length = length_longest_prefix([begin, end])
#
#     itf = IPv4Interface(str(begin) + '/' + str(length_longest_prefix([begin, end])))
#     traffic_by_prefix[itf] = traffic_by_ipv4_sorted.iloc[i]['ibyt']
#
#     i = j+1


# ---- COMPUTATIONS

# ---- Average packet size

# ipkt_size = ibyts/ipkts
ipkt_size =ibyts_pd/ipkts_pd
ipkt_size = ipkt_size[~np.isnan(ipkt_size)]
if ipkt_size.size > 0:
    print("Average packet size: {0:.2f}\n".format(ipkt_size.mean()))

# ---- CCDF
# -- Compute

# # Flow duration
# # durations_values, durations_base = np.histogram(durations, bins=40)
# durations_values, durations_base = np.histogram(durations_pd.tolist(), bins=40)
# # durations_values = durations_values / len(durations)
# durations_values = durations_values / len(durations_pd.index)
# durations_cumulative = np.cumsum(durations_values)
#
# # Number of bytes (in)
# ibyts_values, ibyts_base = np.histogram(ibyts, bins=40)
# ibyts_values = ibyts_values / len(ibyts)
# ibyts_cumulative = np.cumsum(ibyts_values)
#
# # Number of packets (in)
# ipkts_values, ipkts_base = np.histogram(ipkts, bins=40)
# ipkts_values = ipkts_values / len(ipkts)
# ipkts_cumulative = np.cumsum(ipkts_values)

# Flow duration
# durations_values, durations_base = np.histogram(durations, bins=40)
durations_values, durations_base = np.histogram(durations_pd)
# durations_values = durations_values / len(durations)
durations_values = durations_values / len(durations_pd.index)
durations_cumulative = np.cumsum(durations_values)

# Number of bytes (in)
ibyts_values, ibyts_base = np.histogram(ibyts_pd)
ibyts_values = ibyts_values / len(ibyts_pd.index)
ibyts_cumulative = np.cumsum(ibyts_values)

# Number of packets (in)
ipkts_values, ipkts_base = np.histogram(ibyts_pd)
ipkts_values = ipkts_values / len(ibyts_pd.index)
ipkts_cumulative = np.cumsum(ipkts_values)

# durations_pd_ordered = durations_pd.sort_values()
# durations_pd_ordered[len(durations_pd_ordered)] = durations_pd_ordered.iloc[-1]
# cum_dist = 1 - np.linspace(0., 1., len(durations_pd_ordered))
# durations_pd_ordered_cdf = pd.Series(cum_dist, index=durations_pd_ordered)
# ax = durations_pd_ordered_cdf.plot(drawstyle='steps')
# fig = ax.get_figure()
# fig.savefig("test.png", dpi=300)
#
# ccdf_durations = ccdf(durations_pd)
# ccdf_ipkts = ccdf(ipkts_pd)
# ccdf_ibyts = ccdf(ibyts_pd)
#
# d_plot = ccdf_durations.plot(drawstyle='steps')
# pkt_plot = ccdf_ipkts.plot(drawstyle='steps')
# byt_plot = ccdf_ibyts.plot(drawstyle='steps')
#
# d_fig = d_plot.get_figure()
# pkt_fig = pkt_plot.get_figure()
# byt_fig = byt_plot.get_figure()
#
# d_fig.savefig("durations.png", dpi=300)
# pkt_fig.savefig("ipkt.png", dpi=300)
# byt_fig.savefig("ibyt.png", dpi=300)

# -- Draw

# Flow duration
plot_to_file(file_name="ccdf_durations", title="CCDF of flow duration", x=durations_base[:-1], y=1-durations_cumulative, xlabel="Duration", ylabel="Complementary Cumulative Probability Distribution")
plot_to_file(file_name="ccdf_durations_log", title="CCDF of flow duration", x=durations_base[:-1], y=1-durations_cumulative, xlabel="Duration", ylabel="Complementary Cumulative Probability Distribution", scale='log')

# Number of bytes
plot_to_file(file_name="ccdf_byts", title="CCDF of number of bytes", x=ibyts_base[:-1], y=1-ibyts_cumulative, xlabel="Number of bytes", ylabel="Complementary Cumulative Probability Distribution")
plot_to_file(file_name="ccdf_byts_log", title="CCDF of number of bytes", x=ibyts_base[:-1], y=1-ibyts_cumulative, xlabel="Number of bytes", ylabel="Complementary Cumulative Probability Distribution", scale='log')

# Number of packets
plot_to_file(file_name="ccdf_pkts", title="CCDF of number of packets", x=ipkts_base[:-1], y=1-ipkts_cumulative, xlabel="Number of packets", ylabel="Complementary Cumulative Probability Distribution")
plot_to_file(file_name="ccdf_pkts_log", title="CCDF of number of packets", x=ipkts_base[:-1], y=1-ipkts_cumulative, xlabel="Number of packets", ylabel="Complementary Cumulative Probability Distribution", scale='log')


# ---- Port traffic
sum_bytes_sender = port_traffic_sender['bytes_tot'].sum()
sum_bytes_receiver = port_traffic_receiver['bytes_tot'].sum()

port_traffic_sender['bytes_tot_per'] = port_traffic_sender['bytes_tot'] / sum_bytes_sender
port_traffic_receiver['bytes_tot_per'] = port_traffic_receiver['bytes_tot'] / sum_bytes_receiver

top_10_sender = port_traffic_sender.sort_values(by=['bytes_tot_per'], ascending=[False])[0:10]
top_10_receiver = port_traffic_receiver.sort_values(by=['bytes_tot_per'], ascending=[False])[0:10]
not_top_10_sender = port_traffic_sender.sort_values(by=['bytes_tot_per'], ascending=[False])[10:]
not_top_10_receiver = port_traffic_receiver.sort_values(by=['bytes_tot_per'], ascending=[False])[10:]

# -- Sender
port_sender_list = [int(label) for label in top_10_sender['sp'].values.tolist()]
port_sender_list.append("Others")
labels_sender = tuple(port_sender_list)
values_sender = top_10_sender['bytes_tot_per'].values.tolist()
values_sender.append(not_top_10_sender['bytes_tot_per'].sum())

plot_pie_to_file("top_10_ports_sender", values_sender, labels_sender, "Top 10 ports (sender)")

# -- Receiver
port_receiver_list = [int(label) for label in top_10_receiver['dp'].values.tolist()]
port_receiver_list.append("Others")
labels_receiver = tuple(port_receiver_list)
values_receiver = top_10_receiver['bytes_tot_per'].values.tolist()
values_receiver.append(not_top_10_receiver['bytes_tot_per'].sum())

plot_pie_to_file("top_10_ports_receiver", values_receiver, labels_receiver, "Top 10 ports (receiver)")


# 92.106.195.0/24 address block
traffic_by_prefix_source = traffic_by_prefix_source[traffic_by_prefix_source['source_netw'] != -1]
traffic_by_prefix_dest = traffic_by_prefix_dest[traffic_by_prefix_dest['dest_netw'] != -1]

total_traffic_source_pkts = traffic_by_prefix_source['ipkt'].sum()
total_traffic_source_byts = traffic_by_prefix_source['ibyt'].sum()
total_traffic_dest_pkts = traffic_by_prefix_dest['ipkt'].sum()
total_traffic_dest_byts = traffic_by_prefix_dest['ibyt'].sum()

block_source = traffic_by_prefix_source[traffic_by_prefix_source['source_netw'] == '92.106.195.0/24']
block_dest = traffic_by_prefix_dest[traffic_by_prefix_dest['dest_netw'] == '92.106.195.0/24']

if len(block_source.index) > 0:
    print('Fraction of traffic sent by 92.106.195.0/24 (in pkts): {:.3%}'.format(traffic_by_prefix_source[traffic_by_prefix_source['source_netw'] == '92.106.195.0/24']['ipkt'].iloc[0]/total_traffic_source_pkts))
    print('Fraction of traffic sent by 92.106.195.0/24 (in bytes): {:.3%}'.format(traffic_by_prefix_source[traffic_by_prefix_source['source_netw'] == '92.106.195.0/24']['ibyt'].iloc[0]/total_traffic_source_byts))

if len(block_dest.index) > 0:
    print('Fraction of traffic sent to 92.106.195.0/24 (in pkts): {:.3%}'.format(traffic_by_prefix_dest[traffic_by_prefix_dest['dest_netw'] == '92.106.195.0/24']['ipkt'].iloc[0]/total_traffic_dest_pkts))
    print('Fraction of traffic sent to 92.106.195.0/24 (in bytes): {:.3%}'.format(traffic_by_prefix_dest[traffic_by_prefix_dest['dest_netw'] == '92.106.195.0/24']['ibyt'].iloc[0]/total_traffic_dest_byts))




print("Execution time: {}".format(time.time() - start_time))