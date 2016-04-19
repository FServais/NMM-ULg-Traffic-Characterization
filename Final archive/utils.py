import matplotlib.pyplot as plt
from matplotlib import cm
from ipaddress import *
from os.path import commonprefix
import numpy as np

def plot_to_file(file_name, x, y, title, xlabel, ylabel, scale='lin'):
    plt.figure()
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)

    if scale == 'log':
        plt.loglog(x, y)
    else:
        plt.plot(x, y)

    plt.savefig("{}.pdf".format(file_name))

    plt.close()

def plot_fig(x, y, title, xlabel, ylabel, scale='lin'):
    plt.figure()
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)

    if scale == 'log':
        plt.loglog(x, y)
    else:
        plt.plot(x, y)


def plot_pie(values, labels, title):
    plt.figure()
    plt.title(title)

    cs = cm.Set1(np.arange(len(values)) / len(values))
    plt.pie(values, labels=labels, colors=cs, labeldistance=0.7)

    labels_with_values = []
    for i in range(0,len(values)):
        labels_with_values.append(str(labels[i]) + ' ({:.2f}%)'.format(values[i]*100))

    lgd = plt.legend(labels_with_values, loc='center left', bbox_to_anchor=(1, 0.5))
    plt.axis('equal')


def plot_pie_to_file(file_name, values, labels, title):
    plt.figure()
    plt.title(title)

    cs = cm.Set1(np.arange(len(values)) / len(values))
    plt.pie(values, labels=labels, colors=cs, labeldistance=0.7)

    labels_with_values = []
    for i in range(0,len(values)):
        labels_with_values.append(str(labels[i]) + ' ({:.2f}%)'.format(values[i]*100))

    lgd = plt.legend(labels_with_values, loc='center left', bbox_to_anchor=(1, 0.5))
    plt.axis('equal')
    plt.savefig("{}.pdf".format(file_name), bbox_extra_artists=(lgd,), bbox_inches='tight')

    plt.close()


def dist_ips(ip1, ip2):
    dist = 0
    for subnet in summarize_address_range(ip1, ip2):
        ips_in_subnet = list(subnet.subnets(new_prefix=32))
        dist += len(ips_in_subnet)

    return dist

def binary_str_ipv4(ip):
    bytes = v4_int_to_packed(int(ip))
    binary = 0
    for byte in bytes:
        binary = (binary << 8) | byte

    binary_without_front_0 = bin(binary)[2:]
    return '0' * (32-len(binary_without_front_0)) + binary_without_front_0

def length_longest_prefix(ips):
    ips = [binary_str_ipv4(ip) for ip in ips]
    lengths = [len(ip) for ip in ips]
    prefix = commonprefix(ips)
    print(prefix)
    return len(prefix)

