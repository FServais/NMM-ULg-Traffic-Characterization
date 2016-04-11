import matplotlib.pyplot as plt
from matplotlib import cm

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

def plot_pie_to_file(file_name, values, labels, title):
    plt.figure()
    plt.title(title)

    cs = cm.Set1(np.arange(len(values)) / len(values))
    plt.pie(values, labels=labels, autopct='%1.1f%%', colors=cs)
    plt.axis('equal')
    plt.savefig("{}.pdf".format(file_name))

    plt.close()