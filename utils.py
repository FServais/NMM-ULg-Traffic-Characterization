import matplotlib.pyplot as plt

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