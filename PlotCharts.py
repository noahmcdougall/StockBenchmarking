import matplotlib.pyplot as plt
import numpy as np
from DatabaseConnection import establishconnection
from pandas import DataFrame
import matplotlib.dates as mdates
import matplotlib.ticker as mtick
from datetime import timedelta, date

engine, cnx, cursor = establishconnection()

chartname = ['onemonthbenchmark', 'threemonthbenchmark', 'sixmonthbenchmark', 'oneyearbenchmark']

for j in range(0, len(chartname)):
    # Define column names
    cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '"+chartname[j]+"';")
    tupledcolumns = cursor.fetchall()
    columnnames = [tupledcolumns[0] for tupledcolumns in tupledcolumns]

    # Pull each column's data from SQL table
    data = []
    for i in range(0, len(columnnames)):
        cursor.execute("SELECT "+str(columnnames[i])+" FROM "+chartname[j])
        tupleddata = cursor.fetchall()
        data.append([tupleddata[0] for tupleddata in tupleddata])

    # Puts data into a table that can be entered into a dataframe
    table = {}
    for i in range(0, len(columnnames)):
        table[columnnames[i]] = data[i]

    df = DataFrame(table,columns=columnnames)

    # Defines a color table
    colors= ['green', 'blue', 'red', 'orange', 'yellow', 'purple', 'cyan', 'magenta', 'black', np.random.rand(3,), np.random.rand(3,), np.random.rand(3,), np.random.rand(3,), np.random.rand(3,)]

    # Defining last value in each column for legend reasons
    lastdatapoints = []
    for k in range(1, len(data)):
        lastdatapoints.append(round(data[k][-1],1))

    legendlabels = []
    for k in range(0, len(lastdatapoints)):
        legendlabels.append(columnnames[k+1] + " ("+str(lastdatapoints[k])+"%)")

    # use the plot function
    fig, ax = plt.subplots(figsize=(10,6))

    months = mdates.MonthLocator()
    weeks = mdates.WeekdayLocator()
    monthsFmt = mdates.DateFormatter("%b %y")
    weeksFmt = mdates.DateFormatter("%d %b %y")
    if chartname[j] == 'onemonthbenchmark':
        ax.xaxis.set_major_locator(weeks)
        ax.xaxis.set_major_formatter(weeksFmt)
    else:
        ax.xaxis.set_major_locator(months)
        ax.xaxis.set_major_formatter(monthsFmt)
    ax.yaxis.set_major_formatter(mtick.FormatStrFormatter('%.0f%%'))


    for i in range(1, len(columnnames)):
        plt.plot('Date', columnnames[i], data=df, color=colors[i-1], linewidth=1)
        plt.text(data[0][-1], data[i][-1], str(round(data[i][-1],1))+"%", size=8)

    plt.legend(legendlabels)
    # plt.adjust(right=10)
    plt.xlim(data[0][0], data[0][-1])
    if chartname[j] == 'onemonthbenchmark':
        plt.title("One Month Benchmark")
    elif chartname[j] == 'threemonthbenchmark':
        plt.title("Three Month Benchmark")
    elif chartname[j] == 'sixmonthbenchmark':
        plt.title("Six Month Benchmark")
    elif chartname[j] == 'oneyearbenchmark':
        plt.title("One Year Benchmark")
    else:
        plt.title("Benchmark")
    plt.ylabel('Stock Price Change')
    plt.grid(which='major', axis='both')
    plt.savefig('//vmlinux/noahmcdougallwebsite/assets/img/'+chartname[j]+'.png')
    plt.savefig('//vmlinux/noahmcdougallwebsite/_site/assets/img/'+chartname[j]+'.png')
    print("Successfully saved "+chartname[j]+".png")
