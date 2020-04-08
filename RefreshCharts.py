from PushScrapedDataToSQLTables import pushtotable
from Benchmarks import computeonemonthbenchmark, computethreemonthbenchmark, computesixmonthbenchmark, computeoneyearbenchmark

stockstocheck = ['bp', 'cvx', 'xom', 'cop', 'rds-a', 'tot']

for i in range (0, len(stockstocheck)):
    if i == 0:
        print("**Starting on "+stockstocheck[0]+"**")
    elif i == len(stockstocheck):
        pass
    else:
        print("**Moving onto "+stockstocheck[i]+"**")
    pushtotable(stockstocheck[i])
print("**Completed updating historic tables**")

for i in range(0, len(stockstocheck)):
    computeonemonthbenchmark(stockstocheck[i], i, len(stockstocheck)-1)
    computethreemonthbenchmark(stockstocheck[i], i, len(stockstocheck)-1)
    computesixmonthbenchmark(stockstocheck[i], i, len(stockstocheck)-1)
    computeoneyearbenchmark(stockstocheck[i], i, len(stockstocheck)-1)
print("**Completed updating benchmarks**")
