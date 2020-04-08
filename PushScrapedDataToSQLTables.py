from PullDataFromAlphaVantage import pulldata
from DatabaseConnection import establishconnection
import pandas as pd
import sqlalchemy

engine, cnx, cursor = establishconnection()
print("**Connected to SQL Database**")

def pushtotable(ticker):
    ##Drops temporary table in case it already exists
    try:
        cursor.execute("DROP TABLE tempimporttable;")
        print("**Found an existing temporary table and deleted it**")
    except:
        pass

    ## Runs script that webscrapes from Alpha Vantage
    dataframe = pulldata(ticker)
    print("**Data successfully scraped**")

    ## Imports the data into a temporary import table
    dataframe.to_sql( name = 'tempimporttable',
                        con = engine,
                        if_exists = 'fail',
                        chunksize = 1000,
                        index = True,
                        dtype={ 'Date': sqlalchemy.DateTime(),
                                'Open': sqlalchemy.types.Float(precision=2, asdecimal=True),
                                'High': sqlalchemy.types.Float(precision=2, asdecimal=True),
                                'Low': sqlalchemy.types.Float(precision=2, asdecimal=True),
                                'Close': sqlalchemy.types.Float(precision=2, asdecimal=True),
                                'Volume': sqlalchemy.types.INTEGER()
                                })
    print("**Data imported into temporary table**")

    ## Appends non-unique values into table and then drops the temporary table
    cursor.execute("INSERT INTO "+ticker.replace('-','')+"_historic_stock SELECT * FROM tempimporttable WHERE Date NOT IN (SELECT Date FROM "+ticker.replace('-','')+"_historic_stock);")
    cursor.execute("DROP TABLE tempimporttable;")
    cnx.commit()
    print("**Data appended into "+ticker+"_historic_stock**")
    print("**Temporary table dropped**")
    return

if __name__ == '__main__':
    # Chooses the stock tickers to look for, and iterates through each of them to import into the SQL database
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
