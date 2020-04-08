from DatabaseConnection import establishconnection

engine, cnx, cursor = establishconnection()

def computeonemonthbenchmark(ticker, iterator, maxiterator):
    #Sets the date that all other close values will be compared to.  Since the financial data isn't available on weekends, it takes the following Monday if 30 days falls on the weekend.
    cursor.execute("SET @referencedate = (SELECT MIN(Date) FROM "+ticker.replace('-','')+"_historic_stock WHERE Date BETWEEN DATE_SUB(NOW(), INTERVAL 30 DAY) AND NOW());")
    #Pulls close value from the referencedate
    cursor.execute("SET @referenceclose = (SELECT Close FROM "+ticker.replace('-','')+"_historic_stock WHERE Date = @referencedate);")
    if iterator == 0:
        #Deletes existing data in the onemonthbenchmark
        cursor.execute("DELETE FROM onemonthbenchmark;")
        print("**Deleted existing data from onemonthbenchmark... out with the old and in with the new!**")
        #Computes change in close value compared to referencedate and updates onemonthbenchmark table with the values.  This creates the unique date primary keys.
        cursor.execute("INSERT INTO onemonthbenchmark (Date, "+ticker.replace('-','')+") SELECT Date,(Close-@referenceclose)/@referenceclose*100 FROM "+ticker.replace('-','')+"_historic_stock WHERE Date >= @referencedate;")
        cnx.commit()
    else:
        #Does the same as the INSERT, only this inner joins since the table rows have already been created.
        cursor.execute("UPDATE onemonthbenchmark INNER JOIN "+ticker.replace('-','')+"_historic_stock ON onemonthbenchmark.Date = "+ticker.replace('-','')+"_historic_stock.Date SET onemonthbenchmark."+ticker.replace('-','')+" = ("+ticker.replace('-','')+"_historic_stock.Close-@referenceclose)/@referenceclose*100;")
        cnx.commit()
    if iterator == maxiterator:
        print("**Completed updating One Month Benchmark**")
    return

def computethreemonthbenchmark(ticker, iterator, maxiterator):
    #Sets the date that all other close values will be compared to.  Since the financial data isn't available on weekends, it takes the following Monday if 30 days falls on the weekend.
    cursor.execute("SET @referencedate = (SELECT MIN(Date) FROM "+ticker.replace('-','')+"_historic_stock WHERE Date BETWEEN DATE_SUB(NOW(), INTERVAL 91 DAY) AND NOW());")
    #Pulls close value from the referencedate
    cursor.execute("SET @referenceclose = (SELECT Close FROM "+ticker.replace('-','')+"_historic_stock WHERE Date = @referencedate);")
    if iterator == 0:
        #Deletes existing data in the threemonthbenchmark
        cursor.execute("DELETE FROM threemonthbenchmark;")
        print("**Deleted existing data from threemonthbenchmark... out with the old and in with the new!**")
        #Computes change in close value compared to referencedate and updates onemonthbenchmark table with the values.  This creates the unique date primary keys.
        cursor.execute("INSERT INTO threemonthbenchmark (Date, "+ticker.replace('-','')+") SELECT Date,(Close-@referenceclose)/@referenceclose*100 FROM "+ticker.replace('-','')+"_historic_stock WHERE Date >= @referencedate;")
        cnx.commit()
    else:
        #Does the same as the INSERT, only this inner joins since the table rows have already been created.
        cursor.execute("UPDATE threemonthbenchmark INNER JOIN "+ticker.replace('-','')+"_historic_stock ON threemonthbenchmark.Date = "+ticker.replace('-','')+"_historic_stock.Date SET threemonthbenchmark."+ticker.replace('-','')+" = ("+ticker.replace('-','')+"_historic_stock.Close-@referenceclose)/@referenceclose*100;")
        cnx.commit()
    if iterator == maxiterator:
        print("**Completed updating Three Month Benchmark**")
    return

def computesixmonthbenchmark(ticker, iterator, maxiterator):
    #Sets the date that all other close values will be compared to.  Since the financial data isn't available on weekends, it takes the following Monday if 30 days falls on the weekend.
    cursor.execute("SET @referencedate = (SELECT MIN(Date) FROM "+ticker.replace('-','')+"_historic_stock WHERE Date BETWEEN DATE_SUB(NOW(), INTERVAL 182 DAY) AND NOW());")
    #Pulls close value from the referencedate
    cursor.execute("SET @referenceclose = (SELECT Close FROM "+ticker.replace('-','')+"_historic_stock WHERE Date = @referencedate);")
    if iterator == 0:
        #Deletes existing data in the sixmonthbenchmark
        cursor.execute("DELETE FROM sixmonthbenchmark;")
        print("**Deleted existing data from sixmonthbenchmark... out with the old and in with the new!**")
        #Computes change in close value compared to referencedate and updates onemonthbenchmark table with the values.  This creates the unique date primary keys.
        cursor.execute("INSERT INTO sixmonthbenchmark (Date, "+ticker.replace('-','')+") SELECT Date,(Close-@referenceclose)/@referenceclose*100 FROM "+ticker.replace('-','')+"_historic_stock WHERE Date >= @referencedate;")
        cnx.commit()
    else:
        #Does the same as the INSERT, only this inner joins since the table rows have already been created.
        cursor.execute("UPDATE sixmonthbenchmark INNER JOIN "+ticker.replace('-','')+"_historic_stock ON sixmonthbenchmark.Date = "+ticker.replace('-','')+"_historic_stock.Date SET sixmonthbenchmark."+ticker.replace('-','')+" = ("+ticker.replace('-','')+"_historic_stock.Close-@referenceclose)/@referenceclose*100;")
        cnx.commit()
    if iterator == maxiterator:
        print("**Completed updating Six Month Benchmark**")
    return

def computeoneyearbenchmark(ticker, iterator, maxiterator):
    #Sets the date that all other close values will be compared to.  Since the financial data isn't available on weekends, it takes the following Monday if 30 days falls on the weekend.
    cursor.execute("SET @referencedate = (SELECT MIN(Date) FROM "+ticker.replace('-','')+"_historic_stock WHERE Date BETWEEN DATE_SUB(NOW(), INTERVAL 365 DAY) AND NOW());")
    #Pulls close value from the referencedate
    cursor.execute("SET @referenceclose = (SELECT Close FROM "+ticker.replace('-','')+"_historic_stock WHERE Date = @referencedate);")
    if iterator == 0:
        #Deletes existing data in the oneyearbenchmark
        cursor.execute("DELETE FROM oneyearbenchmark;")
        print("**Deleted existing data from oneyearbenchmark... out with the old and in with the new!**")
        #Computes change in close value compared to referencedate and updates onemonthbenchmark table with the values.  This creates the unique date primary keys.
        cursor.execute("INSERT INTO oneyearbenchmark (Date, "+ticker.replace('-','')+") SELECT Date,(Close-@referenceclose)/@referenceclose*100 FROM "+ticker.replace('-','')+"_historic_stock WHERE Date >= @referencedate;")
        cnx.commit()
    else:
        #Does the same as the INSERT, only this inner joins since the table rows have already been created.
        cursor.execute("UPDATE oneyearbenchmark INNER JOIN "+ticker.replace('-','')+"_historic_stock ON oneyearbenchmark.Date = "+ticker.replace('-','')+"_historic_stock.Date SET oneyearbenchmark."+ticker.replace('-','')+" = ("+ticker.replace('-','')+"_historic_stock.Close-@referenceclose)/@referenceclose*100;")
        cnx.commit()
    if iterator == maxiterator:
        print("**Completed updating One Year Benchmark**")
    return


if __name__ == '__main__':
    stockstocheck = ['bp', 'cvx', 'xom', 'cop', 'rds-a', 'tot']

    for i in range (0, len(stockstocheck)):
        computeonemonthbenchmark(stockstocheck[i], i, len(stockstocheck)-1)
        computethreemonthbenchmark(stockstocheck[i], i, len(stockstocheck)-1)
        computesixmonthbenchmark(stockstocheck[i], i, len(stockstocheck)-1)
        computeoneyearbenchmark(stockstocheck[i], i, len(stockstocheck)-1)
