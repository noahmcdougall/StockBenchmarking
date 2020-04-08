import pandas_datareader

#This is my API key from https://www.alphavantage.co/
key = 'notgoingtoputtheactualoneherebutyoucangetyourownprettyeasilywiththelinkabove'

def pulldata (ticker):
    df = pandas_datareader.data.DataReader(ticker, 'av-daily', start=None, end=None, api_key = key)
    df.index.name = "Date"
    df = df.rename(columns={'Date': 'Date', 'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'})
    # print(df)
    return df
