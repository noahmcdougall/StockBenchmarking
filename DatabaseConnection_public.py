import mysql.connector
from mysql.connector import errorcode
from sqlalchemy import create_engine
import sqlalchemy

def establishconnection():
    ## Creates connection to SQL Database
    engine = create_engine('mysql+mysqlconnector://noahmcdougall:thisiswherethepasswordgoesbecauseihardcodeditinlikeadumbdumb@vmlinux:3306/stocks', echo=False)

    ##Builds Cursor for sending commands to SQL Database
    cnx = engine.raw_connection()
    cursor = cnx.cursor(buffered=True)

    return engine, cnx, cursor
