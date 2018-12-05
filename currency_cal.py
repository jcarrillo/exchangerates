#!/usr/bin/python
"""
   Owner: Jhon Carrillo
   currency_cal.py --lookback=<lookback> --base=<base_currency> --currency=<currency_codes>
   --lookback: Years to analyze
   --base: Base currency
   --currency: Set of currency codes
   sample: python currency_cal.py --lookback=2 --base=EUR --currency=USD,AUD,CAD,PLN,MXN

"""
import sys, getopt
import pandas as pd
import sqlite3
from sqlite3 import Error
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
import json
import urllib

class process_currency:
    """
    Class definition

    """
    def __init__(self,base_currency,lookback,currency_codes):
       """
        Class Initialization

       """
        self.access_key=[YOUR TOKEN]
        self.base=base_currency
        self.symbols=currency_codes
        self.lookback=int(lookback)
        self.startdate=self.getpastdate()
        self.enddate=self.getcurrentdate()
        self.dbname="db3.db"
        self.createsql="CREATE TABLE IF NOT EXISTS stg_exchange_currency (id INTEGER PRIMARY KEY AUTOINCREMENT,base_code varchar(3),dt date,rate numeric(22,10),currency_code varchar(3));"
        self.sql="Insert into stg_exchange_currency (base_code,dt,rate,currency_code) values "
        self.truncsql="Delete from  stg_exchange_currency"
        self.avgsql="Select  avg(rate),currency_code,base_code from stg_exchange_currency group by currency_code,base_code "
        self.url="http://data.fixer.io/api/%s?access_key=%s&base=%s&symbols=%s"

    def create_connection(self):
        """ create a database connection to the SQLite database
        specified by the db_file
        :return: Connection object or None
        """
        try:
            conn = sqlite3.connect(self.dbname)
            return conn
        except Error as e:
            print(e)
        return None

    def execute(self):
        """
           Function to connect and execute the DML and sql query
        """
        try:
            conn = self.create_connection()
            cur = conn.cursor()
            cur.execute(self.createsql)
            cur.execute(self.truncsql)
            cur.execute(self.sql)
            cur.execute(self.avgsql)
            rows = cur.fetchall()
            print("Summary:")
            for row in rows:
                print("Currency Base: %s Currency Code: %s Average: %s " % (row[2],row[1],row[0]))
            cur.close()
            conn.commit()
            return 1
        except Error as e:
            print(e)
            conn.rollback()

        return None
    def getcurrentdate(self):
        """
           Function to extract the currect date
        """
        dt=str(datetime.now().isoformat())[0:10]
        return dt
    def getpastdate(self):
        """
           Function to extract the past date
        """
        dt=str((datetime.now() - relativedelta(years=self.lookback)).isoformat())[0:10]
        return dt
    def load(self):
        """  Prepare the date range using panda, extract datasets from the API and insert the information in the database  """
        try:
            print("Starting date processing...")
            print(datetime.now())
            print("Getting API currency data...")
            date_range = pd.date_range(
                                        self.startdate,
                                        self.enddate,
                                        freq='D')
            for d in date_range:
                dt=str(d)[0:10]
                currenturl=self.url % (
                                    dt,
                                    self.access_key,
                                    self.base,
                                    self.symbols)
                req = requests.get(currenturl)
                data=req.json()
                print(currenturl)
                if data["success"]:
                    for symbol,rate in data["rates"].items():
                        self.sql=self.sql+" ('%s','%s',%s,'%s')," % (self.base, dt, rate,symbol)

            self.sql=self.sql[:-1]
            print("Data collected from API correctly...")
            if self.execute() is not None:
                print("The exchange currency information has been inserted correctly")
            else:
                print("An Error has happened")
        except:
            print(e)

def main(argv):
    """
       main module initialize the script
    """
    currency_codes=""
    lookback=""
    base_currency=""
    try:
        opts, args = getopt.getopt(argv, 'x', [
                                                  'lookback=',
                                                  'base=',
                                                  'currency='
                                                  ])
    except getopt.GetoptError:
        print("Something goes wrong!!, please review the parameters")
        print ('currency_cal.py --lookback=<lookback> --base=<base_currency> --currency=<currency_codes>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('currency_cal.py --lookback=<lookback> --base=<base_currency> --currency=<currency_codes>')
            sys.exit()
        elif opt in ("--lookback"):
            lookback = arg
        elif opt in ("--base"):
            base_currency = arg
        elif opt in ("--currency"):
            currency_codes = arg
    pc=process_currency(base_currency,lookback,currency_codes)
    pc.load()

if __name__ == "__main__":
    main(sys.argv[1:])
