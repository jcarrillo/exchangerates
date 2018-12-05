# exchangerates
Sample project to extract information from fixer.io and store the result on a DB


   Owner: Jhon Carrillo
   
   
   currency_cal.py --lookback=<lookback> --base=<base_currency> --currency=<currency_codes>
   
   --lookback: Years to analyze
   --base: Base currency
   --currency: Set of currency codes
   
   sample: python currency_cal.py --lookback=2 --base=EUR --currency=USD,AUD,CAD,PLN,MXN

