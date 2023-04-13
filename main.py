import pandas as pd
import yfinance as yf
import sqlite3
import time
import datetime
from fastapi import FastAPI
from company import tickers_list
from company import tickers_data

app = FastAPI()

new_list=tickers_list;

for ticker_symbol in tickers_list:
        yf_link_csv=(f"https://query1.finance.yahoo.com/v7/finance/download/{ticker_symbol}?period1=1649748822&period2=1681284822&interval=1d&events=history&includeAdjustedClose=true")
        df=pd.read_csv(yf_link_csv)

#create table & connection
conn = sqlite3.connect('stocks.db')

create_sql = "CREATE TABLE IF NOT EXISTS stocks(ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,Symbol TEXT NOT NULL UNIQUE,Date Date,Open Numeric(10,2),High Numeric(10,2),Low Numeric(10,2),Close Numeric(10,2),Adj_Close Numeric(10,2),Volume Numeric(10,2),UNIQUE(ID,Symbol))"
    
cursor = conn.cursor()
cursor.execute(create_sql)


@app.get("/stock_data_for_particular_day")
async def get_data(year,month,day):
    """a. Get all companies stock data for a particular day (Input to API would be date). Output will be displayed in the uvicorn live server console."""       

    period1=int(time.mktime(datetime.datetime(int(year),int(month),int(day),00,00).timetuple()))
    period2=int(time.mktime(datetime.datetime(int(year),int(month),int(day),23,59).timetuple())) 
    print(f'from {period1} to {period2}')
    for ticker in tickers_list:
        ticker_object = yf.Ticker(ticker)

        temp = pd.DataFrame.from_dict(ticker_object.info,orient='index')
        temp.reset_index(inplace=True)
        temp.columns = ["Attribute","Values"]

        tickers_data[ticker] = temp
    
    for ticker_symbol in tickers_list:
        yf_link_csv=(f"https://query1.finance.yahoo.com/v7/finance/download/{ticker_symbol}?period1={period1}&period2={period2}&interval=1d&events=history&includeAdjustedClose=true")
        df=pd.read_csv(yf_link_csv)

    print(tickers_data)

    return{
        "Code" : "Success",
        "Message" : "Data added"
    }

@app.get("/stock_data_for_particular_company_for_particular_day")
async def get_data(company_id,year,month,day):
    """b. Get all stock data for a particular company for a particular day (Input to API would be company ID/name and date). Output will be displayed in the uvicorn live server console."""
    company_id.upper()

    period1=int(time.mktime(datetime.datetime(int(year),int(month),int(day),00,00).timetuple()))
    period2=int(time.mktime(datetime.datetime(int(year),int(month),int(day),23,59).timetuple())) 
    print(f'from {period1} to {period2}')

    ticker_object = yf.Ticker(company_id)

    temp = pd.DataFrame.from_dict(ticker_object.info,orient='index')
    temp.reset_index(inplace=True)
    temp.columns = ["Attribute","Values"]

    tickers_data[company_id] = temp

    yf_link_csv=(f"https://query1.finance.yahoo.com/v7/finance/download/{company_id}?period1={period1}&period2={period2}&interval=1d&events=history&includeAdjustedClose=true")
    df=pd.read_csv(yf_link_csv)
    

    print(tickers_data)

    return{
        "Code" : "Success",
        "Message" : "Data added"
    }

@app.get("/stock_data_for_particular_company")
async def get_data(company_id):
    """c. Get all stock data for a particular company (Input to API would be company ID/name). Output will be displayed in the uvicorn live server console."""
    company_id.upper()

    ticker_object = yf.Ticker(company_id)

    temp = pd.DataFrame.from_dict(ticker_object.info,orient='index')
    temp.reset_index(inplace=True)
    temp.columns = ["Attribute","Values"]

    tickers_data[company_id] = temp

    yf_link_csv=(f"https://query1.finance.yahoo.com/v7/finance/download/{company_id}?period1=1649748822&period2=1681284822&interval=1d&events=history&includeAdjustedClose=true")
    df=pd.read_csv(yf_link_csv)
    

    print(tickers_data)

    return{
        "Code" : "Success",
        "Message" : "Data added"
    }

@app.get('/add_company')
async def add_company(com_sym):
    """ You can add company by company ID or Symbol. Output will be displayed in the uvicorn live server console."""
    new_list.append(com_sym.upper())

    for ticker in new_list:
        ticker_object = yf.Ticker(ticker)

        temp = pd.DataFrame.from_dict(ticker_object.info,orient='index')
        temp.reset_index(inplace=True)
        temp.columns = ["Attribute","Values"]

        tickers_data[com_sym] = temp

    for ticker in new_list:
        yf_link_csv=(f"https://query1.finance.yahoo.com/v7/finance/download/{ticker}?period1=1649748822&period2=1681284822&interval=1d&events=history&includeAdjustedClose=true")
        df=pd.read_csv(yf_link_csv)
    

    print(tickers_data)

    return{
        "Code" : "Success",
        "Message" : "Company added"
    }

@app.post('/update_local_stock_data_for_company')
async def insert_data():    
    """d. POST API to update stock data for a company by date. If it shows error that means stocks is already added in the table."""
    for row,sym in zip(df.itertuples(),new_list):
        insert_sql=f"INSERT INTO stocks(Symbol,Date,Open,High,Low,Close,Adj_Close,Volume) VALUES ('{sym}','{row[1]}',{row[2]},{row[3]},{row[4]},{row[5]},{row[6]},{row[7]})"
        cursor.execute(insert_sql);
        
    conn.commit()

    return{
        "Code" : "Success",
        "Message" : "Data Inserted into Table"
    }