import yfinance as yf   
import streamlit as st

st.write("""
#simple stock price app
shown are the stock closing price and volume of google
""")
tickerSymbol='GOOGL'
tickerData=yf.Ticker(tickerSymbol)
df=tickerData.history(period='1d',start='2019-5-31',end='2019-12-31')
st.line_chart(df.Close)

st.line_chart(df.Volume)