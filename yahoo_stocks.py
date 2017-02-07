#!/usr/bin/env python
"""
Import historical stock/index/ETF data into pandas from Yahoo! Finance.

Gets these variables: ['Open','High','Low','Close','Volume','Adj Close'].
Also calculates ['Dividend','ShareMultiplier','TotalValue'].

'ShareMultiplier' is "How many shares would I have if I bought 1 share on start_date?"

'TotalValue' is "How much cash would I have if I bought 1 share on start_date,
saved all the cash dividends I received, and sold all my shares today?"
"""
import pandas as pd
import numpy as np
import datetime as dt


def make_url(
	symbol,
	start_date	= '2000-1-1',
	stop_date	= dt.date.today(),
	freq		= 'd'):
	'''
	Create url to download raw CSV data from Yahoo! Finance.
	start_date and stop_date can be any format recognized by pd.to_datetime().
	freq must be one of ['d','w','m'] meaning daily, weekly, monthly.
	'''
	
	symbol 		= symbol.upper()
	start_date 	= pd.to_datetime(start_date)
	stop_date 	= pd.to_datetime(stop_date)
	
	params = dict()
	params['s'] = symbol
	params['a'] = start_date.month - 1
	params['b'] = start_date.day
	params['c'] = start_date.year
	params['d'] = stop_date.month - 1
	params['e'] = stop_date.day
	params['f'] = stop_date.year
	params['g'] = freq				
	params['y'] = str(0)
	params['z'] = str(30000)
	
	yurl = 'http://real-chart.finance.yahoo.com/x?'
	for key in sorted(params.keys()):
	    yurl += '&' + key + '=' + str(params[key])
	
	return yurl

def get_table(*args,**kwargs):
	'''
	Get raw data from Yahoo! Finance, clean it up, and return a DataFrame.
	Inputs should be same as url() function.
	Index will be DatetimeIndex of selected frequency.
	CAUTION: Only includes days on which exchange was open! 
	'''

	# Get raw table
	url 	= make_url(*args,**kwargs)
	raw_df 	= pd.read_csv(url)

	# Rename 'Adj Close' -> 'AdjClose'
	raw_df 	= raw_df.rename(columns={'Adj Close':'AdjClose'})

	# Last 4 rows are metadata we don't need
	raw_df = raw_df.iloc[0:-4]
	
	# Convert 'Date' column to datetime if possible
	def ymd2datetime(x):
		try:
			return pd.to_datetime(x,format='%Y%m%d')
		except:
			return x
	
	raw_df['Date'] = raw_df['Date'].apply(ymd2datetime)
	
	# Extract rows with invalid dates and set them aside for now.
	# Sort everything else in chronological order.
	def is_datetime(x):
		return isinstance(x,dt.datetime)
	
	fValidDate 	= raw_df['Date'].apply(is_datetime)
	df 			= raw_df.loc[fValidDate,:].set_index('Date').sort_index()
	extra_stuff = raw_df.loc[~fValidDate,:]
	
	# Get splits and dividends from [extra_stuff].
	# Columns outside 0:3 are garbage, so delete them.
	# Dates are in the wrong column and might be stored as floats.
	extra_stuff 		= extra_stuff.iloc[:,0:3]
	extra_stuff.columns = ['EventType','Date','Value']
	extra_stuff['Date'] = extra_stuff['Date'].astype(int).astype(str).apply(ymd2datetime)
	extra_stuff 		= extra_stuff.set_index('Date').sort_index()

	# Do we need to adjust for dividends and splits?
	event_types = extra_stuff['EventType'].unique()
	has_events 	= len(event_types) > 0

	# Store dividends, if they exist
	df['Dividend'] = 0.0
	if has_events:
		if ('DIVIDEND' in event_types):
			dividends 		= extra_stuff.loc[extra_stuff['EventType']=='DIVIDEND','Value']
			df['Dividend'] 	= dividends.astype(float).reindex(df.index).fillna(0)
	
	# Splits are stored as strings, e.g. '2:1'. That's not very helpful.
	df['ShareMultiplier'] = 1.0
	if has_events:
		if 'SPLIT' in event_types:
			splits 	= extra_stuff.loc[extra_stuff['EventType']=='SPLIT','Value'].astype(str)	
			def split_ratio(x):
				parts = x.split(':')
				return float(parts[0]) / float(parts[1])
			split_factor 			= splits.apply(split_ratio).reindex(df.index).fillna(1.0)
			df['ShareMultiplier'] 	= split_factor.cumprod()

	# Calculate value (at closing time) if you had bought 1 share on start_date.
	# Caution: does not adjust dividends for time value.
	total_dividends 	= (df['ShareMultiplier'] * df['Dividend']).cumsum()
	df['TotalValue']	= df['ShareMultiplier'] * df['Close'] + total_dividends
	
	# Ensure that all values stored as floats
	df = df.astype(float)
	
	return df

def load(
	symbol_list,
	start_date	= '2000-1-1',
	stop_date	= dt.date.today(),
	freq		= 'd',
	verbose		= True):
	'''
	Get data for multiple symbols. Returns a dictionary of DataFrames.
	Each DataFrame stores one variable (e.g 'TotalValue') for all symbols.
	symbol_list should be a list of strings, e.g. ['SPY','AAPL','^GSPC']
	Set verbose = False to disable printing to screen.
	'''
	
	start_date 	= pd.to_datetime(start_date)
	stop_date	= pd.to_datetime(stop_date)
	
	tables = dict()
	if verbose: print( "Loading symbols" )
	for symbol in symbol_list:
	    if verbose: print( symbol )
	    tables[symbol] = get_table(symbol,start_date,stop_date,freq)
	if verbose: print( "All symbols loaded.\n" )
	
	p = pd.Panel(tables)
	p = p.transpose(2,1,0)
	
	return dict(p)