#!/usr/bin/env python3
""" Good test version
Black-Litterman portfolio optimization
Use expected returns, covarance matrix, risk-free rate and market-caps to calc optimized weights
Ref. https://github.com/dkensinger/python/blob/master/Black-Litterman%20portfolio%20optimization.py
literature: http://www.quantandfinancial.com/2013/08/black-litterman.html
TBD:
from _alan_calc import sqlQuery,pd,json;df=sqlQuery("select name,pbdate,close from prc_hist_iex where name in ('AAPL','MDB','IBM') and pbdate>20170901 ORDER BY pbdate").dropna();df = df.pivot_table(index='pbdate',columns='name',values='close').pct_change();print df.corr(, file=sys.stderr)
"""
from __future__ import print_function
import sys
if sys.version_info.major == 2:
	from pyEX import batchDF
else:
	from pyEX import bulkBatchDF as batchDF
from numpy import matrix, array, zeros, empty, sqrt, ones, dot, append, mean, cov, transpose, linspace
from numpy.linalg import inv, pinv
from pylab import *
#from structures.quote import QuoteSeries
import scipy.optimize
import random
import pandas_datareader.data as web
import datetime
import pandas as pd
import numpy as np
from pandas.io.json import json_normalize

####################################
# Helper Functions
####################################

def load_data_iex(tkLst=[]):
	if len(tkLst)<3:
		tkLst = ['SPY','AAPL', 'AXP', 'BA', 'CAT', 'CSCO', 'CVX', 'DIS', 'DWDP', 'GS', 'HD', 'IBM', 'INTC', 'JNJ', 'JPM', 'KO', 'MCD', 'MMM', 'MRK', 'MSFT', 'NKE', 'PFE', 'PG', 'TRV', 'UNH', 'UTX', 'V', 'VZ', 'WBA', 'WMT', 'XOM']
		#tkLst=['SPY','AAPL','GOOG','JPM','MSFT','GE','JNJ','AMZN','GS','TSM','JD','MCD']
	statx = batchDF(tkLst, types='stats')['stats']
	prdx = batchDF(tkLst, types='chart', _range='1y')['chart']
	prcs = prdx.pivot_table(index='date',columns=['KEY'],values=['close'])
	prcs.columns = prcs.columns.droplevel()
	rtns = prcs.apply(pd.Series.pct_change).dropna()
	datD={}
	
	capsV = [0 if x not in statx.index else statx.loc[x]['marketcap'] for x in tkLst]
	corrM = rtns.corr().values
	covM = rtns.cov().values
	stdV = rtns.std().values
	prcM = prcs.T.values
	rtnM = rtns.T.values
	datD = dict(names=tkLst,caps=capsV,corrM=corrM,covM=covM,stdV=stdV,prices=prcM,rtns=rtnM)
	return datD
		
def load_data_net():
	symbols = ['XOM', 'AAPL', 'MSFT', 'JNJ', 'GE', 'GOOG', 'CVX', 'PG', 'WFC']
	cap = {'SPY':14.90e12, 'XOM':403.02e9, 'AAPL':392.90e9, 'MSFT':283.60e9, 'JNJ':243.17e9, 'GE':236.79e9, 'GOOG':292.72e9, 'CVX':231.03e9, 'PG':214.99e9, 'WFC':218.79e9}
	n = len(symbols)
	start = datetime.datetime(2018,7,1)
	end = datetime.datetime(2018,10,10)
	prices_out, caps_out = [], []
	for s in symbols:
		print("Reading symbol %s" % s, file=sys.stderr)
		q = web.DataReader(s, 'iex',start=start, end=end)
		prices = q['close'].sort_index().values
		prices_out.append(prices)
		caps_out.append(cap[s])
	return symbols, prices_out, caps_out

# Function loads historical stock prices of nine major S&P companies and returns them together
# with their market capitalizations, as of 2013-07-01
# def load_data():
#	 symbols = ['XOM', 'AAPL', 'MSFT', 'JNJ', 'GE', 'GOOG', 'CVX', 'PG', 'WFC']
#	 cap = {'SPY':14.90e12, 'XOM':403.02e9, 'AAPL':392.90e9, 'MSFT':283.60e9, 'JNJ':243.17e9, 'GE':236.79e9, 'GOOG':292.72e9, 'CVX':231.03e9, 'PG':214.99e9, 'WFC':218.79e9}
#	 n = len(symbols)
#	 prices_out, caps_out = [], []
#	 for s in symbols:
#		 print("Reading symbol %s" % s, file=sys.stderr)
#		 q = QuoteSeries.loadfromfile(s, 'data/black_litterman/%s.csv' % s)
#		 prices = q.getprices()[-500:]
#		 prices_out.append(prices)
#		 caps_out.append(cap[s])
#	 return symbols, prices_out, caps_out

# Function takes historical stock prices together with market capitalizations and calculates
# names       - array of assets' names
# prices      - array of historical (daily) prices
# caps	- array of assets' market capitalizations
# returns:
# names       - array of assets' names
# weights     - array of assets' weights (derived from mkt caps)
# expreturns  - expected returns based on historical data
# covars	  - covariance matrix between assets based on historical data
def assets_meanvar(names, prices, caps, returns):
	prices = matrix(prices)			 # create numpy matrix from prices
	weights = array(caps) / float(sum(caps)) # create weights

	rows, cols = returns.shape
	if returns.size<1:
		# create matrix of historical returns
		rows, cols = prices.shape
		returns = empty([rows, cols-1])
		for r in range(rows):
			for c in range(cols-1):
				p0, p1 = prices[r,c], prices[r,c+1]
				returns[r,c] = (p1/p0)-1

	# calculate expected returns
	expreturns = array([])
	for r in range(rows):
		expreturns = append(expreturns, mean(returns[r]))
	# calculate covariances
	covars = cov(returns)

	expreturns = (1+expreturns)**250-1      # Annualize expected returns
	covars = covars * 250			   # Annualize covariances

	return names, weights, expreturns, covars

#       rf	      risk free rate
#       lmb	     lambda - risk aversion coefficient
#       C	       assets covariance matrix
#       V	       assets variances (diagonal in covariance matrix)
#       W	       assets weights
#       R	       assets returns
#       mean    portfolio historical return
#       var	     portfolio historical variance
#       Pi	      portfolio equilibrium excess returns
#       tau     scaling factor for Black-litterman

# Calculates portfolio mean return
def port_mean(W, R):
	return sum(R*W)

# Calculates portfolio variance of returns
def port_var(W, C):
	return dot(dot(W, C), W)

# Combination of the two functions above - mean and variance of returns calculation
def port_mean_var(W, R, C):
	return port_mean(W, R), port_var(W, C)

# Given risk-free rate, assets returns and covariances, this function calculates
# mean-variance frontier and returns its [x,y] points in two arrays
def solve_frontier(R, C, rf):
	def fitness(W, R, C, r):
		# For given level of return r, find weights which minimizes
		# portfolio variance.
		mean, var = port_mean_var(W, R, C)
		# Big penalty for not meeting stated portfolio return effectively serves as optimization constraint
		penalty = 50*abs(mean-r)
		return var + penalty
	frontier_mean, frontier_var, frontier_weights = [], [], []
	n = len(R)      # Number of assets in the portfolio
	for r in linspace(min(R), max(R), num=20): # Iterate through the range of returns on Y axis
		W = ones([n])/n	 # start optimization with equal weights
		b_ = [(0,1) for i in range(n)]
		c_ = ({'type':'eq', 'fun': lambda W: sum(W)-1. })
		optimized = scipy.optimize.minimize(fitness, W, (R, C, r), method='SLSQP', constraints=c_, bounds=b_)
		if not optimized.success:
			raise BaseException(optimized.message)

		# add point to the min-var frontier [x,y] = [optimized.x, r]
		frontier_mean.append(r)						 # return
		frontier_var.append(port_var(optimized.x, C))   # min-variance based on optimized weights
		frontier_weights.append(optimized.x)
	return array(frontier_mean), array(frontier_var), frontier_weights

# Given risk-free rate, assets returns and covariances, this
# function calculates weights of tangency portfolio with respect to
# sharpe ratio maximization
def solve_weights(R, C, rf):
	def fitness(W, R, C, rf):
		mean, var = port_mean_var(W, R, C)      # calculate mean/variance of the portfolio
		util = (mean - rf) / sqrt(var)	  # utility = Sharpe ratio
		return 1/util					   # maximize the utility, minimize its inverse value
	n = len(R)
	W = ones([n])/n					 # start optimization with equal weights
	b_ = [(0.,1.) for i in range(n)]	# weights for boundaries between 0%..100%. No leverage, no shorting
	c_ = ({'type':'eq', 'fun': lambda W: sum(W)-1. })       # Sum of weights must be 100%
	optimized = scipy.optimize.minimize(fitness, W, (R, C, rf), method='SLSQP', constraints=c_, bounds=b_)
	if not optimized.success:
		raise BaseException(optimized.message)
	return optimized.x

def print_assets(names, W, R, C,file=sys.stderr):
	print("%-10s %6s %6s %6s %s" % ("Name", "Weight", "Return", "Dev", " Correlations"), file=file)
	for i in range(len(names)):
		xstr = "{:10s} {:5.1f}% {:5.1f}% {:5.1f}% ".format(names[i], 100*W[i], 100*R[i], 100*C[i,i]**.5)
		file.write(xstr)
		for j in range(i+1):
			corr = C[i,j] / (sqrt(C[i,i]) * (sqrt(C[j,j]))) # calculate correlation from covariance
			file.write("%.2f " % corr)
		print("", file=sys.stderr)

def optimize_and_display(title, names, R, C, rf, color='black'):
	# optimize
	W = solve_weights(R, C, rf)
	mean, var = port_mean_var(W, R, C) # calculate tangency portfolio
	f_mean, f_var, f_weights = solve_frontier(R, C, rf) # calculate min-var frontier

	# display min-var frontier
	print(title, file=sys.stderr)
	print_assets(names, W, R, C, file=sys.stderr)
	n = len(names)
	scatter([C[i,i]**.5 for i in range(n)], R, marker='x',color=color) # draw assets
	for i in range(n): # draw labels
		text(C[i,i]**.5, R[i], '  %s'%names[i], verticalalignment='center', color=color)
	scatter(var**.5, mean, marker='o', color=color)	 # draw tangency portfolio
	if debugTF is True:
		plot(f_var**.5, f_mean, color=color) # draw min-var frontier
		xlabel('$\sigma$'), ylabel('$r$')
		grid(True)
		# show()

	# Display weights
	#m = empty([n, len(f_weights)])
	#for i in range(n):
	#       for j in range(m.shape[1]):
	#	       m[i,j] = f_weights[j][i]
	#stackplot(f_mean, m)
	#show()
	return (names, R, W, C)

# given the pairs of assets, prepare the views and link matrices. This function is created just for users' convenience
def prepare_views_and_link_matrix(names, views):
	r, c = len(views), len(names)
	Q = [views[i][3] for i in range(r)] # view matrix
	P = zeros([r, c]) # link matrix
	nameToIndex = dict()
	for i, n in enumerate(names):
		nameToIndex[n] = i
	for i, v in enumerate(views):
		name1, name2 = views[i][0], views[i][2]
		P[i, nameToIndex[name1]] = +1 if views[i][1]=='>' else -1
		P[i, nameToIndex[name2]] = -1 if views[i][1]=='>' else +1
	return array(Q), P

def bl_example(tkLst=[]):
	"""
	Example of Black-Litterman portfolio optimization
	"""
	# INPUTS: Load names, prices, capitalizations from the data source(yahoo finance)
	# Note,
	# 1. input names should be dynamically assigned
	# 2. scaling factor [tau] & risk-free rate [rf] should use 1.0/n**2, TSY-3MO respectively
	datD = load_data_iex(tkLst)
	names, prices, caps, rtns  = (datD['names'],datD['prices'],datD['caps'],datD['rtns'])
	corrM, covM, stdV  = (datD['corrM'],datD['covM'],datD['stdV'])
	#names, prices, caps = load_data_net()
	n = len(names)
	tau = 1.0/n/n # scaling factor, dynamically assign rather than a constant 0.025
	rf = .020     # Risk-free rate
	views = [
		('MSFT', '>', 'GE', 0.02),
		('AAPL', '<', 'JNJ', 0.02)
		]
	views = []
	#quit()
	# Estimate assets's expected return and covariances
	# Note,
	# 1. returns calculation can be monthly rather than daily returns
	# 2. Naive mean/variance calculation, can be re-calc via ARIMA/ARCH models with forecasts 
	names, W, R, C = assets_meanvar(names, prices, caps, rtns)

	print("Historical Weights", file=sys.stderr)
	print_assets(names, W, R, C, file=sys.stderr)
	mW=W

	# Calculate portfolio historical return and variance
	mean, var = port_mean_var(W, R, C)

	try:
		# Mean-Variance Optimization (based on historical returns)
		(names, R, W, C) = optimize_and_display('Optimization based on Historical returns', names, R, C, rf, color='red')
		if debugTF==True:
			show()

		# Black-litterman reverse optimization
		lmb = (mean - rf) / var	 # Calculate return/risk trade-off
		Pi = dot(dot(lmb, C), W) # Calculate equilibrium excess returns

		# Mean-variance Optimization (based on equilibrium returns)
		(names, R, W, C) = optimize_and_display('Optimization based on Equilibrium returns', names, Pi+rf, C, rf, color='green')
		if debugTF==True:
			show()
	except BaseException as e:
		#except Exception as e:
		print('**ERROR: {}'.format(str(e)), file=sys.stderr)

	dn = pd.DataFrame({'Ticker':names})
	dw = pd.DataFrame(np.array([W,mW,R]).transpose().round(4),columns=['Weight','MktWgh','RateOfReturn'])
	dc = pd.DataFrame(np.sqrt(C),columns=[x+'_sd' for x in names])
	df = pd.concat([dn, dw, dc], axis=1)

	# Determine views to the equilibrium returns and prepare views (Q) and link (P) matrices
	if len(views)<1:
		print(df, file=sys.stderr)
		return df

	Q, P = prepare_views_and_link_matrix(names, views)
	print('Views Matrix', file=sys.stderr)
	print(Q, file=sys.stderr)
	print('Link Matrix', file=sys.stderr)
	print(P, file=sys.stderr)


	# Calculate omega - uncertainty matrix about views
	omega = dot(dot(dot(tau, P), C), transpose(P)) # 0.025 * P * C * transpose(P)
	# Calculate equilibrium excess returns with views incorporated
	sub_a = inv(dot(tau, C))
	sub_b = dot(dot(transpose(P), inv(omega)), P)
	sub_c = dot(inv(dot(tau, C)), Pi)
	sub_d = dot(dot(transpose(P), inv(omega)), Q)
	Pi = dot(inv(sub_a + sub_b), (sub_c + sub_d))

	# Mean-variance Optimization (based on equilibrium returns)
	optimize_and_display('Optimization based on Equilibrium returns with adjusted views', names, Pi+rf, C, rf, color='blue')
	if debugTF==True:
		show()
	df = pd.DataFrame({'ticker':names,'weight':W,'rrt':R})
	print(df,file=sys.stderr)
	return df

####################################
debugTF=False
if __name__ == '__main__':
	tkLst = sys.argv[1:] if len(sys.argv)>3 else []
	df = bl_example(tkLst)
