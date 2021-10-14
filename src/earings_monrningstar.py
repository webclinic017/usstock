import pandas as pd
url='http://www.morningstar.com/earnings/earnings-calendar.aspx'
ret = pd.read_html(url)
df = ret[1]
vct= df[u'Company Name']
tkLst = [str(x.split()[-1]) for x in vct]
print(tkLst)
