import pandas as pd
import numpy as np
import datetime
import bisect
from pandas.tseries.offsets import MonthEnd
from openpyxl import load_workbook

import gc
import multiprocessing as mp
import resource

# Constants
xl = '/media/sf_sharedvm/sensitive/DepositAverageLife2.xlsx'
xl = pd.ExcelFile(xl)
lastDate = pd.to_datetime('today')

# Average product life constants
BISECT=20 # bisect stepper (increments of 5%)
avgLifeDict = {'mortality':   [],
                'months': [],
                'averagelife': []}

# Measure memory usage
def mem():
    print('Memory usage         : % 2.2f MB' % round(
        resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024.0,1)
    )

print("Starting memory: ", mem())

# Excel writer
outputPath = '/media/sf_sharedvm/sensitive/dal_pandas_new.xlsx'
writer = pd.ExcelWriter(outputPath, engine='xlsxwriter')

# Open DAL excel sheet
df = xl.parse(sheetname=2,parse_cols="A:F") # Acct_Open_Date, Acct_Closed_Date, CIF_Type, Currency, Entity, BCPS_Segmentation
NUMBER_ACCOUNTS = df.shape[0] # df row count

print("Memory after loading excel book: ", mem())

# Generate unique filters
uniqueFilters = df[['CIF_Type', 'Currency', 'Entity', 'BCPS_Segmentation']].drop_duplicates()

# DAL computation
def dal(df, dalFilter0, dalFilter1, dalFilter2, dalFilter3):
  def defragment(x):
    values = x.dropna().values
    return pd.Series(values, index=dp.columns[:len(values)])

  # Generate Account_Number series
  df['Account_Number'] = df.index + 1

  # Compute and add the Opening Month and Closing Month fields
  df['Opening Month'] = df['Acct_Open_Date'] # + MonthEnd()
  df['Closing Month'] = df['Acct_Closed_Date'] # + MonthEnd()

  # Filter rows by 'CIF_Type', 'Currency', 'Entity', 'BCPS_Segmentation'
  df = df[(df.loc[:, 'CIF_Type'] == dalFilter0) &
          (df.loc[:, 'Currency'] == dalFilter1) &
          (df.loc[:, 'Entity'] == dalFilter2) &
          (df.loc[:, 'BCPS_Segmentation'] == dalFilter3)
          ]

  # test if filter generates a NoneType object (i.e. whether there is data)
  try: 
    # Create date range dataframe
    # daterange = pd.DataFrame({'daterange' : pd.date_range(start = df.loc[:, 'Opening Month'].min(),
    #                                                   end = df.loc[:, 'Closing Month'].max(),
    #                                                   freq = 'M'),
    #                           'Account_Number' : 1})
    daterange = pd.DataFrame({'daterange' : pd.date_range(start = df.loc[:, 'Opening Month'].min(),
                                                  end = lastDate,
                                                  freq = 'M'),
                          'Account_Number' : 1})

    # Fill NaT with max datetime (or next period)
    df = df.fillna(pd.to_datetime('20990101'))

    # Create NUMBER_ACCOUNTS multiples of the daterange and concatenate
    daterange10 = pd.concat([daterange]*NUMBER_ACCOUNTS)

    # Generate a 'daterange' for each account number
    daterange10.loc[:, 'Account_Number'] = daterange10.groupby('daterange').cumsum()

    # Merge df with daterange10
    df = df.merge(daterange10,
                  how = 'inner',
                  on = 'Account_Number')

    # Limit rows to when 'Opening Month' is <= 'daterange' AND 'Closing Month' is >= 'daterange'
    df = df[(df.loc[:, 'Opening Month'] <= df.loc[:, 'daterange']) &
            (df.loc[:, 'Closing Month'] > df.loc[:, 'daterange'])]

    # Pivot on 'Opening Month', 'daterange'; count unique 'Account_Number'; fill NA with 0
    dp = df.pivot_table(index = 'Opening Month',
                  columns = 'daterange',
                  values = 'Account_Number',
                  aggfunc = pd.Series.nunique)

    long_index = pd.MultiIndex.from_product([dp.index, dp.columns])
    df = dp.stack().groupby(level='Opening Month').apply(defragment).reindex(long_index).unstack().fillna("") 

    # Rename the columns as a series range from 0 upwards
    df = df.rename(columns={x:y for x,y in zip(df.columns,range(0,len(df.columns)))})
    
    # Return final dataframe
    return df

  except:
    print("Empty Dataframe")

def main():
  # Iterate through the filter combinations
  global avgLifeDict
  for dalFilter in uniqueFilters.to_records(index=False):
    sheetName=' '.join(dalFilter)
    print('Processing ', sheetName)
    dalResult = dal(df, dalFilter[0], dalFilter[1], dalFilter[2], dalFilter[3])

    if dalResult is not None: # Skip empty dataframes
      dalResult.to_excel(writer, sheet_name=sheetName)

      try:
        ### Some number crunching! ###

        dalResulttmp = dalResult.replace('', np.nan)    
        dalResulttmp = dalResulttmp.dropna(axis=1, how='all')

        # Generate a list containing last non-NaN value of each column
        lastValues = dalResulttmp.replace('', np.nan).apply(lambda column: column.dropna(axis=0, how='all').values[-1]).tolist()

        # Generate a list containing sum of each column
        columnSums = dalResulttmp.replace('', np.nan).apply(lambda column: column.dropna().sum()).tolist()

        # Temporary lists to compute the average product life
        list1 = lastValues
        list2 = columnSums
        list3 = [None]
        list4 = [None]
        list5 = [None]
        avgPrdLife = []
        lengthList=len(list1)

        for i in range(1,lengthList):
          list3.append(list2[i-1] - list2[i] - list1[i-1])

        for i in range(1,lengthList):
          list4.append((list3[i] / list2[i-1])*100)

        list5.append(list4[1])

        for i in range(2,lengthList):
          list5.append(float("{0:.2f}".format(list5[i-1] + list4[i])))

        # avgPrdLife = bisect.bisect(list5, BISECT) - 1
        ctrbisect = 0
        for i in range(BISECT):
          ctrbisect += 5
          avgPrdLife.append(bisect.bisect(list5, ctrbisect) - 1)

        # Populate the avgLifeDict for summary page
        avgLifeDict['mortality'].append(sheetName)
        avgLifeDict['months'].append(dalResulttmp.shape[1])
        avgLifeDict['averagelife'].append(avgPrdLife)

        print(avgLifeDict)

        ### End of number crunching! ###
      except:
        print("Crunching Error")

      print("End of loop: ", mem())

      # Release memory
      lst = [dalResult]
      del dalResult
      del lst
      lst2 = [dalResulttmp]
      del dalResulttmp
      del lst2
      gc.collect()

      print("After garbage collection: ", mem())

  # Save workbook
  writer.save()
  writer.close()

  # Produce summary into a separate sheet
  print(avgLifeDict)
  dfAvgLife = pd.DataFrame.from_dict(avgLifeDict)
  dfAvgLife.set_index('mortality', inplace=True)

  dfAvgLife = pd.concat([dfAvgLife['months'], dfAvgLife['averagelife'].apply(pd.Series)], axis = 1)
  dfAvgLife.columns = ['months', '5%', '10%', '15%', '20%', '25%', '30%', '35%', '40%', '45%', '50%', '55%', '60%', '65%', '70%', '75%', '80%', '85%', '90%', '95%', '100%']

  book = load_workbook(outputPath)
  writerSummary = pd.ExcelWriter(outputPath, engine='openpyxl')
  writerSummary.book = book
  dfAvgLife.to_excel(writerSummary, sheet_name='Summary')
  writerSummary.save()
  writerSummary.close()

if __name__ == "__main__":
  main()

