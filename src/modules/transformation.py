from sklearn.preprocessing import StandardScaler, OrdinalEncoder, FunctionTransformer
import pandas as pd
import numpy as np

def log_transform(df, cols):
  """
  Apply a log transformation on a data frame. Operates on dataframe in place

  cols:  List of column names to use the log transform on
  """

  subset = df[cols]

  transformer = FunctionTransformer(np.log1p)
  subset = transformer.fit_transform(subset)
  try:
    results = transformer.fit_transform(subset)
    df[cols] = pd.DataFrame(results)
    return df
  except:
    print("Error while trying to transform data. Make sure all columns are numeric")


def standard_scale(df, cols):

  """
  Apply a standard scaler to the given cols on a dataframe. Operates on the dataframe inplace

  cols: List of column names to use the standard scaler on\n
  log: bool: Apply a log transformation before scaling
  """
    
  scaler = StandardScaler()
  subset = df[cols]

  try:
    results = scaler.fit_transform(subset)
    df[cols] = pd.DataFrame(results)
    return df
  except:
    print("Error while trying to scale data. Make sure all columns are numeric")

def ordinal_encode(df, cols, mapping):
  """
  Apply ordinal encoding to the selected columns. Columns should be categorical. Acts on dataframe in place.

  cols: List of columns names to ordinally encode

  mapping: List should contain categorical values in
  order from least to greatest, same order as cols list

  ex:\n
  cols = ["Rank", "Size"]\n
  mapping= [
    ["Last", "Third", "Second", "First"],
    ["Small", "Medium", "Large"]
  ]
  """

  encoder = OrdinalEncoder(categories=mapping)
  try:
    results = encoder.fit_transform(df[cols])
    df[cols] = pd.DataFrame(results)
    return df
  except:
    print("Error while trying to encode the data. Make sure columns are categorical")


def hot_encode(df, cols):

  """
  Apply hot encoding to the dataframe. Acts on dataframe in place.

  cols: List of column names. (categorical)
  """

  for col in cols:
    dummies = pd.get_dummies(df[col], drop_first=True)
    df = pd.concat((df, dummies), axis=1)
  try:
    df = df.drop(columns=cols)
    return df
  except:
    print("Error while dropping columns. Please check column names")
  
  
