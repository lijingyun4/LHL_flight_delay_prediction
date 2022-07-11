import psycopg2
import pandas as pd
import os
import json
import requests
from dotenv import load_dotenv

path = os.path.abspath(os.path.join(__file__, "../../../.env"))
DIR_PATH = os.path.abspath(os.path.join(__file__, "../../../data"))


def get_weather_data():
  data_file = os.path.abspath(os.path.join(__file__, "../../../data/weather_data.json"))
  if os.path.exists(data_file):
    print("Found weather data. Loading from data folder")
    file = open(data_file, "r")
    return json.load(file)
  else:
    print("Weather data not found. Making requests ...")
    df = get_data_sample(table_name="flights")
    start_date = df["fl_date"].min()
    end_date = df["fl_date"].max()
    cities = df["origin_city_name"].unique()
    url = "https://api.worldweatheronline.com/premium/v1/past-weather.ashx"
    responses = []

    for city in cities:
      payload = {
        "key": os.getenv("WEATHER_API_KEY"),
        "date": start_date,
        "enddate": end_date,
        "q": city,
        "format": "json",
        "tp": 24
      }
      res = requests.get(url, params=payload)
      if res.status_code == 200:
        responses.append(res.json())
    
    with open(data_file, "w") as f:
      json.dump(responses, f)

    return responses




def get_data_sample(table_name, sample_size=100_000, q=None, forceRetrieve=False):
  """
  Retrieves data from specified table and saves it into a data folder.
  Returns a pandas Dataframe of the data.

  *Note* this function expects a .env file in the root project directory with 
  keys DB, HOSTNAME, USERNAME, PASSWORD in order to connect to the Postgres Database

  sample_size = int: Number of samples to get (LIMIT value)\n
  table_name = str: name of table in database\n
  q = str: Query to the database. Appends a LIMIT to the end of the query\n
  forceRetrieve = bool: Force query to database instead of reading from local file


  """
  data_path = os.path.abspath(os.path.join(__file__, f"../../../data/{table_name}_sample.csv"))
  data_file = os.path.exists(data_path)
  df = None

  if data_file and not forceRetrieve:
    print(f"{table_name}_sample.csv file exists. Retrieving from /data")
    try:
      df = pd.read_csv(data_path)
    except:
      print("Error while reading csv file to pandas dataframe")
  else:
    print("Data file does not exist or retrieving by force. Querying the database...")
    if not load():
      return
    
    if sample_size > 100_000:
      print("Sample size is too large. Max query allowed is 100 000")
      return

    dbparams = dict(database=os.getenv("DB"), user=os.getenv("USERNAME"), password=os.getenv("PASS") ,host=os.getenv("HOSTNAME"))

    query = f"""
    SELECT * FROM {table_name} 
    ORDER BY random()
    LIMIT {sample_size}
    """
    connection = connect_to_db(dbparams)
    if connection:
      try:
        if q:
          df = pd.read_sql_query(q+f" LIMIT {sample_size}", connection)
        else:
          df = pd.read_sql_query(query, connection)
      except:
        print("An error has occured while querying to the database")
        if q:
          print(f"Provided query was : {q}")
      if not os.path.exists(DIR_PATH):
        os.mkdir(DIR_PATH)
      df.to_csv(data_path, index=False)
  return df

def load():
  bool = load_dotenv(path)
  if not bool:
    print("Environment variables failed to load.")
    print(f"Path used was : {path}")
  return bool

def connect_to_db(dbparams):
  connection = None
  try:
    connection = psycopg2.connect(**dbparams)
  except ConnectionError:
    print("Failed to connect to Database")
  except:
    print("Error has occured")
    print("params: ", dbparams)
  return connection
  
def execute_query(query, connection):
  if not "limit" in query.lower():
    print("Please do not query the entire database. Ensure you are adding a LIMIT statement")
    return
  try:
    cursor = connection.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    return data
  except:
    print("Error occured while executing the query.")


def get_working_dataset():
  """
  Create the basic dataset needed for the problems of regression or classification\n
  Creates a csv file in the data folder if it doesnt exist, and returns a pandas dataframe.\n
  Reads from data folder if exists.
  """
  data_file = os.path.join(DIR_PATH, "supervised_flights_sample.csv")
  working_df = None
  if os.path.exists(data_file):
    print("supervised_flights_sample.csv exists. Reading from data folder")
    working_df = pd.read_csv(data_file)
  else:
    df = get_data_sample(table_name="flights_test")
    df_flights = get_data_sample(table_name="flights")

    cols = [
      "arr_delay", "cancelled", "carrier_delay", "weather_delay", "nas_delay", "security_delay", "late_aircraft_delay"
    ]

    working_df = df_flights[list(df.columns) + cols]

    try:
      working_df.to_csv(data_file, index=False)
    except:
      print("Could not write dataframe to csv file")
  return working_df
