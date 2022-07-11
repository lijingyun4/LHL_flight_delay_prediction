import sys, os

sys.path.append(os.path.abspath(os.path.join(__file__, "../..")))

from modules.data_gathering import get_data_sample, get_working_dataset
from pandas import DataFrame




def test_data_sample():
  a = get_data_sample("flights_test", 100_000)
  try: 
    assert isinstance(a, DataFrame)
    print("Passed")
    assert len(a) == 100_000
  except AssertionError:
    print("One of the tests has failed: ")

def test_working_dataset():
  a = get_working_dataset()
  try:
    assert len(a.columns) == 27
    print("Passed")
    assert "arr_delay" in a.columns
    print("Passed")
    assert len(a) > 0 and len(a) <= 100_000
    print("Passed")
  except AssertionError:
    print("One of the tests has failed")

if __name__ == "__main__":
  test_data_sample()
  test_working_dataset()