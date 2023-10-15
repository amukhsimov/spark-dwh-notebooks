import os
import importlib.util
import pandas as pd
import numpy as np
import pandas as pd
import cx_Oracle
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
#import amukhsimov_jupyter_templates_bigdata as bigdata
from IPython.display import clear_output
from copydf import copyDF
import matplotlib as mpl
import matplotlib.pyplot as plt

mpl.rcParams.update(mpl.rcParamsDefault)
# mpl.style.available
# seaborn-whitegrid
# fivethirtyeight
# bmh
#plt.style.use("seaborn-whitegrid")

pd.options.display.max_columns = 100
pd.options.display.max_rows = 200
pd.set_option('display.float_format', lambda x: f'{x:,.2f}')

mynameis = input("Input your login: ")
master_password = input("Input master password: ")

if os.path.isfile(os.path.join(JUPYTER_DIR, mynameis, '__init__.py')):
    execfile(os.path.join(JUPYTER_DIR, mynameis, '__init__.py'))

execfile(os.path.join(JUPYTER_DIR, 'scripts', '__environment__.py'), globals(), locals())
execfile(os.path.join(JUPYTER_DIR, 'scripts', '__functions__.py'), globals(), locals())
clear_output()
# execfile("/jupyter/scripts/__data__.py", globals(), locals())
