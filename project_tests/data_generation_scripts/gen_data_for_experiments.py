import sys, string
from random import choice
import random
from string import ascii_lowercase
from scipy.stats import beta, uniform
import numpy as np
import struct
import pandas as pd

import data_gen_utils
import math

TEST_BASE_DIR = "/Users/joker/Coding/cs165-2019/test_data"

def generateDataFileExperiment():
    outputFile = TEST_BASE_DIR + '/data_for_selectivity.csv'
    header_line = data_gen_utils.generateHeaderLine('db1', 'tbl1', 2)
    column1 = list(range(0,100000))
    column2 = list(range(0,100000))
    #### For these 3 tests, the seed is exactly the same on the server.
    np.random.seed(47)
    np.random.shuffle(column2)
    #outputTable = np.column_stack((column1, column2)).astype(int)
    outputTable = pd.DataFrame(list(zip(column1, column2)), columns =['col1', 'col2'])
    outputTable.to_csv(outputFile, sep=',', index=False, header=header_line, line_terminator='\n')

    return outputTable
    
generateDataFileExperiment()
