from flask_restful import Resource

import sqlite3 as sq
import pandas as pd
import numpy as np
import pickle


class Records(Resource):
    def get(self):

        with open('record-book.pkl', 'rb') as f:
            records = pickle.load(f)

        return records
