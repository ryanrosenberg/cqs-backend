import firebase_admin
from firebase_admin import firestore
import sqlite3 as sq
import pandas as pd
import numpy as np
import utils
import pickle

# Application Default credentials are automatically created.
app = firebase_admin.initialize_app()
db = firestore.client()


def slug(str):
    new_str = str.lower().replace(" ", "-")
    return new_str


def string(int):
    new_str = str(int).replace(".0", "")
    return new_str

to_delete = ['hector-rathburn', 
             'ben-chapman']

for slug in to_delete:
    print(slug)
    db.collection("players").document(slug).delete()