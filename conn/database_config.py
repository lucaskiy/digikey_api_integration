# -*- coding: utf-8 -*-
import os
from dotenv import load_dotenv


def config():
    load_dotenv()
    creds = {'host': os.getenv('host'),
             'database': os.getenv('database'),
             'user': os.getenv('user'),
             'password': os.getenv('password')}
    return creds
