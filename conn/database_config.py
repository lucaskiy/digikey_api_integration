# -*- coding: utf-8 -*-

import os
from configparser import ConfigParser
from dotenv import load_dotenv


def config():
    load_dotenv()
    creds = {'host': os.getenv('host'),
             'database': os.getenv('database'),
             'user': os.getenv('user'),
             'password': os.getenv('password')}
    return creds


def config2(filename='database.ini', section='postgresql'):
    # other option to executing the same thing
    parser = ConfigParser()
    parser.read(filename)

    db = {}

    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]

    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db
