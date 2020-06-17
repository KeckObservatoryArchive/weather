import os
import hashlib
import urllib.request
import json
import configparser
import pymysql.cursors

def updateWxDb(utDate, column, value, log_writer=''):
    """
    Sends command to update KOA data

    @param utDate:     UT date of processing
    @type utDate:      string (yyyy-mm-dd)
    @param column:     database column to add/update
    @type column:      string
    @param value:      database column value
    @type value:       string
    @param log_writer: logger
    @type log_writer: logging
    """

    user = os.getlogin()
    if user != 'koaadmin':
        if log_writer:
            log_writer.info('update_wx_db.py incorrect user for database update')
        return

    # Database access URL

    dir_path = os.path.dirname(os.path.realpath(__file__))
    config = configparser.ConfigParser()
    config.read(dir_path+'/config.live.ini')
    dbhost = config['DB']['HOST']
    dbuser = config['DB']['USER']
    dbpass = config['DB']['PASS']
    dbdb   = config['DB']['DB']

    try:
        dbConn = pymysql.connect(dbhost, dbuser, dbpass, dbdb, cursorclass=pymysql.cursors.DictCursor)
    except:
        if log_writer:
            log_writer.info('update_wx_db.py could not connect to koa database')
        return

    # Verify if entry exists or not, then update the column requested

    query = f'select * from koawx where utdate="{utDate}"'
    with dbConn.cursor() as cursor:
        num = cursor.execute(query)
        if num == 0:
            query = f'insert into koawx set utdate="{utDate}"'
            if log_writer:
                log_writer.info('update_wx_db.py {}'.format(query))
            cursor.execute(query)
        query = f'update koawx set {column}="{value}" where utdate="{utDate}"'
        if log_writer:
            log_writer.info('update_wx_db.py {}'.format(query))
        cursor.execute(query)

    dbConn.close()
