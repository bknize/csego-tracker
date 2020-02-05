from flask import Flask, g
from flask_restful import Resource, Api
import requests
from requests.auth import HTTPBasicAuth
import sqlite3
from datetime import date
import json
import time
import atexit
from apscheduler.schedulers.background import BackgroundScheduler


app = Flask(__name__)
api = Api(app)

url = "https://dathost.net/api/0.1/game-servers/5e38f033e221d82aed6257a7/files/addons/sourcemod/data/sqlite/rankme.sq3"
username = "bknize@gmail.com"
password = ""
persistantDatabase = './database.db'
stagingDatabase = './rankme.sql'

def get_db(database):
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(database)
    db.row_factory = dict_factory
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def query_db(query, table, db = persistantDatabase, args=(), one=False):
    cur = get_db(db)
    exists = cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='" + table + "'").fetchall()
    if len(exists):
        rv = cur.execute(query + table, args).fetchall()
        cur.close()
        return (rv[0] if rv else None) if one else rv
    return {}

def fetch():
    print('Fetching DatHost db')
    r = requests.get(url, auth=HTTPBasicAuth(username, password))
    file = open('rankme.sql', 'wb')
    file.write(r.content)
    file.close()
    copy_table()

def copy_table():
    dest = sqlite3.connect(persistantDatabase)
    cur = dest.cursor()
    newtable = "rankme_" + date.today().strftime("%d_%m_%y")
    cur.execute("ATTACH DATABASE 'rankme.sql' AS staging_db")
    exists = dest.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='" + newtable + "'").fetchall()
    if len(exists):
        dest.cursor().execute("DROP TABLE " + newtable)
    dest.cursor().execute("CREATE TABLE `" + newtable + "` (`id`, `steam`, `name`, `lastip`, `score`, `kills`, `deaths`, `suicides`, `tk`, `shots`, `hits`, `headshots`, `connected`, `rounds_tr`, `rounds_ct`, `lastconnect`, `knife`, `glock`, `hkp2000`, `p250`, `deagle`, `elite`, `fiveseven`, `tec9`, `nova`, `xm1014`, `mag7`, `sawedoff`, `bizon`, `mac10`, `mp9`, `mp7`, `ump45`, `p90`, `galilar`, `ak47`, `scar20`, `famas`, `m4a1`, `aug`, `ssg08`, `sg556`, `awp`, `g3sg1`, `m249`, `negev`, `hegrenade`, `flashbang`, `smokegrenade`, `incgrenade`, `molotov`, `taser`, `decoy`, `head`, `chest`, `stomach`, `left_arm`, `right_arm`, `left_leg`, `right_leg`, `c4_planted`, `c4_exploded`, `c4_defused`, `ct_win`, `tr_win`, `hostages_rescued`, `vip_killed`, `vip_escaped`, `vip_played`)")

    cur.execute("INSERT INTO " + newtable + " SELECT * FROM staging_db.rankme")
    dest.commit()

class GetData(Resource):
    def get(self, day = date.today().strftime("%d"), month = date.today().strftime("%m"), year = date.today().strftime("%y")):
        currentDate = day + '_' + month + '_' + year
        query = query_db("SELECT * FROM ", "rankme_"+currentDate)
        return query

class FetchDB(Resource):
    def get(self):
        fetch()
        return { 'response': 'DatHost data fetched.'}

scheduler = BackgroundScheduler()
scheduler.add_job(fetch, 'cron', hour=12, day_of_week='thu')
scheduler.add_job(fetch, 'cron', hour=12, day_of_week='fri')
scheduler.start()

api.add_resource(GetData, '/', '/<string:day>/<string:month>/<string:year>')
api.add_resource(FetchDB, '/fetch')

atexit.register(lambda: scheduler.shutdown())

if __name__ == '__main__':
    app.run(debug=True)
