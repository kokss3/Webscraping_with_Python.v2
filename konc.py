import requests as req
from bs4 import BeautifulSoup as soup
import os
import fitz
import time
import pdfkit
import urllib3
import sqlite3
from tqdm import tqdm

urllib3.disable_warnings()

pgNr = 1
iterator = 0
counter = 0
base_link = 'https://www.newtis.info'
url = 'https://www.newtis.info/tisv2/a/en/f80-m3-lim/repair-manuals/'
outPutName = url[url.index('en/') + 3:url.index('/re')]
outPutName = outPutName.replace('-', ' ').upper()


class Database:
    def __init__(self, db):
        self.conn = sqlite3.connect(db)
        self.cur = self.conn.cursor()
        self.cur.execute(
            "CREATE TABLE IF NOT EXISTS links (id INTEGER PRIMARY KEY AUTOINCREMENT, link TEXT, title TEXT, tag TEXT, size INTEGER)")
        self.conn.commit()

    def fetchLinks(self):
        self.cur.execute("SELECT link FROM links")
        rows = self.cur.fetchall()
        return rows

    def fetch(self):
        self.cur.execute("SELECT * FROM links")
        rows = self.cur.fetchall()
        return rows

    def getDistinctTags(self):
        self.cur.execute("SELECT DISTINCT tag FROM links")
        rows = self.cur.fetchall()
        return rows

    def getIdByTags(self, what, tag):
        self.cur.execute("SELECT id FROM links WHERE tag = ? ORDER BY id", (tag,))
        rows = self.cur.fetchall()
        return rows

    def getTitleByTags(self, what, tag):
        self.cur.execute("SELECT title FROM links WHERE tag = ? ORDER BY id", (tag,))
        rows = self.cur.fetchall()
        return rows

    def getAllByTags(self, tag):
        self.cur.execute("SELECT * FROM links WHERE tag = ? ORDER BY id", (tag,))
        rows = self.cur.fetchall()
        return rows

    def insert(self, link, title, tag, size):
        self.cur.execute("INSERT INTO links VALUES(NULL, ?,?,?,?)", (link, title, tag, size))
        self.conn.commit()

    def update(self, id, link, title, tag, size):
        self.cur.execute("UPDATE links SET link = ?, title = ?, tag = ?, size = ? WHERE id = ?",
                         (link, title, tag, size, id))
        self.conn.commit()

    def insertLink(self, id, link):
        self.cur.execute("UPDATE links SET link = ? WHERE id = ?", (link, id))
        self.conn.commit()

    def insertTag(self, link, tag):
        self.cur.execute("UPDATE links SET tag = ? WHERE id = ?", (tag, id))
        self.conn.commit()

    def insertTitle(self, link, title):
        self.cur.execute("UPDATE links SET title = ? WHERE link = ?", (title, link))
        self.conn.commit()

    def insertSize(self, link, size):
        self.cur.execute("UPDATE links SET size = ? WHERE link = ?", (size, link))
        self.conn.commit()

    def __del__(self):
        self.conn.close()


def removeLINK(rawDoc):
    g = 0
    # iterate through pages
    while g < rawDoc.pageCount:
        # load individual page
        grr = rawDoc.loadPage(g)
        # form a list of dicts of links
        sss = grr.getLinks()
        # tierate through list
        for l in sss:
            # delete specific link dict
            grr.deleteLink(l)
        g += 1
    return rawDoc


db = Database(outPutName + '.db')
tagsList = []
forCombining = db.fetch()
for tags in db.getDistinctTags():
    tagsList.append(tags[0])
# Fault Elimination
# Repair instruction is 0
idsByTag = db.getIdByTags('id', tagsList[0])
titlesByTag = db.getTitleByTags('title', tagsList[0])

firstTOC = titlesByTag[][0].split(' - ')[-3]

print(titlesByTag[60][0].split(' - ')[-3])
