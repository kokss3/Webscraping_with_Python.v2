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

nameOfFile = 'F10 M5 LIM'
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

    def getTagForId(self, id):
        self.cur.execute("SELECT tag FROM links WHERE id = ?", (id,))
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


def saveFileFromList(listT, fileName):
    text = ''
    for part in listT:
        text += (str(part) + '\n')
    with open((fileName + '.txt'), 'w', encoding='utf-8-sig') as file:
        file.write(text)


# remove temporary html files
def removeFiles(extension, size):
    htmlFiles = [os.path.join(name)
                 for root, dirs, files in os.walk(os.getcwd())
                 for name in files
                 if name.endswith(extension)]
    for name in htmlFiles:
        if (len(name) < size):
            os.remove(name)


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


db = Database(nameOfFile + '.db')
finalList = []

dTags = db.getDistinctTags()

# Repair instruction is 0
for tag in dTags:
    for page in db.getAllByTags(tag[0]):
        if (page[-1] != None):
            finalList.append(page)
sizePDF = 0
tocList = []
currentPage = 1
out = fitz.open()
last_firstTitle = ''
lastTitle = [None, None, None, None]
for page in finalList:
    # get title
    titleRaw = page[2:-2][0].split(' - ')[-3:]
    titleRaw[1] = titleRaw[1][3:]
    # open pdf
    doc = fitz.open(str(page[0]) + '.pdf')
    sizeOfBook = doc.pageCount

    # remove links
    doc = removeLINK(doc)
    firstTitle = db.getTagForId(int(page[0]))[0]
    firstTitle_ = str(firstTitle[0])

    titleList = []
    titleList.append(firstTitle_)
    for ttl in titleRaw:
        titleList.append(ttl)

    # create toc list
    i = 0
    isDifferent = False

    # ovdje ide dio koji puni listu iza teksta
    for singleTitle in titleList:

        if singleTitle != lastTitle[i] and singleTitle != 'Repair Manuals and Technical Data':
            isDifferent = True

        if isDifferent:
            tocList.append([i + 1, singleTitle, currentPage])
            isDifferent = True

        else:
            isDifferent = False

        lastTitle[i] = singleTitle

        i += 1

    while i < 4:
        lastTitle[i] = None
        i += 1

    out.insertPDF(doc)
    sizePDF += sizeOfBook
    currentPage += sizeOfBook

try:
    out.setToC(tocList)
except ValueError as a:
    print(a)

out.save(nameOfFile + '.pdf')
out.close()
removeFiles('.pdf', 9)
removeFiles('.db', 30)
