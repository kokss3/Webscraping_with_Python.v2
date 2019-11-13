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
startPgNr = 1974

fileOfDB = 'F10 M5 LIM'
base_link = 'https://www.newtis.info'
url = 'https://www.newtis.info/tisv2/a/en/f30-335i-lim/repair-manuals/'
outPutName = url[url.index('en/') + 3:url.index('/re')]
outPutName = outPutName.replace('-', ' ').upper()


class Database:
    def __init__(self, db):
        self.conn = sqlite3.connect(db)
        self.cur = self.conn.cursor()
        self.cur.execute(
            "CREATE TABLE IF NOT EXISTS links (id INTEGER PRIMARY KEY AUTOINCREMENT, link TEXT, title TEXT, tag TEXT, size INTEGER)")
        self.conn.commit()

    def fetch(self):
        self.cur.execute("SELECT * FROM links")
        rows = self.cur.fetchall()
        return rows

    def insert(self, link, title, tag, size):
        self.cur.execute("INSERT INTO links VALUES(NULL, ?,?,?,?)", (link, title, tag, size))
        self.conn.commit()

    def remove(self, id):
        self.cur.execute("DELETE FORM links WHERE ID = ?", (id,))
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


db = Database(fileOfDB + '.db')


# remove temporary html files
def removeFiles(extension):
    htmlFiles = [os.path.join(name)
                 for root, dirs, files in os.walk(os.getcwd())
                 for name in files
                 if name.endswith(extension)]
    for name in htmlFiles:
        os.remove(name)


# this to fool bot-block
def foolCaptcha(iterator):
    if (iterator >= 12):
        time.sleep(9)
        iterator = 0
    else:
        iterator += 1
        time.sleep(1)
    return iterator


# gets all material and generates pdf
def processPage(urlLink, fileName, titl):
    global db

    # get html
    # options for PDFkit
    options = {
        'quiet': ''
    }

    text = soup(req.get(urlLink).text, 'lxml')
    links = ''

    if text.find_all(class_='grid'):

        # get title
        bmw_title = ''
        if text.find_all(class_='title'):
            bmw_title = str(text.find(class_='title').string)
        if text.find_all(class_='TITLE'):
            bmw_title = str(text.find(class_='TITLE').string)

        # get full navlink for title
        navLinks = text.find('nav').find_all('a')
        for link in navLinks:
            links += (link.get_text() + ' - ')
        links = links.replace('Home - ', '')[:-3]
        if (titl != None):
            bmw_title = titl
        links = links + ' - ' + bmw_title

        db.insertTitle(urlLink, links)
        # prepare all css links
        css = text.find_all('link', {'rel': 'stylesheet'})
        cssLinks = [each.get('href') for each in css]
        for individualLink in cssLinks:
            text = replaceLink(str(text), individualLink, '')

        text = soup(text, 'lxml')

        # prepare all imgs links
        img = text.find_all('img')
        imgLinks = [each.get('src') for each in img]
        for individualLink in imgLinks:
            text = replaceLink(str(text), individualLink, '.jpg')

        text = soup(str(text), 'lxml')

        # adding header with css links (ters) on main body (bodyR)
        ters = str(text.find('head'))
        bodyR = ters + str(text.find(class_='content'))

        # generate html file
        with open(fileName + '.html', 'w', encoding='utf-8-sig') as file:
            file.write(bodyR)

        # convert html to pdf
        pdfkit.from_file(fileName + '.html', fileName + '.pdf', options=options)
        doc = fitz.open(fileName + '.pdf')
        bookSize = doc.pageCount
        doc.close()
        db.insertSize(size=bookSize, link=urlLink)
        db.insertTitle(title=links, link=urlLink)
        os.remove(fileName + '.html')

    # return title
    return links


# function for fixing links in html
def replaceLink(rawText, link, extension):
    out_link = base_link + link
    # check if link is alreay present
    if (rawText.find(out_link) == -1):
        rawText = rawText.replace(link, (out_link + extension))
    return rawText


# gives list of links with tags and engine types
# [link, group, engineType],[link, group, engineType],...
def getAllPossibleWithTag(pageData, className, urLink, tag):
    temp = []
    tags = ''
    asd = []
    if (pageData.find_all(class_=className)):
        baseLinks = pageData.find(class_=className).find_all(tag)
        for link in baseLinks:
            pret = []
            pret.append(urLink + str(link.a.get('href')))
            pret.append(link.a.text[:link.a.text.index('(') - 1])
            titl = link.a.text[link.a.text.index(') ') + 2:]
            if (link.span != None):
                links = link.span.find_all('a')
                for ll in links:
                    tags += ll.text + ', '
                pret.append(tags[:-2])
            pret.append(titl)
            asd.append(pret)

        # ovo je ispravno
        temp = asd
    return temp


# gives pure list with just links
def getAllPossible(pageData, className, urLink):
    temp = []
    if (pageData.find_all(class_=className)):
        baseLinks = pageData.find(class_=className).find_all('a')
        for link in baseLinks:
            if (link.get('href') is not None):
                temp.append(urLink + str(link.get('href')))
    return temp


def saveFileFromList(listT, fileName):
    text = ''
    for part in listT:
        text += (str(part) + '\n')
    with open((fileName + '.txt'), 'w', encoding='utf-8-sig') as file:
        file.write(text)


tempTup = db.fetch()
combine = []

for l in tempTup:
    t = []
    for g in l:
        t.append(g)
    combine.append(t)

# finalList[{link, title, pgNr}, {link, title, pgNr}, ...]
final_out_merger = []
# for next page
pageCounter = startPgNr
print('Producing PDF-s')
for rawLink in combine:
    asdf = rawLink[-3].split(' - ')
    titl = asdf[-1]
    print(titl)
    rawLink = rawLink[1]

    if counter > 200:
        time.sleep(31)
        counter = 0

    currentPage = []

    iterator = foolCaptcha(iterator)

    # get link in 1st place
    currentPage.append(rawLink)

    # insert title
    currentPage.append(processPage(rawLink, str(pageCounter), titl))

    # insert filename
    currentPage.append(str(pageCounter))

    # create final list
    final_out_merger.append(currentPage)

    counter += 1
    pageCounter += 1

ggg = ''
for ppp in tqdm(final_out_merger):
    ggg += (str(ppp) + '\n')

with open('finalMerger.txt', 'w', encoding='utf-8-sig') as file:
    file.write(ggg)

import combiner

print('Done!')
