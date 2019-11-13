import requests as req
from bs4 import BeautifulSoup as soup
import os
import fitz
import time
import pdfkit
import urllib3
import sqlite3
from tqdm import tqdm

from itertools import cycle
import traceback
from lxml.html import fromstring

urllib3.disable_warnings()

pgNr = 1
iterator = 0
counter = 0

urlList = {
    'https://www.newtis.info/tisv2/a/en/F30-318d-lim/repair-manuals/',
    'https://www.newtis.info/tisv2/a/en/F30-325d-lim/repair-manuals/',
    'https://www.newtis.info/tisv2/a/en/F30-330d-lim/repair-manuals/',
    'https://www.newtis.info/tisv2/a/en/F30-318i-lim/repair-manuals/',
    'https://www.newtis.info/tisv2/a/en/F30-320i-lim/repair-manuals/',
    'https://www.newtis.info/tisv2/a/en/F30-330i-lim/repair-manuals/',

    'https://www.newtis.info/tisv2/a/en/g20-318d-lim/repair-manuals/',
    'https://www.newtis.info/tisv2/a/en/g20-320d-lim/repair-manuals/',
    'https://www.newtis.info/tisv2/a/en/g20-330d-lim/repair-manuals/',
    'https://www.newtis.info/tisv2/a/en/g20-320i-lim/repair-manuals/',
    'https://www.newtis.info/tisv2/a/en/g20-325i-lim/repair-manuals/',
    'https://www.newtis.info/tisv2/a/en/g20-330i-lim/repair-manuals/'
}

base_link = 'https://www.newtis.info'
outPutName = ''


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


def get_proxies():
    url = 'https://free-proxy-list.net/'
    response = requests.get(url)
    parser = fromstring(response.text)
    proxies = set()
    for i in parser.xpath('//tbody/tr')[:10]:
        if i.xpath('.//td[7][contains(text(),"yes")]'):
            proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
            proxies.add('"http":"' + proxy + '"')
    return proxies


def saveFileFromList(listT, fileName):
    text = ''
    for part in listT:
        text += (str(part) + '\n')
    with open((fileName + '.txt'), 'w', encoding='utf-8-sig') as file:
        file.write(text)


# remove temporary html files
def removeFiles(extension, size):
    files = [os.path.join(name)
             for root, dirs, files in os.walk(os.getcwd())
             for name in files
             if name.endswith(extension)]
    for name in files:
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


# this to fool bot-block
def foolCaptcha(iterator):
    if (iterator >= 12):
        time.sleep(9)
        iterator = 0
    else:
        iterator += 1
        time.sleep(1)
    return iterator


# count big iterations
def countBig(counter):
    if counter > 250:
        time.sleep(31)
        counter = 0
    else:
        counter += 1
    return counter


# gets all material and generates pdf
def processPage(urlLink, fileName, titl):
    global db
    time.sleep(0.5)
    text = soup(req.get(urlLink).text, 'lxml')
    links = ''

    if (text.find_all(class_='grid')):

        bmw_no = ''
        bmw_title = ''
        if (text.find_all(class_='AWNUMBER')):
            bmw_no = str(text.find(class_='AWNUMBER').string)
        if (text.find_all(class_='docnr')):
            bmw_no = str(text.find(class_='docnr').string)
        if (text.find_all(class_='title')):
            bmw_title = str(text.find(class_='title').string)
        if (text.find_all(class_='TITLE')):
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

        # get html
        # options for PDFkit
        options = {
            'quiet': ''
        }

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


print('Start gathering links!')
for url in urlList:
    print(url)
    outPutName = url[url.index('en/') + 3:url.index('/re')]
    outPutName = outPutName.replace('-', ' ').upper()
    db = Database(outPutName + '.db')

    # primary links
    bla = soup(req.get(url).text, 'lxml')
    links = getAllPossible(bla, 'grp', url)

    combine = []
    # start the process and gather all links that are present for future book
    for link in tqdm(links):
        pageData = soup(req.get(link).text, 'lxml')

        # gathered all first links
        ls1 = getAllPossible(pageData, 'grp', link)
        # opening links one by one
        for linkA in ls1:
            counter = countBig(counter)
            iterator = foolCaptcha(iterator)
            pageData1 = soup(req.get(linkA).text, 'lxml')

            if (pageData1.find_all(class_='grp')):
                ls_1 = getAllPossible(pageData1, 'grp', link)
                for linkB in ls_1:
                    counter = countBig(counter)
                    iterator = foolCaptcha(iterator)

                    pageData2 = soup(req.get(linkB).text, 'lxml')

                    # gets all links inside (finals and proceeding)
                    ls_2 = getAllPossibleWithTag(pageData2, 'docs m20', linkB, 'p')
                    for ll in ls_2:
                        print(ll)
                        combine.append(ll)

            # gets all links inside (finals and proceeding)
            ls2 = getAllPossibleWithTag(pageData1, 'docs m20', linkA, 'p')
            for ll in ls2:
                combine.append(ll)

        counter = countBig(counter)

        lm1 = getAllPossibleWithTag(pageData, 'docs m20', link, 'p')
        for ll in lm1:
            combine.append(ll)

    # dump all in DB
    for tLink in combine:
        db.insert(tLink[0], tLink[-1], tLink[1], None)

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
startPgNr = 1
pageCounter = startPgNr
print('Producing PDF-s')
for rawLink in combine[startPgNr - 1:]:
    asdf = rawLink[-3].split(' - ')
    titl = asdf[-1]
    rawLink = rawLink[1]

    iterator = foolCaptcha(iterator)
    counter = countBig(counter)

    currentPage = []

    # get link in 1st place
    currentPage.append(rawLink)

    # insert title
    currentPage.append(processPage(rawLink, str(pageCounter), titl))

    # insert filename
    currentPage.append(str(pageCounter))

    # create final list
    final_out_merger.append(currentPage)

    pageCounter += 1
    print(pageCounter)
finalList = []

# COMBINING
dTags = db.getDistinctTags()

# Repair instruction is 0
for tag in dTags:
    for page in db.getAllByTags(tag[0]):
        if (page[-1] != None):
            finalList.append(page)

tocList = []
sizePDF = 0
currentPage = 1
out = fitz.open()
last_firstTitle = ''
lastTitle = [None, None, None, None]
for page in tqdm(finalList):
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
    doc.close()
try:
    out.setToC(tocList)
except ValueError as a:
    print(a)

out.save(outPutName + '.pdf')

out.close()
# removeFiles('.pdf', 10)
# print(outPutName + ' - Done!')

# removeFiles('.db', 50)	
print('All Done!')
