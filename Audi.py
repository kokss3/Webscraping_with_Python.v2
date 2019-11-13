import requests as req
from bs4 import BeautifulSoup  # as soup
import os
import pdfkit
import urllib3
from bs4.dammit import EncodingDetector

currentFolder = os.getcwd()
urllib3.disable_warnings()

url = input("Insert URL: \n")
# page_number = input("Insert starting page: \n")
page_number = 1
counter = int(page_number)
has_next = True

while has_next:
    resp = req.get(url)
    encoding = EncodingDetector.find_declared_encoding(resp.content, is_html=True)
    soup = BeautifulSoup(resp.content, 'lxml', from_encoding=encoding)

    bodyR = soup.find_all('td', {'style': 'vertical-align:top;background-color:#FFFFFF;'})
    bodyPreR = bodyR[3]

    textR = str(bodyPreR)

    # treba dodati css linker
    css = bodyPreR.find_all('link', {'rel': 'stylesheet'})
    imgs = bodyPreR.find_all('img')
    csslinks = [each.get('href') for each in css]
    links = [each.get('src') for each in imgs]

    base_link = 'https://workshop-manuals.com'

    header_H3 = soup.find_all('h3')
    part_of_out = header_H3[len(header_H3) - 1].text

    page_name1 = part_of_out.replace(" - ", " ")
    page_name1 = page_name1.replace(">", "-")

    titleRaw = page_name1
    if len(titleRaw) < 100:
        simpleName = titleRaw
    else:
        simpleName = titleRaw[:101]

    simpleName = simpleName.replace('\"', '')
    simpleName = simpleName.replace('/', '')
    simpleName = simpleName.replace('<', '')
    simpleName = simpleName.replace('>', '')
    simpleName = simpleName.replace('►', '')
    simpleName = simpleName.replace('▸', '')
    simpleName = simpleName.replace('?', '')
    simpleName = simpleName.replace(':', '')
    simpleName += ' - Page ' + str(counter)

    print(simpleName)

    fileName_pdf = (simpleName + (u".pdf"))
    fileName_html = (simpleName + (u".html"))

    # extract CSS and replace it for offline use
    cssLinksAll = []
    for individualLinks in csslinks:
        cssLinksAll = individualLinks.split('/')
        out_link = base_link + str(individualLinks)
        fileName_css = cssLinksAll[len(cssLinksAll) - 1]

        textR = textR.replace(individualLinks, fileName_css)

        with urllib3.PoolManager() as http:
            r = http.request('GET', out_link)
        with open(fileName_css, 'wb') as fout:
            fout.write(r.data)

    # extract img and replace it for offline use
    inLink = []
    for individualLinks in links:
        nameImg = individualLinks.split('/')
        out_link = base_link + str(individualLinks)
        fileName_png = nameImg[len(nameImg) - 1]

        textR = textR.replace(individualLinks, nameImg[len(nameImg) - 1])
        # textR=textR.replace()

        with urllib3.PoolManager() as http:
            r = http.request('GET', out_link)
        with open(fileName_png, 'wb') as fout:
            fout.write(r.data)

    # , encoding = 'utf-8-sig'
    with open(fileName_html, 'w', encoding='utf-8-sig') as file:
        file.write(textR)

    # options for PDFkit
    options = {
        'quiet': ''
    }

    # convert html to pdf
    pdfkit.from_file(fileName_html, fileName_pdf, options=options)

    # to find next button
    novo = soup.find('div', {'style': 'text-align:center;'})
    body_pre = novo.find('p', {'style': 'text-align:right;'})

    # get link assigned as Next
    body_pre_ps = body_pre.find('a', href=True, text='[NEXT PAGE]')
    # print(len(body_pre_ps.get('href')))

    # loop while there is next link
    if len(body_pre_ps.get('href')) > len('https://workshop-manuals.com/'):
        url = body_pre_ps.get('href')
        has_next = True
    else:
        has_next = False

    counter += 1

    pngFiles = [os.path.join(name)
                for root, dirs, files in os.walk(currentFolder)
                for name in files
                if name.endswith((".png"))]
    for name in pngFiles:
        os.remove(name)

    gifFiles = [os.path.join(name)
                for root, dirs, files in os.walk(currentFolder)
                for name in files
                if name.endswith((".gif"))]
    for name in gifFiles:
        os.remove(name)

    htmlFiles = [os.path.join(name)
                 for root, dirs, files in os.walk(currentFolder)
                 for name in files
                 if name.endswith((".html"))]
    for name in htmlFiles:
        os.remove(name)

    jpgFiles = [os.path.join(name)
                for root, dirs, files in os.walk(currentFolder)
                for name in files
                if name.endswith((".jpg"))]
    for name in jpgFiles:
        os.remove(name)

# remove HTML and Image
print('done!')
