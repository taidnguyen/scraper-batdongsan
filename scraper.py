#!/usr/bin/env python
import time
import datetime
import re
import cloudscraper
import pandas as pd

from bs4 import BeautifulSoup

now = datetime.datetime.now()

# def convertPrice(price, area):
#     '''Convert price format'''
#     if price == "Giá thỏa thuận":
#         return None
#     elif price.split(" ")[1]=="nghìn/m²":
#             return float(re.match("(.*?) nghìn/m²", price).group(1))*float(re.match("(.*?) m²", area).group(1))*1000
#     elif price.split(" ")[1]=="triệu/m²":
#             return float(re.match("(.*?) triệu/m²", price).group(1))*float(re.match("(.*?) m²", area).group(1))*1000000
#     elif price.split(" ")[1]=="tỷ/m²":
#             return float(re.match("(.*?) tỷ/m²", price).group(1))*float(re.match("(.*?) m²", area).group(1))*1000000000
#     elif price.split(" ")[1]=="nghìn":
#         return float(re.match("(.*?) nghìn", price).group(1))*1000
#     elif price.split(" ")[1]=="triệu":
#         return float(re.match("(.*?) triệu", price).group(1))*1000000
#     elif price.split(" ")[1]=="tỷ":
#         return float(re.match("(.*?) tỷ", price).group(1))*1000000000
#     else:
#         return price

def getPages(root):
    '''Scrape root to get all pages from finding the max; sublinks are in the form of ie. /p2, /p3, /p100'''
    last = None
    while last is None: #Retry until last page is found
        try:
            scraper = cloudscraper.create_scraper() #cloudscraper to bypass Cloudfare
            html = scraper.get(root).text
            soup = BeautifulSoup(html, "html.parser")
            last = max([int(link['pid']) for link in soup.findAll("a", href=re.compile(r"/p[0-9]"))])
        except:
            print("Reading root page fails. Retry in 2s...")
            time.sleep(1.5)
            pass
    if int(last) > 2500:
        last = 2500 #grab first 2500 pages only - Lambda function limits
    print("Scanning {0} pages".format(last-2+1))

    #get pages
    pages = []
    for i in range(2, int(last) + 1):  # starts at 2
        page = "/p{0}".format(str(i))
        if page not in pages:
            pages.append(page)

    pages.append('/') #root also scrapable

    return pages

def writeData(subUrls):
    '''Iterate through links and write rows'''
    rows = []
    fails = []

    for subUrl in subUrls:
        print("Writing data: {0}".format(subUrl))
        #grab html soup
        scraper = cloudscraper.create_scraper()
        time.sleep(1)
        html = scraper.get(root+subUrl).text
        soup = BeautifulSoup(html, "html.parser")

        # listings per page
        if not soup.findAll("div", {"class":re.compile(".* product-item clearfix .*")}):
            print("Scrape unsuccessful: {0}".format(subUrl))
            fails.append(subUrl)
        else:
            for listing in soup.findAll("div", {"class":re.compile(".* product-item clearfix .*")}):
                uid       = listing['uid']
                prid      = listing['prid']
                url       = root+subUrl
                title     = listing.find("a", {"class":"wrap-plink"})["title"] if listing.find("a", {"class":"wrap-plink"}) else None
                price     = listing.find("span", {"class":"price"}).text if listing.find("span", {"class":"price"}) else None
                area      = listing.find("span", {"class":"area"}).text if listing.find("span", {"class":"area"}) else None
                bedroom   = listing.find("span", {"class":"bedroom"}).text if listing.find("span", {"class":"bedroom"}) else None
                toilet    = listing.find("span", {"class":"toilet"}).text if listing.find("span", {"class":"toilet"}) else None
                location  = listing.find("span", {"class":"location"}).text if listing.find("span", {"class":"location"}) else None
                content   = listing.find("div", {"class":"product-content"}).text if listing.find("div", {"class":"product-content"}) else None
                post_date = datetime.datetime.strptime(listing.find("span", {"class":"tooltip-time"}).text, "%d/%m/%Y").strftime("%Y-%m-%d") if listing.find("span", {"class":"tooltip-time"}) else None
                # price_vnd = convertPrice(listing.find("span", {"class":"price"}).text, listing.find("span", {"class":"area"}).text) if listing.find("span", {"class":"price"}) and listing.find("span", {"class":"area"}) else None
                # area_m2   = float(re.match("(.*?) m²", listing.find("span", {"class":"area"}).text).group(1)) if listing.find("span", {"class":"area"}) else None
                # city      = listing.find("span", {"class":"location"}).text.split(", ")[1] if listing.find("span", {"class":"location"}) else None
                # district  = listing.find("span", {"class":"location"}).text.split(", ")[0] if listing.find("span", {"class":"location"}) else None
                scrape_timestamp = now

                row = {}
                row.update({
                    "uid": uid,
                    "prid": prid,
                    "url": url,
                    "title_raw": title,
                    "price_raw": price,
                    "area_raw": area,
                    "bedroom_raw": bedroom,
                    "toilet_raw": toilet,
                    "location_raw": location,
                    "content_raw": content,
                    "post_date": post_date,
                    # "price_vnd": price_vnd,
                    # "area_m2": area_m2,
                    # "city": city,
                    # "district": district,
                    "scrape_timestamp": scrape_timestamp
                })
                rows.append(row)

    return rows, fails

if __name__ == "__main__":
    root = "https://batdongsan.com.vn/nha-dat-ban-tp-hcm"
    pages = getPages(root=root)
    rows, fails = writeData(pages)
    print("Number of rows: {0}".format(len(rows)))
    print("Number of pages failed: {0}".format(len(fails)))
    print("Page success rate: {0}%".format(len(fails)/len(pages)))
    df = pd.DataFrame(rows)

    today = now.date()
    sub_root = root.partition("https://batdongsan.com.vn/")[2]
    df.to_csv("{0}-{1}.csv".format(today, sub_root))
    print("Scrape job done!")
