import random
import pandas as pd
import requests
from lxml import etree
import csv
import os
import time

class spider(object):
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
        }

    def init(self):
        if not os.path.exists('./novelData.csv'):
            with open('./novelData.csv', 'a', newline='', encoding='utf-8') as wf:
                write = csv.writer(wf)
                write.writerow(['type', 'title', 'cover', 'author', 'authorImg', 'authorWork', 'authorWords', 'authorDays',
                                'monthRead', 'monthFlower', 'allRead', 'allFlower', 'wordNum', 'updateTicket','reward', 'monthTicker',
                                'shareNum', 'rate', 'startTime', 'updateTime', 'detailLink'])

    def save_to_csv(self, resultData):
        with open('./novelData.csv', 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(resultData)

    def convert_value(self, x):
        if isinstance(x, str) and "万" in x:
            num_str = x.replace("万", "")
            return float(num_str) * 10000
        else:
            return float(x)

    def main(self, detailUrl):
        detailUrl = 'https:' + detailUrl
        response = requests.get(detailUrl, headers=self.headers)
        response.encoding = 'gb18030'
        e = etree.HTML(response.text)
        type = e.xpath('//div[@class="T-R-Top"]/div[@class="T-R-T-Box2"]/div[@class="T-R-T-B2-Box1"][1]/span/span/a/text()')[0]
        title = e.xpath('//h1[@id="novelName"]/text()')[0]
        cover = e.xpath('//div[@class="T-L-T-Img"]/a/img/@src')[0]
        author = e.xpath('//div[@class="authorInfo clearfix"]/div/div[1]/a/text()')[0]
        authorImg = e.xpath('//div[@class="authorInfo clearfix"]/a/img/@src')[0]
        authorWork = e.xpath('//ul[@class="authorInfo2 clearfix"]/li[1]/div[2]/text()')[0]
        authorWords = int(e.xpath('//ul[@class="authorInfo2 clearfix"]/li[2]/div[2]/text()')[0]) * 10000
        authorDays = e.xpath('//ul[@class="authorInfo2 clearfix"]/li[3]/div[2]/text()')[0]
        monthRead = e.xpath('//div[@class="T-L-O-Z-Box2 fs14"]/span[1]/span/text()')[0]
        monthFlower = e.xpath('//div[@class="T-L-O-Z-Box2 fs14"]/span[2]/span/text()')[0]
        allRead = e.xpath('//div[@class="T-L-O-Z-Box2 fs14"]/span[3]/span/text()')[0]
        allFlower = e.xpath('//div[@class="T-L-O-Z-Box2 fs14"]/span[4]/span/text()')[0]
        wordNumList = e.xpath('//div[@class="T-R-Middle"]/div[@class="T-R-Md-Bobx1"][2]/span[@class="SZspan"]/text()')
        # print(wordNumList)
        wordNum = int("".join(wordNumList))
        updateTicket = e.xpath('//div[@class="C-Three mgTop20 bodyBorderShadow"]/div[@class="C-Thr-Box1"][2]/div[@class="C-Thr-B1-Box3 colorQianlan"]/text()')[0]
        reward = e.xpath('//div[@class="C-Three mgTop20 bodyBorderShadow"]/div[@class="C-Thr-Box1"][3]/div[@class="C-Thr-B1-Box3 colorQianlan"]/text()')[0]
        reward = self.convert_value(reward)
        monthTicket = e.xpath('//div[@class="C-Three mgTop20 bodyBorderShadow"]/div[@class="C-Thr-Box1"][4]/div[@class="C-Thr-B1-Box3 colorQianlan"]/text()')[0]
        shareNum = e.xpath('//div[@class="C-Three mgTop20 bodyBorderShadow"]/div[@class="C-Thr-Box1"][5]/div[@class="C-Thr-B1-Box3 colorQianlan"]/text()')[0]
        try:
            rate = e.xpath('//div[@class="C-Thr-Box3"]/div[@class="C-Thr-B3-Box1"]/span[@class="fs47 colorChen fontoblique"]/text()')[0]
        except:
            rate = 0
        startTime = e.xpath('//div[@class="T-R-Top"]/div[@class="T-R-T-Box2"]/div[@class="T-R-T-B2-Box1"][3]/span/span/text()')[0]
        updateTime = e.xpath('//div[@class="T-L-O-Zuo"]/div[1]/span[@class="fs14 colorQianHui"]/span/text()')[0][:10]
        print(type, title, cover, author, authorImg, authorWork, authorWords, authorDays, monthRead, monthFlower,
              allRead, allFlower, wordNum, updateTicket, reward, monthTicket, shareNum, rate, startTime, updateTime, detailUrl)
        self.save_to_csv([type, title, cover, author, authorImg, authorWork, authorWords, authorDays, monthRead, monthFlower,
              allRead, allFlower, wordNum, updateTicket, reward, monthTicket, shareNum, rate, startTime, updateTime, detailUrl])

if __name__ == '__main__':
    # spiderObj2 = spider()
    # spiderObj2.init()
    df = pd.read_csv('tempDetail.csv')
    column_0 = df.iloc[:, 0].tolist()
    print(column_0)
    for detail in column_0:
        print(detail)
        spiderObj2 = spider()
        spiderObj2.main(detail)
        time.sleep(random.randint(1, 5))
        # break