import random
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
        if not os.path.exists('./tempDetail.csv'):
            with open('./tempDetail.csv', 'a', newline='', encoding='utf-8') as wf:
                write = csv.writer(wf)
                write.writerow(['detailLink'])

    def save_to_csv(self,resultData):
        with open('./tempDetail.csv', 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(resultData)

    def main(self, index):
        # 1 44 4 3 2 7
        spiderUrl = f'https://b.faloo.com/y_1_{index}.html'
        print(spiderUrl)
        pageText = requests.get(spiderUrl, headers=self.headers).text
        # print(pageText)
        e = etree.HTML(pageText)
        firstList = e.xpath(
            '//div[@class="centerTwo bodyBorderShadow"]/div[@class="TwoBox02"]/div[@class="TwoBox02_01"]')
        print(len(firstList))
        for alist in firstList:
            secondList = alist.xpath('./div')
            print(secondList)
            if len(secondList) >= 2:  # 确保至少有2个元素
                detailLink = secondList[0].xpath('./div[@class="TwoBox02_03"]/a/@href')
                self.save_to_csv(detailLink)
                detailLink2 = secondList[1].xpath('./div[@class="TwoBox02_03"]/a/@href')
                self.save_to_csv(detailLink2)
                print(detailLink, detailLink2)
            else:
                print(f"警告：secondList 长度不足，当前长度: {len(secondList)}")
            # break


if __name__ == '__main__':
    # spiderObj = spider()
    # spiderObj.init()
    for i in range(1, 70):
        print(i)
        spiderObj = spider()
        spiderObj.main(i)
        # break
