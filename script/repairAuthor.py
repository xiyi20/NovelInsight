import csv
import os
from time import sleep

import requests
from lxml import etree
from pymysql import *


def repair():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
    }
    conn = connect(host='node1', user='root', password='123456', database='bigdata', port=3306)
    cursor = conn.cursor()

    cursor.execute('select * from novelData where author like "%�%"')
    results = cursor.fetchall()

    if not os.path.exists('./author.csv'):
        with open('./author.csv', 'a', newline='', encoding='utf-8') as wf:
            write = csv.writer(wf)
            write.writerow(['id', 'author', 'oldAuthor'])

    def save_to_csv(result):
        with open('./author.csv', 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(result)

    for row in results:
        id = row[21]
        oldAuthor = row[3]
        url = row[20]
        try:
            response = requests.get(url, headers)
            response.encoding = 'gb18030'
            e = etree.HTML(response.text)
            author = e.xpath('//div[@class="Two-Left"]/div/div/div/a/@title')[0]
        except Exception:
            print(id, ':' + response.text)
            author = ""
            sleep(10)
        print(oldAuthor + ' -> ' + author)
        save_to_csv((id, author, oldAuthor))

def update():
    conn = connect(host='node1', user='root', password='123456', database='bigdata', port=3306)
    cursor = conn.cursor()
    with open('./author.csv', 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            sql = "UPDATE novelData SET author = %s WHERE id = %s"
            cursor.execute(sql, (row['author'], row['id']))
        conn.commit()


if __name__ == "__main__":
    update()