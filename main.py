from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool,Manager
import time
from pymongo import MongoClient
import random
import queue

client = MongoClient('localhost',27017)
db = client.dy
collection = db.dangdang
headers = \
    {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:64.0) Gecko/20100101 Firefox/64.0',
        # 'Host': 'category.dangdang.com'
    }

def getUrlLinks():
   Url = "http://category.dangdang.com/"

   r = requests.get(Url,headers=headers)
   soup = BeautifulSoup(r.text,'lxml')

   bookCategory = soup.find('div',id='floor_1',class_='classify_books').find_all('li')
   myQueue = queue.Queue()
   for each in bookCategory:
       if each.a.text == '更多':
           continue
       myQueue.put([each.a.text,each.a['href']])

   links = []
   while not myQueue.empty():
       #time.sleep(random.random())
       qItem = myQueue.get()
       strr = qItem[1]
       try:
           r = requests.get(strr,headers=headers)
       except:
           myQueue.put(qItem)
           continue
       soup = BeautifulSoup(r.text,'lxml')
       page_cnt = 1
       try:
           page_cnt = int(soup.find('div',class_='paging').find_all('a')[-2].text)
       except:
           pass
       print('category: ',qItem[0] , 'page: ',page_cnt)
       for i in range(1, page_cnt + 1):
           cnt = strr.find('cp')
           links.append(strr[:cnt]+'pg'+str(i)+'-'+strr[cnt:])
   print("getlinks Done")
   return links

def crawler(workQueue,index):
    processId = "process-" + str(index)
    while not workQueue.empty():
        url = workQueue.get(timeout=2)
        try:
            r = requests.get(url,headers=headers)
            soup = BeautifulSoup(r.text,'lxml')
            contents = soup.find('div',class_='con shoplist').find_all('li')
            for each in contents:
                data = {'bookName':each.a['title'],'url':each.a['href']}
                collection.insert_one(data)
            print('Page insert sucess')
        except Exception as e:
            workQueue.put(url)
        #time.sleep(random.random())

if __name__ == "__main__":
    urlLinks = getUrlLinks()
    mananger = Manager()
    workQueue = mananger.Queue()
    for url in urlLinks:
        workQueue.put(url)
    print("Queue size: ",workQueue.qsize())
    start = time.time()

    pool = Pool(processes=4)
    for i in range(4):
        pool.apply(crawler,args=(workQueue,i))
    print("Processes started")
    pool.close()
    pool.join()
    end = time.time()
    print("Time: ",end-start)