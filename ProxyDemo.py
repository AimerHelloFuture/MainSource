# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import time


class proxyListGet():
    def __init__(self):
        pass

    def __GetCacheURL(self):
        page = requests.get("http://www.baidu.com/s?ie=utf-8&wd=%E8%A5%BF%E5%88%BA%E4%BB%A3%E7%90%86")
        soup = BeautifulSoup(page.text, "html5lib")
        body = soup.select_one("body")
        if body is None:
            print u"爬取快照失败"
            return
        div = body.select_one('div[id="wrapper_wrapper"]')
        if div is None:
            print u"爬取快照失败"
            return
        div = div.select_one('div[id="container"]')
        if div is None:
            print u"爬取快照失败"
            return
        div = div.select_one('div[id="content_left"]')
        if div is None:
            print u"爬取快照失败"
            return
        divresults = div.select('div[class="result c-container "]')
        if divresults is None:
            print u"爬取快照失败"
            return
        for div in divresults:
            divf13 = div.select_one('div[class="f13"]')
            if divf13.select_one('a[class="c-showurl"]').text == u"www.xicidaili.com/ ":
                cacheurl = divf13.select_one('a[class="m"]')["href"]
                break
        return cacheurl

    def __GetFastCacheURL(self):
        cacheurl = self.__GetCacheURL()
        page = requests.get(cacheurl)
        page.encoding = "gb2312"
        soup = BeautifulSoup(page.text, "html5lib")
        body = soup.select_one("body")
        if body is None:
            print u"爬取快速版失败"
            return
        div = body.select_one('div[id="bd_snap"]')
        if div is None:
            print u"爬取快速版失败"
            return
        div = div.select_one('div[id="bd_snap_txt"]')
        if div is None:
            print u"爬取快速版失败"
            return
        A = div.select('a')
        if A is None:
            print u"爬取快速版失败"
            return
        for a in A:
            if a.text == u"快速版":
                fastcacheurl = a["href"]
                break
        return fastcacheurl

    '''
    timeout为测试代理是否可用的超时时间，建议0.2
    times是测试代理次数，多次测试通过则返回List中有多次
    debug用来控制输出信息
    testurl为测试代理用的网站，建议换成需要爬取的网站的域名
    '''
    def GetProxyList(self, testurl="http://ip.chinaz.com/", timeout=0.2, times=1, debug=0):
        ProxyList = []
        cacheurl = self.__GetFastCacheURL()
        while times != 0:
            page = requests.get(cacheurl)
            soup = BeautifulSoup(page.text, 'html5lib')
            div = soup.select_one('div[class="main"]')
            if div is None:
                print u"西刺代理爬取失败"
                return ProxyList
            trList = div.select('tr')
            if trList is None:
                print u"西刺代理爬取失败"
                return ProxyList
            for tr in trList:
                tdList = tr.select('td')
                if tdList is None:
                    print u"西刺代理爬取失败"
                    return ProxyList
                if len(tdList) == 0:
                    continue
                if tdList[5].string.lower() == 'http' or tdList[5].string.lower() == 'https':
                    proxy = {tdList[5].string.lower(): tdList[1].string + ':' + tdList[2].string, }
                    try:
                        requests.get(testurl, proxies=proxy, timeout=timeout)
                    except Exception:
                        continue
                    else:
                        ProxyList.append(proxy)
                        if debug:
                            for _proxy in proxy:
                                print "%s://%s" % (_proxy, proxy[_proxy])
            times -= 1
        if debug:
            print u'西刺代理爬取结束'

        return ProxyList


if __name__ == '__main__':
    proxy = proxyListGet()
    proxyLists = proxy.GetProxyList(testurl='http://d.qianzhan.com/xdata/list/xfyyy0yyIxPyywyy2xDxfd.html')
    for list in proxyLists:
        print list
