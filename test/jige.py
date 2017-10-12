#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import pymysql
import logging
import requests
from mysql import mysqlUri
from bs4 import BeautifulSoup
from ProxyDemo import proxyListGet
import pymongo
from datetime import datetime

headers = {

'Host': 'forms.hubspot.com',
'Connection': 'keep-alive',
'Content-Length': '2185',
'Cache-Control': 'max-age=0',
'Origin': 'http://resources.newzoo.com',
'Upgrade-Insecure-Requests': '1',
'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
'Content-Type': 'multipart/form-data; boundary=----WebKitFormBoundary5xiGkFqzWHj3si3e',
'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
'Referer': 'http://resources.newzoo.com/2017-newzoo-global-esports-market-report-light',
'Accept-Encoding': 'gzip, deflate, br',
'Accept-Language': 'zh-CN,zh;q=0.8'

}

mm = 'application/json'
mmm = 'multipart/form-data; boundary=----WebKitFormBoundary5xiGkFqzWHj3si3e'

r = requests.session()
# r.headers.update({"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36"})

post_data = {
        "firstname": 'aaa',
        "lastname": 'bbb',
        "email": '2023947099@qq.com',
        "role": 'CEO/Founder',
        "hs_context": None
    }

str = u'''
------WebKitFormBoundary5xiGkFqzWHj3si3e
Content-Disposition: form-data; name="firstname"

aaa
------WebKitFormBoundary5xiGkFqzWHj3si3e
Content-Disposition: form-data; name="lastname"

bbb
------WebKitFormBoundary5xiGkFqzWHj3si3e
Content-Disposition: form-data; name="email"

2023947099@qq.com
------WebKitFormBoundary5xiGkFqzWHj3si3e
Content-Disposition: form-data; name="role"

CEO/Founder
------WebKitFormBoundary5xiGkFqzWHj3si3e
Content-Disposition: form-data; name="hs_context"

{"rumScriptExecuteTime":332.685,"rumServiceResponseTime":861.1200000000001,"rumFormRenderTime":494.175,"rumTotalRequestTime":490.0050000000001,"pageUrl":"http://resources.newzoo.com/2017-newzoo-global-esports-market-report-light","pageTitle":"2017 Newzoo Global Esports Market Report Light","source":"FormsNext-static-1.523","isHostedOnHubspot":true,"timestamp":1502692006360,"userAgent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36","referrer":"","hutk":"3fdbdb26c9fc848d17d616654678cfe6","originalEmbedContext":{"portalId":"700740","formId":"a607e9a6-4db0-4d67-b8b0-fa301915187d","formInstanceId":"1330","pageId":4838662505,"pageName":"2017 Newzoo Global Esports Market Report Light","redirectUrl":"http://resources.newzoo.com/thank-you-esports-report-light-2017","css":"","target":"#hs_form_target_module_13885066546126190","contentType":"landing-page","formData":{"cssClass":"hs-form stacked hs-custom-form"}},"recentFieldsCookie":{},"pageId":"4838662505","pageName":"2017 Newzoo Global Esports Market Report Light","boolCheckBoxFields":"","dateFields":"","redirectUrl":"http://resources.newzoo.com/thank-you-esports-report-light-2017","formInstanceId":"1330","smartFields":{"blog_default_hubspot_blog_subscription":"false"},"urlParams":{},"formValidity":{"firstname":{"valid":true,"errors":[]},"lastname":{"valid":true,"errors":[]},"email":{"valid":true,"errors":[]},"role":{"valid":true,"errors":[]}},"domFields":["firstname","lastname","email"],"correlationId":"ead47989-6321-42cb-8333-c8f23886e51a","disableCookieSubmission":false,"usingInvisibleRecaptcha":false}
------WebKitFormBoundary5xiGkFqzWHj3si3e--
'''


files = {
    'firstname': (None, 'aaa'),
    'lastname': (None, 'bbb'),
    'email': (None, '2023947099@qq.com'),
    'role': (None, 'CEO/Founder')
 }





html0 = r.get('https://newzoo.com/insights/articles/')
html1 = r.get('https://newzoo.com/insights/articles/played-core-pc-games-july-diablo-re-enters-new-dlc-rocket-league-turns-two/')
html2 = r.get('http://cta-redirect.hubspot.com/cta/redirect/700740/262233ab-c743-45ac-abe9-d0ebbb713d71')
mm = r.head('https://forms.hubspot.com/uploads/form/v2/700740/8e8435a5-b349-4cc6-ad0e-0cb902186532').headers

html4 = requests.get('http://resources.newzoo.com/global-games-market-report-light-2017')


m = 3




# r.headers.update({"Referer": "http://resources.newzoo.com/global-games-market-report-light-2017"})
html3 = r.post('https://forms.hubspot.com/uploads/form/v2/700740/8e8435a5-b349-4cc6-ad0e-0cb902186532', data=str, headers=headers)
m = 3