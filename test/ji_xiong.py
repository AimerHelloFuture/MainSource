#!/usr/bin/env python
# -*- coding: utf-8 -*-

import urllib2

import json


def post_data():
    parameters = {
        "firstname": 'aaa',
        "lastname": 'bbb',
        "email": '2023947099@qq.com',
        "role": 'CEO/Founder'}

    jdata = json.dumps(parameters)

    post_multipart('https://forms.hubspot.com/uploads/form/v2/700740/8e8435a5-b349-4cc6-ad0e-0cb902186532', jdata)


def post_multipart(url, fields):
    content_type, body = encode_multipart_formdata("data", fields)

    req = urllib2.Request(url, body)

    req.add_header("User-Agent",
                   "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36")

    req.add_header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8")

    req.add_header("Accept-Language", "zh-CN,zh;q=0.8")

    req.add_header("Accept-Encoding", "gzip, deflate, br")

    req.add_header("Connection", "keep-alive")

    req.add_header("Content-Type", content_type)

    req.add_header("Host", "forms.hubspot.com")

    req.add_header("Content-Length", "2185")

    req.add_header("Cache-Control", "max-age=0")

    req.add_header("Origin", "http://resources.newzoo.com")

    req.add_header("Referer", "http://resources.newzoo.com/2017-newzoo-global-esports-market-report-light")

    req.add_header("Upgrade-Insecure-Requests", "1")

    try:

        response = urllib2.urlopen(req)

        the_page = response.read().decode('utf-8')

        print the_page

        return the_page

    except Exception, e:
        print e

    except urllib2.HTTPError, e:

        print e.code

        pass

    except urllib2.URLError, e:

        print str(e)

        pass


def encode_multipart_formdata(key, value):
    BOUNDARY = '----WebKitFormBoundary5xiGkFqzWHj3si3e'

    CRLF = '\n'

    L = []

    L.append('--' + BOUNDARY)

    L.append('Content-Disposition: form-data; name="%s"' % "firstname")

    L.append('')

    L.append('aaa')

    L.append('--' + BOUNDARY)

    L.append('Content-Disposition: form-data; name="%s"' % "lastname")

    L.append('')

    L.append('bbb')

    L.append('--' + BOUNDARY)

    L.append('Content-Disposition: form-data; name="%s"' % "email")

    L.append('')

    L.append('2023947099@qq.com')

    L.append('--' + BOUNDARY)

    L.append('Content-Disposition: form-data; name="%s"' % "role")

    L.append('')

    L.append('CEO/Founder')

    L.append('--' + BOUNDARY)

    L.append('Content-Disposition: form-data; name="%s"' % "hs_context")

    L.append('')

    L.append('{"rumScriptExecuteTime":332.685,"rumServiceResponseTime":861.1200000000001,"rumFormRenderTime":494.175,"rumTotalRequestTime":490.0050000000001,"pageUrl":"http://resources.newzoo.com/2017-newzoo-global-esports-market-report-light","pageTitle":"2017 Newzoo Global Esports Market Report Light","source":"FormsNext-static-1.523","isHostedOnHubspot":true,"timestamp":1502692006360,"userAgent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36","referrer":"","hutk":"3fdbdb26c9fc848d17d616654678cfe6","originalEmbedContext":{"portalId":"700740","formId":"a607e9a6-4db0-4d67-b8b0-fa301915187d","formInstanceId":"1330","pageId":4838662505,"pageName":"2017 Newzoo Global Esports Market Report Light","redirectUrl":"http://resources.newzoo.com/thank-you-esports-report-light-2017","css":"","target":"#hs_form_target_module_13885066546126190","contentType":"landing-page","formData":{"cssClass":"hs-form stacked hs-custom-form"}},"recentFieldsCookie":{},"pageId":"4838662505","pageName":"2017 Newzoo Global Esports Market Report Light","boolCheckBoxFields":"","dateFields":"","redirectUrl":"http://resources.newzoo.com/thank-you-esports-report-light-2017","formInstanceId":"1330","smartFields":{"blog_default_hubspot_blog_subscription":"false"},"urlParams":{},"formValidity":{"firstname":{"valid":true,"errors":[]},"lastname":{"valid":true,"errors":[]},"email":{"valid":true,"errors":[]},"role":{"valid":true,"errors":[]}},"domFields":["firstname","lastname","email"],"correlationId":"ead47989-6321-42cb-8333-c8f23886e51a","disableCookieSubmission":false,"usingInvisibleRecaptcha":false}')

    L.append('--' + BOUNDARY + '--')

    L.append('')

    body = CRLF.join(L)

    # print body

    content_type = 'multipart/form-data; boundary=%s' % BOUNDARY

    return content_type, body


if __name__ == '__main__':
    # parameters = {'id': '', 'user': {'username': '', 'password': ''},
    #               'query': {'fromStation': 'beijing', 'fromStationText': 'shanghai'}}
    #
    # jdata = json.dumps(parameters)

    post_data()