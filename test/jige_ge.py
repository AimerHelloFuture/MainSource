#!/usr/bin/env python
# -*- coding: utf-8 -*-

import urllib2

import json


def post_data():
    parameters = {'id': '', 'user': {'username': '', 'password': ''},
                  'query': {'fromStation': 'beijing', 'fromStationText': 'shanghai'}}

    jdata = json.dumps(parameters)

    post_multipart('http://extention.ie.sogou.com/mm_12306/prebook', jdata)


def post_multipart(url, fields):
    content_type, body = encode_multipart_formdata("data", fields)

    req = urllib2.Request(url, body)

    req.add_header("User-Agent",
                   "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36 SE 2.X MetaSr 1.0)")

    req.add_header("Accept", "*/*")

    req.add_header("Accept-Language", "zh-CN,zh;q=0.8")

    req.add_header("Accept-Encoding", "gzip,deflate,sdch")

    req.add_header("Connection", "keep-alive")

    req.add_header("Content-Type", content_type)

    req.add_header("User-Agent1", "SogouMSE")

    try:

        response = urllib2.urlopen(req)

        the_page = response.read().decode('utf-8')

        print the_page

        return the_page

    except urllib2.HTTPError, e:

        print e.code

        pass

    except urllib2.URLError, e:

        print str(e)

        pass


def encode_multipart_formdata(key, value):
    BOUNDARY = '----------ThIs_Is_tHe_bouNdaRY_$'

    CRLF = '\r\n'

    L = []

    L.append('--' + BOUNDARY)

    L.append('Content-Disposition: form-data; name="%s"' % key)

    L.append('')

    L.append(value)

    L.append('--' + BOUNDARY + '--')

    L.append('')

    body = CRLF.join(L)

    content_type = 'multipart/form-data; boundary=%s' % BOUNDARY

    return content_type, body


if __name__ == '__main__':
    post_data()