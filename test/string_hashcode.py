#!/usr/bin/env python3
# -*-coding:utf-8-*-
"""Copyright: Copyright (c) 2018 Asiainfo
    @Description: ***新建，填写用途***
    @fileName: string_hashcode.py
    @version: v1.0.0
    @author:  mazhe
    @created on: 2018/10/26 1:31 PM 17:29
"""

import hashlib

user_number = "15910458603"
day = "20180920"
store_str = user_number + day

store_md5 = hashlib.md5(store_str.encode("utf-8")).hexdigest()
collection_md5 = hashlib.md5(user_number.encode("utf-8")).hexdigest()
# print(store_md5)
# print(collection_md5)


def get_hashcode(s):
    h = 0
    n = len(s)
    for i, c in enumerate(s):
        h += ord(c) * 31**(n - i - 1)
    return h


store_hashcode = get_hashcode(store_md5)
collection_hashcode = get_hashcode(collection_md5)
max_int = 2147483647
store_mod = (store_hashcode & max_int) % 360
collection_mod = (collection_hashcode & max_int) % 1

print("Store number:{},Collection number:{}".format(store_mod, collection_mod))
