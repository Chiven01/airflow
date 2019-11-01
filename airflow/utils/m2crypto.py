#!/usr/bin/env python
# coding=utf-8

import os
import base64
from urllib.parse import unquote
import M2Crypto


ptoken = """BypZbX86xpNv32EUZr1uk8V%2Fw4OjhpN9i54WgfgiL%2BJ9K0RCXvIVks8Jwork6e7jbtUapH7WI%2BaXhfNLyXC6qgfjYPsgMNtwG7xut3SQ8No6%2B64XvjmioR6y%2BOXc%2FKmQkVPDQHjELAeUCoKQHnm9Trq%2FhVeG0dnBC5HZSKJl%2Fjbz4vglNo%2FF4Y0%2BaII5gRaGyMaA1FLxrrvzrr%2F2B7YssIFPnvhs5XexVFwm%2FdrIeY5yyZQLSbBcikAjXIINwRDsCqEDQZhIukqoQx%2F%2FgxkHsYiqemlpT8kDwwVj9FVnq6X8iNVPH72q0MG9Gpr%2BH61Gk7sG6bKNE%2F8uX1XgG9gqIQ%3D%3D"""

def rsa_decrypt(ptoken):

    # url decode
    data = unquote(ptoken)
    
    # decode base64
    strcode = base64.b64decode(data)
    
    # load public key
    path1=os.path.abspath('.')   # 表示当前所处的文件夹的绝对路径
    print(path1)
    pkey = M2Crypto.RSA.load_pub_key('/search/odin/airflow/pub.pem')
    
    # decrypt
    output = pkey.public_decrypt(strcode, M2Crypto.RSA.pkcs1_padding)
    
    # convert bytes to string
    print(output.decode('utf-8', 'ignore'))
    
    return eval(output)
