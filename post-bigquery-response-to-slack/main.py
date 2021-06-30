import base64
import json
import os

# main function
def main_function(event, context=None):
    try:
        attributes = event['attributes']
        print(attributes)

    except Exception as e:
        print(e)     
        