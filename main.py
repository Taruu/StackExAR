import concurrent.futures
import time

from app import app

import uvicorn

if __name__ == "__main__":

    uvicorn.run(app, host="0.0.0.0", port=8000)

# from py7zr import SevenZipFile
# import xml.etree.ElementTree as ET
#
# with SevenZipFile('worldbuilding.stackexchange.com.7z', 'r') as zip:
#     allfiles = zip.getnames()
#     print(allfiles)
#     for target in allfiles:
#         for fname, data in zip.read(targets=target).items():
#             parser = ET.XMLPullParser(['start', 'end'])
#             for chunk in iter(lambda: data.read(100), b''):
#                 parser.feed(chunk)
#                 for event, elem in parser.read_events():
#                     print(event)
#                     print(elem.tag)
#                     if elem.tag == 'row':
#                         print(elem.tag, elem.attrib)
