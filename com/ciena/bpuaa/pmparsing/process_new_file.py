import os
from com.ciena.bpuaa.pmparsing.convertXmlToCsv import parse_xml_file


def parse_xml_files(path):
    # PARSE XML
    retry = True
    while retry:
        try:
            fileNames = os.listdir(path)
            for name in fileNames:
                if name.__contains__(".xml"):
                    parse_xml_file(path, name)
        except:
            retry = True

