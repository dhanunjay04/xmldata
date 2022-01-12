# press the green button in the gutter to run the script.
import os
import threading
from com.ciena.bpuaa.pmparsing.process_new_file import parse_xml_files
import configparser

if __name__ == '__main__':
    # os.chdir("/centina/med/NokiaNsp/processingXmls/")
    os.chdir("/Users/dvarakal/workspace/others/special/docomo/allxmls")
    path = os.getcwd() + "/"
    config = configparser.RawConfigParser()
    config.read('/Users/dvarakal/Documents/Documents-Desktop/nsp.properties')
    # config.read('/centina/med/NokiaNsp/nsp.properties')
    if config['DEFAULT']['nodeId'] is not None:
        # neFilesPath = "/centina/med/pm_files/" + config['DEFAULT']['nodeId'] + "/"
        neFilesPath = "/Users/dvarakal/Documents/Documents-Desktop/" + config['DEFAULT']['nodeId'] + "/"
        t1 = threading.Thread(target=parse_xml_files, args=(path, neFilesPath,))
        t1.start()
