# press the green button in the gutter to run the script.
import os
import threading
from com.ciena.bpuaa.pmparsing.process_new_file import parse_xml_files


if __name__ == '__main__':
    os.chdir(os.getcwd() + "/../../../../responseXmls/")
    path = os.getcwd() + "/"
    t1 = threading.Thread(target=parse_xml_files, args=(path,))
    t1.start()
    # parse_xml_files(path)