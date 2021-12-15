import os
import time
import xml.etree.ElementTree as ET
import csv

# Metrics list
from multiprocessing import Process

from com.ciena.bpuaa.pmparsing.consumer import consumeMsg

services_metric_list = ["monitoredObjectSiteId", "portId", "timeCaptured", "inProfilePktsForwarded",
                      "inProfilePktsDropped", "outOfProfilePktsForwarded", "outOfProfilePktsDropped",
                      "inProfileOctetsForwarded", "inProfileOctetsDropped",
                      "outOfProfileOctetsForwarded", "outOfProfileOctetsDropped"]

system_common = ["monitoredObjectSiteId", "timeCaptured", "availableMemoryInKb", "systemCpuUsage",
                 "systemMemoryUsage", "systemMemoryUsageInKb"]

eqpt_card_metric_list = ["monitoredObjectSiteId", "displayedName", "timeCaptured", "cpuIdle-1", "cpuIdle-60",
                         "cpuIdle-300", "memoryUsed", "memoryAvailable"]

eqpt_interface_metric_list = ["monitoredObjectSiteId", "displayedName", "timeCaptured",
                           "receivedTotalOctetsPeriodic", "receivedUnicastPacketsPeriodic",
                           "receivedMulticastPacketsPeriodic", "receivedBroadcastPacketsPeriodic",
                           "transmittedTotalOctetsPeriodic", "transmittedUnicastPacketsPeriodic",
                           "transmittedMulticastPacketsPeriodic", "transmittedBroadcastPacketsPeriodic"]


def parse_xml_files(path):
    # PARSE XML
    fileNames = os.listdir(path)
    sys_dic = {}
    card_dic = {}
    for name in fileNames:
        xml = ET.parse(path + name)
        name = name.replace(".xml", "")
        root = xml.getroot()
        prefix = ""
        for child in root:
            prefix = str(child.tag).split("}")[0] + "}"
            break
        if root.find(prefix + "equipment.AvailableMemoryStatsLogRecord") is not None:
            sys_dic = get_system_stats(root, prefix, system_common,
                                       "equipment.AvailableMemoryStatsLogRecord", sys_dic)
        elif root.find(prefix + "equipment.SystemCpuStatsLogRecord") is not None:
            sys_dic = get_system_stats(root, prefix, system_common,
                                       "equipment.SystemCpuStatsLogRecord", sys_dic)
        elif root.find(prefix + "equipment.SystemMemoryStatsLogRecord") is not None:
            sys_dic = get_system_stats(root, prefix, system_common,
                                       "equipment.SystemMemoryStatsLogRecord", sys_dic)
        elif root.find(prefix + "service.CombinedQueueGroupNetworkEgressLogRecord") is not None:
            get_stats(path + "../csvFiles/" + name + ".csv", root, prefix, services_metric_list,
                     "service.CombinedQueueGroupNetworkEgressLogRecord")
        elif root.find(prefix + "equipment.CpuUtilizationStatsLogRecord") is not None:
            card_dic = get_cpu_stats(root, prefix, eqpt_card_metric_list,
                                     "equipment.CpuUtilizationStatsLogRecord", card_dic)
        elif root.find(prefix + "equipment.InterfaceAdditionalStatsLogRecord") is not None:
            get_stats(path + "../csvFiles/" + name + ".csv", root, prefix, eqpt_interface_metric_list,
                     "equipment.InterfaceAdditionalStatsLogRecord")
        elif root.find(prefix + "equipment.MemoryUtilizationStatsLogRecord") is not None:
            card_dic = get_cpu_stats(root, prefix, eqpt_card_metric_list,
                                     "equipment.MemoryUtilizationStatsLogRecord", card_dic)
    write_file(path + "../csvFiles/system_stats.csv", system_common, sys_dic)
    write_file(path + "../csvFiles/card_stats.csv", eqpt_card_metric_list, card_dic)


def get_values(obj, prefix, metricList):
    values = []
    for metric in metricList:
        if metric.__eq__("timeCaptured"):
            values.append(my_time(obj.find(prefix + metric).text))
        elif metric.__eq__("cpuIdle-1"):
            values.append(get_sampling_value(obj, prefix, "1"))
        elif metric.__eq__("cpuIdle-60"):
            values.append(get_sampling_value(obj, prefix, "60"))
        elif metric.__eq__("cpuIdle-300"):
            values.append(get_sampling_value(obj, prefix, "300"))
        elif obj.find(prefix + metric) is not None:
            values.append(obj.find(prefix + metric).text)
        else:
            values.append("")
    return values


def get_sampling_value(obj, prefix, match):
    m_value = ""
    if obj.find(prefix + "samplingTime") is not None and obj.find(prefix + "samplingTime").text.__eq__(match):
        m_value = obj.find(prefix + "cpuIdle").text
    return m_value


def get_system_stats(root, prefix, metricList, tagName, dict):
    # FOR EACH Object
    for obj in root.findall(prefix + tagName):
        values = get_values(obj, prefix, metricList)
        monitoredObjectSiteId = values[0]
        if dict.keys().__contains__(monitoredObjectSiteId) and dict[monitoredObjectSiteId] is not None:
            temp_values = dict[monitoredObjectSiteId]
            if values[2] is not None and not values[2].__eq__(""):
                temp_values[2] = values[2]
            if values[3] is not None and not values[3].__eq__(""):
                temp_values[3] = values[3]
            if values[4] is not None and not values[4].__eq__(""):
                temp_values[4] = values[4]
            if values[5] is not None and not values[5].__eq__(""):
                temp_values[5] = values[5]
            dict[monitoredObjectSiteId] = temp_values
        else:
            dict[monitoredObjectSiteId] = values
    return dict


def get_stats(file_name, root, prefix, metricList, tagName):
    dict = {}
    for obj in root.findall(prefix + tagName):
        values = []
        values = get_values(obj, prefix, metricList)
        monitoredObjectSiteId = values[0] + values[1]
        dict[monitoredObjectSiteId] = values
    # ADD THE HEADER TO CSV FILE
    write_file(file_name, metricList, dict)


def get_cpu_stats(root, prefix, metricList, tagName, dict):
    for obj in root.findall(prefix + tagName):
        values = get_values(obj, prefix, metricList)
        monitoredObjectSiteId = values[0] + values[1]
        if dict.keys().__contains__(monitoredObjectSiteId) and dict[monitoredObjectSiteId] is not None:
            temp_values = dict[monitoredObjectSiteId]
            if values[3] is not None and not values[3].__eq__(""):
                temp_values[3] = values[3]
            if values[4] is not None and not values[4].__eq__(""):
                temp_values[4] = values[4]
            if values[5] is not None and not values[5].__eq__(""):
                temp_values[5] = values[5]
            if values[6] is not None and not values[6].__eq__(""):
                temp_values[6] = values[6]
            if values[7] is not None and not values[7].__eq__(""):
                temp_values[7] = values[7]
            dict[monitoredObjectSiteId] = temp_values
        else:
            dict[monitoredObjectSiteId] = values
    return dict


def write_file(file_name, metric_list, dict):
    # ADD THE HEADER TO CSV FILE
    with open(file_name, 'w') as csvFile:
        # creating a csv writer object
        csvFile_writer = csv.writer(csvFile)
        csvFile_writer.writerow(metric_list)
        # FOR EACH Object
        for key in sorted(dict):
            # ADD A NEW ROW TO CSV FILE
            csvFile_writer.writerow(dict[key])


def my_time(timeCaptured):
    import datetime
    return datetime.datetime.fromtimestamp(float(timeCaptured) / 1000).strftime('%Y-%m-%d %H:%M:%S')


# press the green button in the gutter to run the script.
if __name__ == '__main__':
    t1 = Process(target=consumeMsg, args=('test',))
    t1.start()
    # st = time.time()
    # parse_xml_files("/Users/dvarakal/workspace/others/special/docomo/pm-response/xmldata/responseXmls/")
    # parseXmls("/Users/dvarakal/wish/response/")
    # print(time.time() - st)
