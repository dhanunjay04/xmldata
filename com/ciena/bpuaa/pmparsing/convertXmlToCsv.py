import logging
import os
import xml.etree.ElementTree as ET
import csv

# Metrics list

# combined_services_metric_list = ["monitoredObjectSiteId", "portId", "timeCaptured", "inProfilePktsForwarded",
#                         "inProfilePktsDropped", "outOfProfilePktsForwarded", "outOfProfilePktsDropped",
#                         "inProfileOctetsForwarded", "inProfileOctetsDropped",
#                         "outOfProfileOctetsForwarded", "outOfProfileOctetsDropped"]

services_mpls_metric_list = ["monitoredObjectSiteId", "displayedName", "timeCaptured", "aggregatePkts",
                             "aggregateOctets"]

services_gress_metric_list = ["monitoredObjectSiteId", "displayedName", "timeCaptured", "ingress-highPktsOffered-1",
                              "ingress-highPktsDropped-1", "ingress-lowPktsOffered-1", "ingress-lowPktsDropped-1",
                              "ingress-highOctetsOffered-1", "ingress-highOctetsDropped-1",
                              "ingress-lowOctetsOffered-1", "ingress-lowOctetsDropped-1",
                              "ingress-inProfilePktsForwarded-1", "ingress-inProfileOctetsForwarded-1",
                              "ingress-outOfProfilePktsForwarded-1", "ingress-outOfProfileOctetsForwarded-1",
                              "ingress-highPktsOffered-11", "ingress-highPktsDropped-11", "ingress-lowPktsOffered-11",
                              "ingress-lowPktsDropped-11", "ingress-highOctetsOffered-11",
                              "ingress-highOctetsDropped-11", "ingress-lowOctetsOffered-11",
                              "ingress-lowOctetsDropped-11", "ingress-inProfilePktsForwarded-11",
                              "ingress-inProfileOctetsForwarded-11", "ingress-outOfProfilePktsForwarded-11",
                              "ingress-outOfProfileOctetsForwarded-11",
                              "egress-inProfilePktsForwarded-8", "egress-inProfilePktsDropped-8",
                              "egress-inProfileOctetsForwarded-8", "egress-inProfileOctetsDropped-8",
                              "egress-outOfProfilePktsForwarded-8", "egress-outOfProfilePktsDropped-8",
                              "egress-outOfProfileOctetsForwarded-8", "egress-outOfProfileOctetsDropped-8",
                              "egress-inProfilePktsForwarded-5", "egress-inProfilePktsDropped-5",
                              "egress-inProfileOctetsForwarded-5", "egress-inProfileOctetsDropped-5",
                              "egress-outOfProfilePktsForwarded-5", "egress-outOfProfilePktsDropped-5",
                              "egress-outOfProfileOctetsForwarded-5", "egress-outOfProfileOctetsDropped-5",
                              "egress-inProfilePktsForwarded-4", "egress-inProfilePktsDropped-4",
                              "egress-inProfileOctetsForwarded-4", "egress-inProfileOctetsDropped-4",
                              "egress-outOfProfilePktsForwarded-4", "egress-outOfProfilePktsDropped-4",
                              "egress-outOfProfileOctetsForwarded-4", "egress-outOfProfileOctetsDropped-4",
                              "egress-inProfilePktsForwarded-3", "egress-inProfilePktsDropped-3",
                              "egress-inProfileOctetsForwarded-3", "egress-inProfileOctetsDropped-3",
                              "egress-outOfProfilePktsForwarded-3", "egress-outOfProfilePktsDropped-3",
                              "egress-outOfProfileOctetsForwarded-3", "egress-outOfProfileOctetsDropped-3",
                              "egress-inProfilePktsForwarded-2", "egress-inProfilePktsDropped-2",
                              "egress-inProfileOctetsForwarded-2", "egress-inProfileOctetsDropped-2",
                              "egress-outOfProfilePktsForwarded-2", "egress-outOfProfilePktsDropped-2",
                              "egress-outOfProfileOctetsForwarded-2", "egress-outOfProfileOctetsDropped-2",
                              "egress-inProfilePktsForwarded-1", "egress-inProfilePktsDropped-1",
                              "egress-inProfileOctetsForwarded-1", "egress-inProfileOctetsDropped-1",
                              "egress-outOfProfilePktsForwarded-1", "egress-outOfProfilePktsDropped-1",
                              "egress-outOfProfileOctetsForwarded-1", "egress-outOfProfileOctetsDropped-1"]

services_saspm_twl_metric_list = ["monitoredObjectSiteId", "monitoredObjectPointer", "timeCaptured", "delayTwl2wyMin",
                                  "delayTwl2wyMax", "delayTwl2wyAvg", "delayTwlFwdMin", "delayTwlFwdMax",
                                  "delayTwlFwdAvg", "delayTwlBwdMin", "delayTwlBwdMax", "delayTwlBwdAvg"]

system_common = ["monitoredObjectSiteId", "timeCaptured", "availableMemoryInKb", "systemCpuUsage",
                 "systemMemoryUsage", "systemMemoryUsageInKb"]

eqpt_card_metric_list = ["monitoredObjectSiteId", "displayedName", "timeCaptured", "cpuIdle-1", "cpuIdle-60",
                         "cpuIdle-300", "memoryUsed", "memoryAvailable"]

eqpt_interface_metric_list = ["monitoredObjectSiteId", "displayedName", "timeCaptured",
                              "receivedTotalOctetsPeriodic", "receivedUnicastPacketsPeriodic",
                              "receivedMulticastPacketsPeriodic", "receivedBroadcastPacketsPeriodic",
                              "transmittedTotalOctetsPeriodic", "transmittedUnicastPacketsPeriodic",
                              "transmittedMulticastPacketsPeriodic", "transmittedBroadcastPacketsPeriodic"]


def parse_xml_file(path, name):
    try:
        xml = ET.parse(path + name)
        os.remove(path + name)
        name = name.replace(".xml", "")
        file_name = path + "../csvFiles/" + name + ".csv"
        root = xml.getroot()
        prefix = ""
        for child in root:
            prefix = str(child.tag).split("}")[0] + "}"
            break
        if root.find(prefix + "equipment.AvailableMemoryStatsLogRecord") is not None:
            get_system_stats(file_name, root, prefix, system_common,
                             "equipment.AvailableMemoryStatsLogRecord")
        elif root.find(prefix + "equipment.SystemCpuStatsLogRecord") is not None:
            get_system_stats(file_name, root, prefix, system_common,
                             "equipment.SystemCpuStatsLogRecord")
        elif root.find(prefix + "equipment.SystemMemoryStatsLogRecord") is not None:
            get_system_stats(file_name, root, prefix, system_common,
                             "equipment.SystemMemoryStatsLogRecord")
        # elif root.find(prefix + "service.CombinedQueueGroupNetworkEgressLogRecord") is not None:
        #     get_stats(file_name, root, prefix, combined_services_metric_list,
        #               "service.CombinedQueueGroupNetworkEgressLogRecord")
        elif root.find(prefix + "service.CompleteServiceIngressPacketOctetsLogRecord") is not None or \
                root.find(prefix + "service.CompleteServiceEgressPacketOctetsLogRecord") is not None:
            diction = {}
            if root.find(prefix + "service.CompleteServiceEgressPacketOctetsLogRecord") is not None:
                diction = get_service_stats(root, prefix, services_gress_metric_list,
                                            "service.CompleteServiceEgressPacketOctetsLogRecord", diction)
            if root.find(prefix + "service.CompleteServiceIngressPacketOctetsLogRecord") is not None:
                diction = get_service_stats(root, prefix, services_gress_metric_list,
                                            "service.CompleteServiceIngressPacketOctetsLogRecord", diction)
            try:
                write_file(file_name, services_gress_metric_list, diction)
            except Exception:
                logging.error(" Error while creating system stats: ", Exception)
        elif root.find(prefix + "mpls.MplsLspEgressStatsLogRecord") is not None:
            get_stats(file_name, root, prefix, services_mpls_metric_list,
                      "mpls.MplsLspEgressStatsLogRecord")
        elif root.find(prefix + "saspm.TWLSessionAccStatsLogRecord") is not None:
            get_stats(file_name, root, prefix, services_saspm_twl_metric_list,
                      "saspm.TWLSessionAccStatsLogRecord")
        elif root.find(prefix + "equipment.InterfaceAdditionalStatsLogRecord") is not None:
            get_stats(file_name, root, prefix, eqpt_interface_metric_list,
                      "equipment.InterfaceAdditionalStatsLogRecord")
        elif root.find(prefix + "equipment.CpuUtilizationStatsLogRecord") is not None:
            get_cpu_stats(file_name, root, prefix, eqpt_card_metric_list,
                          "equipment.CpuUtilizationStatsLogRecord")
        elif root.find(prefix + "equipment.MemoryUtilizationStatsLogRecord") is not None:
            get_cpu_stats(file_name, root, prefix, eqpt_card_metric_list,
                          "equipment.MemoryUtilizationStatsLogRecord")
    except Exception:
        logging.error("Error while parsing the xml file " + path + name, Exception.with_traceback())


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


def get_service_values(obj, prefix, metricList):
    values = []
    for metric in metricList:
        if metric.__eq__("timeCaptured"):
            values.append(my_time(obj.find(prefix + metric).text))
        elif metric.__contains__("-"):
            try:
                queue = metric.split("-")[2]
                tag = metric.split("-")[1]
                values.append(get_queue_value(obj, prefix, tag, queue))
            except Exception:
                values.append("")
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


def get_queue_value(obj, prefix, tag, match):
    m_value = ""
    if obj.find(prefix + "queueId") is not None and obj.find(prefix + "queueId").text.__eq__(match):
        m_value = obj.find(prefix + tag).text
    return m_value


def get_system_stats(file_name, root, prefix, metricList, tagName):
    diction = {}
    # FOR EACH Object
    for obj in root.findall(prefix + tagName):
        values = get_values(obj, prefix, metricList)
        monitoredObjectSiteId = values[0]
        if diction.keys().__contains__(monitoredObjectSiteId) and diction[monitoredObjectSiteId] is not None:
            temp_values = diction[monitoredObjectSiteId]
            if values[2] is not None and not values[2].__eq__(""):
                temp_values[2] = values[2]
            if values[3] is not None and not values[3].__eq__(""):
                temp_values[3] = values[3]
            if values[4] is not None and not values[4].__eq__(""):
                temp_values[4] = values[4]
            if values[5] is not None and not values[5].__eq__(""):
                temp_values[5] = values[5]
            diction[monitoredObjectSiteId] = temp_values
        else:
            diction[monitoredObjectSiteId] = values
    try:
        write_file(file_name, metricList, diction)
    except Exception:
        logging.error(" Error while creating system stats: ", Exception)


def get_service_stats(root, prefix, metricList, tagName, diction):
    # FOR EACH Object
    for obj in root.findall(prefix + tagName):
        values = get_service_values(obj, prefix, metricList)
        monitoredObjectSiteId = values[0] + values[1]
        if diction.keys().__contains__(monitoredObjectSiteId) and diction[monitoredObjectSiteId] is not None:
            temp_values = diction[monitoredObjectSiteId]
            for i in range(72):
                no = i + 2
                if values[no] is not None and not values[no].__eq__(""):
                    temp_values[no] = values[no]
            diction[monitoredObjectSiteId] = temp_values
        else:
            diction[monitoredObjectSiteId] = values
    return diction


def get_stats(file_name, root, prefix, metricList, tagName):
    diction = {}
    for obj in root.findall(prefix + tagName):
        values = []
        values = get_values(obj, prefix, metricList)
        monitoredObjectSiteId = values[0] + values[1]
        diction[monitoredObjectSiteId] = values
    # ADD THE HEADER TO CSV FILE
    try:
        write_file(file_name, metricList, diction)
    except:
        logging.error(" Error while creating system stats: ")


# noinspection PyBroadException
def get_cpu_stats(file_name, root, prefix, metricList, tagName):
    diction = {}
    for obj in root.findall(prefix + tagName):
        values = get_values(obj, prefix, metricList)
        monitoredObjectSiteId = values[0] + values[1]
        if diction.keys().__contains__(monitoredObjectSiteId) and diction[monitoredObjectSiteId] is not None:
            temp_values = diction[monitoredObjectSiteId]
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
            diction[monitoredObjectSiteId] = temp_values
        else:
            diction[monitoredObjectSiteId] = values
    try:
        write_file(file_name, metricList, diction)
    except:
        logging.error(" Error while creating system stats: ")


def write_file(file_name, metric_list, diction):
    # ADD THE HEADER TO CSV FILE
    with open(file_name, 'w') as csvFile:
        # creating a csv writer object
        csvFile_writer = csv.writer(csvFile)
        csvFile_writer.writerow(metric_list)
        # FOR EACH Object
        for key in sorted(diction):
            # ADD A NEW ROW TO CSV FILE
            csvFile_writer.writerow(diction[key])


def my_time(timeCaptured):
    import datetime
    return datetime.datetime.fromtimestamp(float(timeCaptured) / 1000).strftime('%Y-%m-%d %H:%M:%S')
