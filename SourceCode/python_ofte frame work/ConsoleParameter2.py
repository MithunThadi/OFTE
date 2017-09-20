'''
Created on Jul 25, 2017

@author: rlakkimsetti
'''
import sys

from xml.dom import minidom
from xml.etree import ElementTree
from xml.etree.ElementTree import Element, SubElement, Comment, tostring
#from OFTE import TimerCheck
from OFTE import TimerCheck
from OFTE import New
d={}
sys.argv = raw_input('Enter command line arguments: ').split()
for i in range(len(sys.argv)):
    #print("in loop")
    if(str(sys.argv[i]) == '-dd'):
        destinationDirectory=sys.argv[i+1]
        d.update({'destinationDirectory': destinationDirectory})
        sourceFile=sys.argv[i+2]
        d.update({'sourceFile': sourceFile})
    elif(str(sys.argv[i]) == '-df'):
        destiationFile=sys.argv[i+1]
        d.update({'destiationFile':destiationFile})
        sourceFile=sys.argv[i+2]
        d.update({'sourceFile':sourceFile})
    elif(str(sys.argv[i]) == '-tr'):
        triggerPattern=sys.argv[i+1]
        d.update({'triggerPattern':triggerPattern})
    elif(str(sys.argv[i]) == '-trd'):
        triggerCondition=sys.argv[i+1]
        d.update({'triggerCondition':triggerCondition})
    elif(str(sys.argv[i]) == '-pi'):
        pollInterval=sys.argv[i+1]
        d.update({'pollInterval':pollInterval})
    elif(str(sys.argv[i]) == '-pu'):
        pollUnits=sys.argv[i+1]
        d.update({'pollUnits':pollUnits})
    elif(str(sys.argv[i]) == '-jn'):
        jobName=sys.argv[i+1]
        d.update({'jobName':jobName})
    elif(str(sys.argv[i]) == '-gt'):
        xmlFilePath=sys.argv[i+1]
        d.update({'xmlFilePath':xmlFilePath})
        
print(d)

def prettif(elem):
    """Return a pretty-printed XML string for the Element.
    """
    rough_string = ElementTree.tostring(elem, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")

ManagedCall = Element('managedCall')

originator = SubElement(ManagedCall, 'originator')

hostName = SubElement(originator, 'hostName')
hostName.text = 'localhost'

userId = SubElement(originator, 'userId')
userId.text = 'miracle'

TransferSet = SubElement(ManagedCall, 'transferSet')

Item = SubElement(TransferSet, 'item')

Source = SubElement(Item, 'source')

SourceFile = SubElement (Source, 'file')
if 'sourceFile' in d.keys():
    SourceFile.text = d.get('sourceFile')
SourceFilePattern = SubElement (Source, 'filePattern')
if 'filePattern' in d.keys():
    SourceFilePattern.text = d.get('filePattern')
SourceTriggerPattern = SubElement(Source, 'triggerPattern')
if 'triggerPattern' in d.keys():
    SourceTriggerPattern.text = d.get('triggerPattern')

Destination = SubElement(Item, 'destination')
DestinationFile = SubElement (Destination, 'file')
if 'destinationDirectory' in d.keys():
    DestinationFile.text = d.get('destinationDirectory')
DestinationFilePattern = SubElement (Destination, 'filePatternd')
if 'filePatternd' in d.keys():
    DestinationFilePattern.text = d.get('filePatternd')
DestinationTriggerPattern = SubElement(Destination, 'triggerPatternd')
if 'triggerCondition' in d.keys():
    DestinationTriggerPattern.text = d.get('triggerCondition')

Poll = SubElement(TransferSet, 'poll')
Units = SubElement(Poll, 'pollUnits')
if 'pollUnits' in d.keys():
    Units.text = d.get('pollUnits')
Interval = SubElement(Poll, 'pollInterval')
if 'pollInterval' in d.keys():
    Interval.text = d.get('pollInterval')

Job = SubElement(ManagedCall, 'job')
JobName = SubElement(Job, 'jobName')
if 'jobName' in d.keys():
    JobName.text = d.get('jobName')

if __name__ == '__main__':
    # print(tostring(ManagedCall))
    file = open("D:\Test\PythonMonitors\monitortest6.xml", "w")
    file.write(prettif(ManagedCall))
    file.close()
    print (prettif(ManagedCall))
    #pass
   # TimerCheck.timer(d)
    New.timer(d)