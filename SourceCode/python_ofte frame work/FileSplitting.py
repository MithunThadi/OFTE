import os
import threading, logging, time
import multiprocessing
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.client import KafkaClient
from OFTE import ProdCons
#from OFTE.ConsoleParameters1 import file
d2={}
# t='four4'

def split(d2,file1):
    #filename="D:/pyt/meen119.txt"
    file2=d2.get('sourceFile').replace('\\','/')+'/'+file1
    splitsize= 1024*1024
    print (file1)
    print (file2)
    orginalfilename = file2.split(".")
    #if file2.isfile(file2):
#     print("No such file as: \"%s\"" % file2)
    with open(file2,"rb") as fr:
        fr.seek(0,2) # move to end of the file
        givenfilesize=fr.tell()
        print("getfilesize: size: %s" % givenfilesize)
        n_split=(-(-givenfilesize//splitsize))
        
        print(n_split)
        counter=1
        #fr.seek(0)
        fr.seek(0)
        for i in range(n_split):
            data=fr.read(splitsize)
            #print(data)
#             with open(orginalfilename[0]+"{id}.".format(id=str(counter))+orginalfilename[1],"wb") as fw:
#                 fw.write(data)
#                 fw.close()
            
#             kafkaProduce(data)
            c=ProdCons.Producer()
            ProdCons.Producer.run1(c, data)
#             con=ProdCons.Consumer()
#             ProdCons.Consumer.run1(con)
#             kafkaConsumer()
#             kafkaConsume(t)
            print(fr.tell())
            fr.seek(fr.tell())
                #fr.seek(splitsize)
#             counter+=1
#             if(fr.tell()==givenfilesize):
#                 con=ProdCons.Consumer()
#                 ProdCons.Consumer.run1(con)
#             con=ProdCons.Consumer()
#             ProdCons.Consumer.run1(con)  
# def consume():
#     con=ProdCons.Consumer()
#     ProdCons.Consumer.run1(con)
            
#             if (fr.tell==)
#     else:
#         print ('elseblock')            
# def kafkaProduce(data):
#     
#     producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
#     #topic = "third3"
#     print("Topic Created")
# # # producer = KafkaProducer(kafka)
# #     #producer.send(topic, "Message for the topic just for sending Hello"+ str(datetime.now().time()) )
#     producer.send(t,data)
#     print("Topic Created and Message published")
#     time.sleep(1)
#     producer.close()
#     kafkaConsume(t)
#     kafkaDataConsume(t)
#     consumer = KafkaConsumer(topic, bootstrap_servers='localhost:9092')
#     for message in consumer:
#         print ('from consumer:::::::::::::::::::::::::::::')
#         print(message)
     
# def kafkaConsume(topic):
#     daemon=True
#     consumer = KafkaConsumer(topic, bootstrap_servers='localhost:9092')
#     for message in consumer:
#         print ('from consumer:::::::::::::::::::::::::::::')
#         print(message.value)
#         file = open('D:\\Test\\Destination\\testfile.txt','ab')
#         file.write(message.value)
#         file.close()
# def kafkaConsumer():
#     kafka = KafkaClient(bootstrap_servers=['localhost:9092'],auto_offset_reset='latest', enable_auto_commit=True,session_timeout_ms=1000)
#     print("After connecting to kafka in fileSplittting")
#     consumer = KafkaConsumer("topic", "four4")
#     
#     print("subscribing")
# # consumer = KafkaConsumer('test', bootstrap_servers='localhost:9092')
#     for message in consumer:
#         print(message.value)
#         file = open('D:\\Test\\Destination\\testfile.txt','ab')
#         file.write(message.value)
#         file.close()
#     consumer(session_timeout_ms=3000)
#     KafkaClient.close()
# def kafkaDataConsume(t):
#     
#     print ('from consumer::::::::::::;')
#     consumer = KafkaConsumer("topic", t)
#     
#     for message in consumer:
#          print(message.value) 
     
                
                
            
            