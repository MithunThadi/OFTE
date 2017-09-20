'''
Created on 25 Jul 2017

@author: rlakkimsetti
'''
import threading, logging, time
import multiprocessing
# from OFTE import Newx
from kafka import KafkaProducer
from kafka import KafkaConsumer


class Producer(threading.Thread):
    daemon = True

    def run1(self,data):
        producer = KafkaProducer(bootstrap_servers='localhost:9093')

#         while True:
#         print(data)
        producer.send('n16', data)
#         producer.flush()
        
#             producer.send('my-topic', b"\xc2Hola, hello!")
#         time.sleep(1)
        producer.close()
        con=Consumer()
        Consumer.run1(con)
#         d=Consumer()
#         d.run1()


class Consumer(multiprocessing.Process):
    #daemon = True

    def run1(self):
#         consumer = KafkaConsumer(bootstrap_servers='localhost:9093',enable_auto_commit=True,auto_offset_reset='smallest',fetch_max_wait_ms=1000,max_partition_fetch_bytes=1073741824,
#                                 fetch_max_bytes=1073741824,auto_commit_interval_ms=1000,receive_buffer_bytes=32768)
        consumer = KafkaConsumer(bootstrap_servers='localhost:9093')
        consumer.subscribe(['n16'])

        for message in consumer:
#             print message.value
            print ('consumed')
            print (message.offset)
            
            file = open('D:\\Test\\Destination\\testfile.txt','ab')
            file.write(message.value)
            file.close()
            exit(0)
        consumer.close()
            
#             f=KafkaConsumer()
#             KafkaConsumer.close(f, autocommit=True)
#             consumer.unsubscribe()
#             consumer.close(f,autocommit=True)
#             f.close()
#             consumer.close(consumer,autocommit=True)
           # consumer.close()
#             consumer._closed
#         New.timer()
#         consumer.unsubscribe()
#         print('completed')
            

# def main():
#     tasks = [ Producer(), Consumer()]
# 
#     for t in tasks:
#         t.run1()
# 
# #     time.sleep(10)
# 
# if __name__ == "__main__":
#     logging.basicConfig(
#         format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
#         level=logging.INFO
#         )
#     main()