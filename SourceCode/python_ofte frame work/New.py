'''
Created on 17 Jul 2017

@author: rlakkimsetti
'''
import os
import time
from time import sleep
from OFTE import FileSplitting
# from kaf import Consumer
d1={}
def timer(d):
    d1=d;
    s=0
    while s<=60:
        
        
    #os.system('cls')
       #print s
       #get the trigger directory if present 
       for file in os.listdir(d1.get('sourceFile').replace('\\','/')):
           
           #
#          Consumer.consuming()
           if file.endswith(".txt"):
               #print( file)
               FileSplitting.split(d1,file)
               
               time.sleep(20)
       s+=1
       if s==60:
          s=0
    
    
