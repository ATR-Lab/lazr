#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jul 12 18:24:55 2020

spark config - documentation: https://spark.apache.org/docs/latest/configuration.html#overriding-configuration-directory

Purpose: Video processing

@author: skywalker
"""

print("--- Starting ---")


import cv2
import requests
import json

class Video_data:
    
    file_meta_data = None
    
    def __init__(self, time_, operation):
        
        self.query_transform  = datetime_query.split(',')
        
        self.query_info = (self.query_transform[0].strip()+"-"+self.query_transform[1].strip()+"-"+self.query_transform[2].strip()+"_"+self.query_transform[3].strip())
        
        print("", self.query_info)
        
        #blocked while testing the LoC
        self.endpoint = None
    
        
    def file_checksum(self):
        
        """
        Checksums are used to detect in any errors were caused
        during storage or file transmissions
        """
        
        temp0 = self.endpoint.split('50070')
        print(temp0)
        temp1 = temp0[1].split('=')
        print(temp1)
        checksum_endpoint = temp0[0]+"50075"+temp1[0]+"=GETFILECHECKSUM&namenoderpcaddress=localhost:9000"
        print(checksum_endpoint)
        file_checksum = requests.get(checksum_endpoint)
        
        return file_checksum
    
    
    
    def get_files(self):
        
        endpoint_directory = "http://skywalker-G7-7588:50070/webhdfs/v1/user/hduser?op=LISTSTATUS"

        a = requests.get(endpoint_directory)
        
        #data in bytes
        self.data = a.content
        
        
        file_metadata = json.loads(a.content)
        
        
        Video_data.file_meta_data = file_metadata.get('FileStatuses', {}).get('FileStatus',  {})
        
        print(type(Video_data.file_meta_data))
        #temp = Video_data.file_meta_data.get('pathSuffix')
        
        date, time = self.query_info.split('_')
        
        print("Given date: {} time:{}".format(date, time))
        
        
        files_list = list()
        
        for i in range(len(Video_data.file_meta_data)):
            
            method_metadata_split = Video_data.file_meta_data[i]['pathSuffix'].split("_")
            
            date_file_metadata = method_metadata_split[0]
            time_file_metadata = str(method_metadata_split[1])[:2]
            
            print(time_file_metadata)
            print(date_file_metadata)
            print("checking value: ",date, time)
            
            
            if((date_file_metadata == date) and time_file_metadata == time):
                
                print("Added to list")
                
                files_list.append(Video_data.file_meta_data[i]['pathSuffix'])
                
                        
        print(len(files_list))
        
        if(len(files_list)==0):
            
            message = "We do not have any entries for the provided time or date"
            return(message, files_list)
                    
        elif(len(files_list)>1):
            count = 0
            for i in range(len(files_list)):

                print("* [{}] -- To select please enter: {}".format(count, files_list[i]))
                print("------------------------------------")
                
                count+=1
            
            selected_file_point = int(input())
            
            print("You've selected: ",files_list[selected_file_point])
            
            self.endpoint = "http://skywalker-G7-7588:50070/webhdfs/v1/user/hduser/"+files_list[selected_file_point]+"?op=OPEN"
            
            return(self.endpoint, files_list[selected_file_point])
                
        else:
            
            self.endpoint = "http://skywalker-G7-7588:50070/webhdfs/v1/user/hduser/"+files_list[0]+"?op=OPEN"
                        
            return(self.endpoint, files_list[-1])
            
    
    def endpoint_value(self):
        
        return self.endpoint
    
    
    def show_video(self):
        
#        temp_path = "/home/skywalker/Desktop/NASA/"
#        video_binary_string = data
#        decoded_string = (video_binary_string)
#        
#        with open(temp_path+'/video1234.mp4', 'wb') as wfile:
#           wfile.write(decoded_string)
#        cap = cv2.VideoCapture(temp_path+'/video.mp4')
#        success, frame = cap.read()
        
        if self.endpoint == None:
            
            self.endpoint, filename_queried = self.get_files()
            
        else: 
            
            print("Selected endpoint", self.endpoint)
        
        #data = requests.get(self.endpoint)
        
        print("working")
        
        cap = cv2.VideoCapture(self.endpoint)
        
        #temp_path+ "video.mp4"
        
        while(cap.isOpened()):
            ret, frame = cap.read()
            if ret == True:
                gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
                cv2.imshow('frame', frame)
                # & 0xFF is required for a 64-bit system
                if cv2.waitKey(1) & 0xFF == ord('q'):
                    break
            else:
                print("working")
                break

        
    
    def fetch_frame(self):
        
        time_to_retrieve_seconds = input("Please enter the time in the format of mins, secs").split(',')
                
        total_seconds_retrieval_time = (int(time_to_retrieve_seconds[0])*60 + int(time_to_retrieve_seconds[1]))*100
        
        vidcap = cv2.VideoCapture(self.endpoint)
        vidcap.set(cv2.CAP_PROP_POS_MSEC, 20000)      # just cue to 20 sec. position
        success,image = vidcap.read()
        print(success, self.endpoint)
        if success:
            cv2.imwrite("frame20sec.jpg", image)     # save frame as JPEG file
            cv2.imshow(str(total_seconds_retrieval_time),image) #str(total_seconds_retrieval_time)
#            cv2.waitKey()
            cv2.waitKey(25000)
            cv2.destroyAllWindows()
            
    
    def operation(self, operation):
        
        #self.endpoint = self.get_files()
        
        if(operation == "open"):
            
            return self.show_video()
        
        elif(operation == "fetch frame"):
            
            return self.fetch_frame()
        
        elif(operation == "status"):
        
            return "---Yet to implement checksum operation---"
        
        elif(operation=="checksum"):
            
            return self.file_checksum()
            
        return("ALl conditions failed, please check your input")
    




"""

status - checking the status of file

open - viewing the video

fetch frame - retrieve particular frame

checksum - checking the integrity of file

"""
print()
#operation = str(input("Please enter your operation: "))

print("Please enter the time in 24-hour format to query the video file")
#datetime_query = input("Please enter in format of year, month, date, time : ")
#2020-07-17_112245.mp4
ab = Video_data(datetime_query, operation)

querying_hdfs_api_file, file_found = ab.get_files()

#ab.show_video()

ab.endpoint_value()

#ab.file_checksum()

ab.operation()






#
#
#
#
#
#
#import cv2
#
#vidcap = cv2.VideoCapture("/home/skywalker/Desktop/flask-movie-bag/2020-07-21_170850.mp4")
#vidcap.set(cv2.CAP_PROP_POS_MSEC, 98000)      # just cue to 20 sec. position
#success,image = vidcap.read()
#print(success)
#if success:
#    cv2.imwrite("frame20sec.jpg", image)     # save frame as JPEG file
#    cv2.imshow(str(200000),image) #str(total_seconds_retrieval_time)
##           cv2.waitKey()
#    cv2.waitKey(10000)
#    cv2.destroyAllWindows()
#            
#
#
#
#
#
#80000/1000
#




























#
#
#response = requests.put("http://skywalker-G7-7588:50070/webhdfs/v1/test/2/trial2.mp4?op=CREATE", allow_redirects = True)
#
#
#
#response.status_code
#
#response.headers['Location'] =  "http://skywalker-G7-7588:50075/webhdfs/v1/test/1/trial.mp4?op=GETFILECHECKSUM&namenoderpcaddress=localhost:9000"
#
#
#
#requests.get("http://skywalker-G7-7588:50070/webhdfs/v1/user/hduser/2020-07-17_112245.mp4?op=OPEN")
#
#
#sending = requests.put(a.url, file = open("/home/skywalker/Desktop/flask-movie-bag/2020-06-20_145450.mp4", "rb"))
#
#sending = requests.Request("PUT", url = response.url, files = open("/home/skywalker/Desktop/flask-movie-bag/2020-06-20_145450.mp4", "rb"))
#
#sending.url
#
#sending.__dict__
#
#
#response2 = requests.get("http://skywalker-G7-7588:50070/webhdfs/v1/test/1/trial.mp4?op=OPEN")
#
#










#---------------------------------Back up--------------------------------------
#
#endpoint = "http://skywalker-G7-7588:50070/webhdfs/v1/user/hduser/video_.mp4?op=LISTSTATUS"
#endpoint_open = "http://skywalker-G7-7588:50070/webhdfs/v1/user/hduser/video_.mp4?op=OPEN"
#
#
#endpoint_open = "http://skywalker-G7-7588:50070/webhdfs/v1/user/hduser/2020-06-20_145920.mp4?op=OPEN"
##/user/hduser/2020-06-20_145450.265856.mp4
##/user/hduser/2020-06-20_145920.mp4
##/user/hduser/2020-07-17_112245.mp4
#
#a = requests.get(endpoint_open)
#
#a.url


#time_to_retrieve_seconds = input("enter").split(',')
#total_seconds_retrieval_time = int(time_to_retrieve_seconds[0])*60 + int(time_to_retrieve_seconds[1])





















