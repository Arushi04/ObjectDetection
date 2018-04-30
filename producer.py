import time
import os
import sys
import cv2


from kafka import SimpleProducer, KafkaClient
#  connect to Kafka
kafka = KafkaClient('localhost:9092')
producer = SimpleProducer(kafka)
# Assign a topic
topic = 'video'
video = cv2.VideoCapture(0)

while True:

    i = 0
    # read the file
    while (video.isOpened):
        # read the image in each frame
        success, image = video.read()
        # check if the file has read to the end
        if not success:
            if i == 0:
                print("ERROR: can't read video file!")            
                sys.exit()
            else:
                break;
            
        # convert the image png
        ret, jpeg = cv2.imencode('.png', image)
        # Convert the image to bytes and send to kafka
        producer.send_messages(topic, jpeg.tobytes())
        # To reduce CPU usage create sleep time of 0.2sec  
        time.sleep(0.2)
        if (i % 100):
            print("frame = " + str(i * 100))
        i = i + 1
       
    # clear the capture
    video.release()
    print('done emitting')



