#!/usr/bin/env python
import pika
import sys
import time
import os
import sys
import random

def publish_message(queue,msg):
    with pika.BlockingConnection(pika.ConnectionParameters('localhost')) as connection:
        channel = connection.channel()
        channel.queue_declare(queue=queue)
        channel.basic_publish(exchange='',
                    routing_key=queue,
                    body=str(msg))
        print("Published Message!",str(msg))

def main():
    queue = "lab_queue"

    while True:
        randomlist = []
        device_count = random.randint(0,384) ## because the number of devices varies
        for i in range(0,device_count):
            n = random.randint(1,30)
            publish_message(queue,n)
        print("done")
        time.sleep(5)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)