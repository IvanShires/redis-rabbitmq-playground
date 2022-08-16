#!/usr/bin/env python
import pika
import time
import sys
import requests
import concurrent.futures
import os
import random
import redis

redis_strings = ['object_1','object_2','object_3','object_4','object_5','object_6']
redis_statuses = ['OK','Pending','Unknown']

ttl_minutes = 60
ttl = 60 * ttl_minutes ## 60 seconds, times 60 mins


def process_redis(redis_key,status):

    r = redis.Redis(
        host= 'localhost',
        port= '6379')

    redis_key = str(redis_key) 

    if (r.exists(redis_key)):
        value = str(r.get(str(redis_key)))
        if ("pending" not in value.lower()):
            print(str(redis_key),"exists - set value",status)
            r.set(str(redis_key), status)
        else:
            print(str(redis_key),"is pending")
    else:
        print(str(redis_key),"not exists - set value",status)
        if ("pending" in status.lower()):
            r.setex(str(redis_key), ttl, status)  
        else:
            r.set(str(redis_key), status)
    
def callback(ch, method, properties, body):
    n = random.randint(25,60) ## to simulate some devices coming up quickly, some not
    time.sleep(n) ## simulates a connection to a device
    string = random.choice(redis_strings)
    status = random.choice(redis_statuses)
    redis_key = str(string)+"-"+str(body)
    print(redis_key)
    process_redis(redis_key,status)

def consumer():
    with pika.BlockingConnection(pika.URLParameters("amqp://guest:guest@localhost/")) as connection:
        channel = connection.channel()
        channel.queue_declare(queue='lab_queue')

        channel.basic_consume(queue='lab_queue',
                            auto_ack=True,
                            on_message_callback=callback)
        
        print(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()

def main():
    # create a thread pool with 10 worker threads
    max_workers = 192
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for i in range(0,max_workers):
            future = executor.submit(consumer)



if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)