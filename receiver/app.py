import pika, sys, os
from time import sleep 


def main():
    
    print('init_connect')
    
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbit'))
    
    channel = connection.channel()

    channel.queue_declare(queue='hello')

    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body)

    channel.basic_consume(queue='hello', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    sleep(60)
    print('receiver')
    main()