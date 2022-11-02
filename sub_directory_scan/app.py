import pika, sys, os
from time import sleep 
from utilities import primary_tools as pt



def main():
    
    print('init_connect')
    
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbit'))
    
    channel = connection.channel()

    channel.queue_declare(queue='subdir')

    def callback(ch, method, properties, body):
        
        print(pt.get_file_object_from_dir(body))
        
        print(f"[x] Received {body}")

    channel.basic_consume(queue='subdir', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    sleep(60)
    print('receiver')
    main()