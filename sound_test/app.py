from utilities import primary_tools as pt

import pika

from time import sleep



def callback(ch, method, properties, body):
    
    print(f"Received from queue {body}")
    
    sleep(30)




def main():
    
    print('SOUND TEST')
    
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbit'))
    
    channel = connection.channel()

    channel.queue_declare(queue='sound_test')


    channel.basic_consume(queue='sound_test', on_message_callback=callback, auto_ack=True)
    

    print(' [*] Waiting for messages. To exit press CTRL+C')
    
    
    channel.start_consuming()
    
    


if __name__ == '__main__':
    
    main()