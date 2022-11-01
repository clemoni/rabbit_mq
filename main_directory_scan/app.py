from utilities import primary_tools as pt

import pika

from time import sleep

from datetime import datetime 

from os import path


def init_folder_scan(folder_path, scan_time=3, current_size=None, current_ts=None,):
    current_size = len(pt.get_folder_object_from_dir(folder_path)) if current_size is None else current_size
    current_ts= datetime.now().timestamp() if current_ts is None else current_ts
    
    folder_scan_dict={
        'current_size':current_size,
        'current_ts':current_ts
    }
    
   
    def collect_current_size(folder_path):
            
        return len(pt.get_folder_object_from_dir(folder_path))
    
        
    def collect_new_folder_created():
        folder_collect=pt.get_folder_object_from_dir(folder_path)
            
        return [folder for folder in folder_collect if path.getctime(folder.path) > folder_scan_dict['current_ts']]
        
        
        
    def r_get_earliest_ts_from_new_folder(new_folder_list, current_ts, save_list=None, earliest_ts=None):
        save_list=new_folder_list.copy() if save_list is None else save_list
        earliest_ts=current_ts if earliest_ts is None else earliest_ts
    
        if len(save_list)==0:
            return earliest_ts
        else:
            current_folder=save_list.pop(0)
                
            folder_ctime=path.getctime(current_folder.path)
                
            earliest_ts= folder_ctime if earliest_ts < folder_ctime else earliest_ts
                
            return r_get_earliest_ts_from_new_folder(new_folder_list, current_ts, save_list, earliest_ts)
            
        
        
            
    while True:
                
        print('new_loop')
        
        print('test', pt.get_folder_object_from_dir('/src'))
                
        new_size=collect_current_size(folder_path)
                
        if new_size > folder_scan_dict['current_size']:
            
            connection = pika.BlockingConnection(pika.ConnectionParameters('rabbit'))
                    
            new_folders_list=collect_new_folder_created()
                    
            for new_folder in new_folders_list:
                
                send(new_folder.path, connection)
                    
            folder_scan_dict['current_size']=new_size
                    
            folder_scan_dict['current_ts']=r_get_earliest_ts_from_new_folder(new_folders_list, folder_scan_dict['current_ts'])
            
            connection.close()
                    
        sleep(scan_time)
                
       

        


def send(i, connection):
    
    # connection = pika.BlockingConnection(pika.ConnectionParameters('rabbit'))
    
    channel = connection.channel()

    channel.queue_declare(queue='subfolder')

    channel.basic_publish(exchange='', routing_key='subfolder', body=i)
    
    print(f"{i} sent to queue")
    




if __name__ == '__main__':
    
    print('hello')
    
    print(pt.get_folder_object_from_dir('/src'))
    
    init_folder_scan('/src')
    
        
    
# assuming there's a working local RabbitMQ server with a working # guest/guest account
# def message(topic, message):
    