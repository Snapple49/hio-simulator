import threading
import requests
import json
import time
import os
from harmonicIO.stream_connector import stream_connector
            



def read_cfg_json():
    path = "./config.json"
    with open(path, 'r') as cfg:
        try:
            ret = json.loads(cfg.read())
        except json.decoder.JSONDecodeError as e:
            ret = None
            print("Could not read config file: \n{}".format(e.msg))
    return ret

def data_collector(dict_storage, logger):
    """
    Gather data from master node

    Every 5 seconds, send a request to master and get the available metadata,
    pair it with the current time in seconds and add the pair to provided dict
    """
    url = "http://130.238.28.48:8080/messagesQuery?token=None&command=verbose"
    time.clock()
    while True:
        time.sleep(5)
        resp = requests.get(url)
        if resp.status_code == 200:
            dict_storage[int(time.clock())] = resp.text
            logger.log_event("Got data from master!")

def event_thread(events):
    time.clock()
    while True:
        time.sleep(1)
        for event in events:
            if event.time < time.clock():
                pass



class Event:

    def __init__(self, params, cfg, logger):
        self.periodic = params.get('periodic')
        self.time = params.get('time')
        self.container = params.get('c_name')
        self.type = params.get('type')
        self.ip = cfg.get('master_ip')
        self.port = cfg.get('master_port')
        
    def send_host_request(self, num, volatile):
        url = "http://{}:8080/jobRequest?token=None&type=new_job".format(self.ip) 
        req_data = {"c_name" : self.container, "num" : num, "volatile" : volatile}
        resp = requests.post(url, data=req_data)
        return resp.status_code

    def read_data_from_file(self, path):
        func_data = bytearray()

        with open(path, 'rb') as f:
            lines = f.readlines()

            for line in lines:
                func_data += line

        return func_data

    def send_stream_request(self):
        sc = stream_connector.StreamConnector(self.ip, self.port , token="None", std_idle_time=1)

        
        if sc.is_master_alive():
            data = self.read_data_from_file("./sample_image.bmp")
            data_to_send = sc.get_data_container()
            data_to_send += data

            sc.send_data(self.container, "ubuntu", data_to_send)
        
        


class Logger:
    def __init__(self):
        self.logname = "simulator_log_{}".format(time.strftime("%Y-%m-%d__%H_%M", time.localtime()))
        self.starttime = time.clock()
        self.i = 1
        while self.logname in os.listdir():
            self.logname += "({})".format(self.i)
            self.i+=1
            
        self.logname += ".log"

        logfile = open(self.logname, 'x')
        logfile.close()

    def log_event(self, message):
        try:
            logfile = open(self.logname, 'a')
            logfile.write(time.strftime("%H:%M:%S - ", time.localtime()) + " --- " + message + '\n')
            logfile.close()
        except:
            print("Could not write to log!")

class Simulator:

    def __init__(self, duration):
        self.duration = duration
        self.events = []
        self.system_output = {}
        self.logger = Logger()
        
        self.sim_config = read_cfg_json()
        self.create_events(self.sim_config.get('events'))
        
        # init data collection thread
        self.data_col_thread = threading.Thread(target=data_collector, args=self.system_output)
        self.data_col_thread.daemon = True
    
        # init event thread
        self.event_thread = threading.Thread()
        self.event_thread.daemon = True

    def create_events(self, cfg_events):
        for item in cfg_events:
            self.events.append(Event(item, self.sim_config, self.logger))
        
    def start_sim(self):
        starting_time = time.clock()
        self.logger.log_event("Starting simulation!")
        self.data_col_thread.start()
        time.sleep(self.duration)
        self.logger.log_event("Simulation finished, time elapsed: {} seconds".format(time.clock() - starting_time))

        







