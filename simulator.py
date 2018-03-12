import threading
import requests
import json
import time

            

def read_cfg_json():
    path = "./config.json"
    with open(path, 'r') as cfg:
        try:
            ret = json.loads(cfg.read())
        except json.decoder.JSONDecodeError as e:
            ret = None
            print("Could not read config file: \n{}".format(e.msg))
    return ret

def data_collector(dict_storage):
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

def event_thread(events):
    time.clock()
    while True:
        time.sleep(1)
        for event in events:
            if event.time < time.clock():
                pass

                

class Event:

    def __init__(self, params):
        self.periodic = params.get('periodic')
        self.time = params.get('time')
        self.container = params.get('c_name')
        self.type = params.get('type')
        

class Simulator:
    
    def __init__(self, duration):
        self.duration = duration
        self.events = []
        self.system_output = {}
        
        self.sim_config = read_cfg_json()
        self.create_events(sim_config.get('events'))
        
        # init data collection thread
        self.data_col_thread = threading.Thread(target=data_collector, args=self.system_output)
        self.data_col_thread.daemon = True
    
        # init event thread
        self.event_thread = threading.Thread()
        self.event_thread.daemon = True

    def create_events(self, cfg_events):
        for item in cfg_events:
            events.append(Event(item))
        
    def start_sim(self):
        self.data_col_thread.start()
        







