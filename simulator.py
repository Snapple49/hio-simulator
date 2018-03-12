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

    def __init__(self, params, cfg):
        self.periodic = params.get('periodic')
        self.time = params.get('time')
        self.container = params.get('c_name')
        self.type = params.get('type')
        self.ip = cfg.get('master_ip')
        
    def send_host_request(self, num, volatile):
        url = "http://{}:8080/jobRequest?token=None&type=new_job".format(self.ip) 
        req_data = {"c_name" : self.container, "num" : num, "volatile" : volatile}
        resp = requests.post(url, data=req_data)
        return resp.status_code
        
    def send_stream_request(self):
        



class Simulator:
    
    def __init__(self, duration):
        self.duration = duration
        self.events = []
        self.system_output = {}
        
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
            self.events.append(Event(item, self.sim_config))
        
    def start_sim(self):
        self.data_col_thread.start()
        







