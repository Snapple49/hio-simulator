import threading
import requests
import json
import time
import os
from harmonicIO.stream_connector import stream_connector
            

class EventType:
    HOST = "host_req"
    STREAM = "stream_req"


def read_cfg_json():
    path = "./config.json"
    with open(path, 'r') as cfg:
        try:
            ret = json.load(cfg)
        except json.decoder.JSONDecodeError as e:
            ret = None
            print("Could not read config file: \n{}".format(e.msg))
    return ret

def data_collector(dict_storage, logger, cfg):
    """
    Gather data from master node

    Every 5 seconds, send a request to master and get the available metadata,
    pair it with the current time in seconds and add the pair to provided dict
    """

    url = "http://{}:{}/messagesQuery?token=None&command=verbose".format(cfg.get('master_ip'), cfg.get('master_port'))
    start = int(time.time())
    while start + cfg.get('duration') > int(time.time()):
        time.sleep(cfg.get('polling_interval'))
        resp = requests.get(url)
        if resp.status_code == 200:
            dict_storage[int(time.time()) - start] = resp.text
            logger.log_event("Got data from master!")


def event_manager(events, duration):
    start = int(time.time())
    while duration > int(time.time()) - start:
        for event in list(events):
            if event.start_time > int(time.time()) - start:
                # if it's a host request, just send request
                if event.type == EventType.HOST:
                    print("Sending a host request!")
                    event.send_host_request()
                
                # if stream request, do it once if not periodic else start periodic thread
                elif event.type == EventType.STREAM:
                    if not event.periodic:
                        print("Sending a stream request!")
                        event.send_stream_request()
                    else:
                        periodic_event_thread = threading.Thread(target=periodic_thread, args=(event))
                        periodic_event_thread.start()

                # event has been handled, remove from list
                event.logger.log_event("Event processed: --- {} | {} | {} | {} ---".format(event.container, event.start_time, event.periodic, event.type))
                events.remove(event)

def periodic_thread(event):
    start = int(time.time())
    while start + event.lifetime > int(time.clock):
        print("Sending a stream request!")
        event.send_stream_request()
        time.sleep(event.frequency)



class Event:
    def __init__(self, params, cfg, logger):
        self.type = params.get('type')
        self.periodic = params.get('periodic')
        self.start_time = params.get('time')
        self.container = params.get('c_name')
        self.volatile = params.get('volatile')
        self.num = params.get('num_req')
        self.frequency = params.get('frequency')

        self.ip = cfg.get('master_ip')
        self.port = cfg.get('master_port')
        self.logger = logger
        self.lifetime = cfg.get('duration') - self.start_time
        
    def send_host_request(self):
        url = "http://{}:{}/jobRequest?token=None&type=new_job".format(self.ip, self.port) 
        req_data = json.dumps({"c_name" : self.container, "num" : self.num, "volatile" : self.volatile})
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
        sc = stream_connector.StreamConnector(self.ip, self.port , token="None", std_idle_time=1, source_name="demo_program")

        for _ in range(self.num):
            if sc.is_master_alive():
                data = self.read_data_from_file("./sample_image.bmp")
                data_to_send = sc.get_data_container()
                data_to_send += data
                sc.send_data(self.container, "ubuntu", data_to_send)

class Logger:
    def __init__(self):
        self.timestamp = time.strftime("%Y-%m-%d__%H_%M", time.localtime())
        self.logname = "{}_simulator_log".format(self.timestamp)
        self.starttime = time.time()
        self.i = 1
        while self.logname in os.listdir('.'):
            self.logname += "({})".format(self.i)
            self.i+=1
            
        self.logname += ".log"

        logfile = open(self.logname, 'a')
        logfile.close()

    def log_event(self, message):
        try:
            logfile = open(self.logname, 'a')
            logfile.write(time.strftime("%H:%M:%S - ", time.localtime()) + " --- " + message + '\n')
            logfile.close()
        except:
            print("Could not write to log!")

class Simulator:

    def __init__(self):
        self.events = []
        self.system_output = {}
        self.logger = Logger()
        
        self.sim_config = read_cfg_json()
        self.create_events(self.sim_config.get('events'))
        self.duration = self.sim_config.get('duration')
        
        # init data collection thread
        self.data_col_thread = threading.Thread(target=data_collector, args=(self.system_output, self.logger, self.sim_config))
        self.data_col_thread.daemon = True
    
        # init event thread
        self.event_thread = threading.Thread(target=event_manager, args=(self.events, self.duration))
        self.event_thread.daemon = True

    def create_events(self, cfg_events):
        for item in cfg_events:
            self.events.append(Event(item, self.sim_config, self.logger))
        
    def start_sim(self):
        # read events from file
        print("Created events!")
        for item in self.events:
            print("Event created: --- {} | {} | {} | {} ---".format(item.container, item.start_time, item.periodic, item.type))

        # start simulation threads
        starting_time = int(time.time())
        self.logger.log_event("Starting simulation!")
        self.data_col_thread.start()
        self.event_thread.start()
        time.sleep(self.duration)

        # wait on threads to finish
        self.data_col_thread.join()
        self.event_thread.join()
        
        # write output of master verbose to json file
        self.logger.log_event("Simulation finished, time elapsed: {} seconds".format(int(time.time()) - starting_time))
        with open("{}_simulator_output.json".format(self.logger.timestamp) , 'w') as output:
            json.dump(self.system_output, output)

def run_simulation():
    sim = Simulator()
    sim.start_sim()

if __name__ == "__main__":
    run_simulation()    

