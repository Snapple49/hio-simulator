import socket
from concurrent.futures import ProcessPoolExecutor
from harmonicIO.general.services import SysOut


class MessagingConfiguration(object):
    __queue_threshold = 4
    __max_in_memory_msg = 500

    @staticmethod
    def get_queue_threshold():
        return MessagingConfiguration.__queue_threshold

    @staticmethod
    def get_max_in_memory_msg():
        return MessagingConfiguration.__max_in_memory_msg


class MessagesQueue(object):
    __msg_queue = dict()
    __pool = ProcessPoolExecutor()

    @staticmethod
    def push_to_queue(image_name, item):
        if not isinstance(item, bytearray):
            raise Exception("Invalid implementation! requires byte array but got something else.")

        if image_name in MessagesQueue.__msg_queue:
            MessagesQueue.__msg_queue[image_name].append(item)
        else:
            MessagesQueue.__msg_queue[image_name] = [item]

        MessagesQueue.__check_for_scale()

    @staticmethod
    def get_queues_length(image_name):
        if image_name in MessagesQueue.__msg_queue:
            return len(MessagesQueue.__msg_queue[image_name])

        return None

    @staticmethod
    def get_queues_all():
        ret = {}
        for key, value in MessagesQueue.__msg_queue.items():
            ret[key] = len(value)

        return ret

    @staticmethod
    def pop_queue(image_name, index=0):
        if image_name in MessagesQueue.__msg_queue:
            if len(MessagesQueue.__msg_queue[image_name]) > 0:
                return MessagesQueue.__msg_queue[image_name].pop(index)

        return None

    @staticmethod
    def is_queue_available(image_name):
        if image_name in MessagesQueue.__msg_queue:
            if len(MessagesQueue.__msg_queue[image_name]) < MessagingConfiguration.get_max_in_memory_msg():
                return True

        return False

    @staticmethod
    def __check_for_scale():
        tmp = "MSGs "
        for key, value in MessagesQueue.__msg_queue.items():
            tmp += "({0} -> {1}) ".format(key, len(value))

        SysOut.debug_string(tmp)

    @staticmethod
    def verbose():
        ret = dict()
        for key, value in MessagesQueue.__msg_queue.items():
            ret[key] = len(value)

        return ret

    @staticmethod
    async def stream_to_batch(c_addr, c_port, image_name):

        data = MessagesQueue.pop_queue(image_name)

        def __push_stream_end_point(c_addr, c_port, data):
            # Create a client socket to connect to server

            s = None
            for res in socket.getaddrinfo(c_addr, c_port, socket.AF_UNSPEC, socket.SOCK_STREAM):
                af, socktype, proto, canonname, sa = res
                try:
                    s = socket.socket(af, socktype, proto)
                except OSError as msg:
                    print('error creating client socket')
                    print(msg)
                    s = None
                    continue
                try:
                    s.connect(sa)
                except OSError as msg:
                    print('error connecting client socket')
                    print(msg)
                    s.close()
                    s = None
                    continue
                break
            if s is None:
                print('could not open socket')
                return False

            with s:
                s.sendall(data)
                s.sendall(b'')
                s.close()

            return True

        MessagesQueue.__pool.map(__push_stream_end_point(c_addr, c_port, data))
        # while not __push_stream_end_point(c_target, data): pass
