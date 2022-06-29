import socket
import subprocess
import time

from wakeonlan import send_magic_packet
import ipaddress
import macaddress
import logging
import logging.handlers
from datetime import datetime
import pickle
import os



debug = False

logger = logging.getLogger("Rotating Log")
logger.setLevel(logging.NOTSET)
handler = logging.handlers.RotatingFileHandler('log.txt', maxBytes=200000000,
                                               backupCount=2)
logger.addHandler(handler)

sec1 = 1
sec5 = sec1 * 5
min1 = sec1 * 60
min6 = min1 * 6
min10 = min1 * 10
hour1 = min1 * 60
day1 = hour1 * 24
day10 = day1 * 10
day20 = day10 * 2
day30 = day10 * 3
week1 = day1 * 7

pickle_file = "data.pickle"

nas_backups = []


class Command:
    backup_normal = "backup_normal"
    no_backup_wol = "start_ohne_backup"
    backup_now = "backup_jetzt"
    no_command = "tuh_nichts"
    no_backup_wol_no_normal = "starte_ohne_backup_und_setzt_auf_no_command"


comm = Command()


class Nas:
    ip = None
    mac = None
    hostname = None
    last_backup_timestamp = None
    port = None
    online = None
    command = comm.no_command
    backup_day = None
    brother_nas = None
    block_backup = False
    netcat_port = None
    ssh_port = None

    def __init__(self, hostname, ip, mac, port, backup_day, netcat_port, ssh_port):
        self.ip = ipaddress.IPv4Address(ip)
        self.mac = macaddress.MAC(mac)
        self.hostname = hostname
        self.port = port
        self.backup_day = backup_day
        self.netcat_port = netcat_port
        self.ssh_port = ssh_port

    def __str__(self):
        out = ""
        out = out + ("HOSTNAME: " + str(self.hostname))
        out = out + ("IP: " + str(self.ip))
        out = out + ("MAC: " + str(self.mac))
        out = out + ("LAST BACKUP TIME: " + str(self.last_backup_timestamp))
        out = out + ("ONLINE: " + str(self.online))
        out = out + ("--------------------------------------")
        return out

    def print(self):
        print("HOSTNAME: " + str(self.hostname))
        print("IP: " + str(self.ip))
        print("MAC: " + str(self.mac))
        print("LAST BACKUP TIME: " + str(self.last_backup_timestamp))
        print("ONLINE: " + str(self.online))
        print("--------------------------------------")


def init():
    try:
        global nas_backups

        nas_backups = []

        wol_port = 9

        _TBF8A = Nas('TBF_BACKUP_8_A', '192.168.0.221', '00:11:32:EA:13:21', wol_port, 1, 2211, 221)
        _TBF8B = Nas('TBF_BACKUP_8_B', '192.168.0.222', '00:11:32:F4:F8:EB', wol_port, 1, 2222, 222)
        _TBF16A = Nas('TBF_BACKUP_16_A', '192.168.0.223', '00:11:32:EA:0B:6B', wol_port, 16, 2233, 223)
        _TBF16B = Nas('TBF_BACKUP_16_B', '192.168.0.224', '00:11:32:FF:B3:5F', wol_port, 16, 2244, 224)

        _TBF8A.command = comm.backup_now
        _TBF16A.command = comm.backup_now

        # TODO: DEBUG REMOVE FOR PRODUCTION
        global debug
        if debug:
            print("debug="+str(True))
            # _TBF8A = Nas('TBF_BACKUP_8_A', '192.168.0.140', '00:11:32:EA:13:21', wol_port, 1, 2211, 221)
            #_TBF8A.last_backup_timestamp = datetime(2022, 2, 1)
            #_TBF8A.command = comm.no_backup_wol_no_normal
            #_TBF8B.command = comm.no_backup_wol_no_normal
            #_TBF16A.command = comm.no_backup_wol_no_normal
            #_TBF16B.command = comm.no_backup_wol_no_normal

        # TODO: DEBUG REMOVE FOR PRODUCTION

        _TBF8A.brother_nas = _TBF8B
        _TBF8B.brother_nas = _TBF8A
        _TBF16A.brother_nas = _TBF16B
        _TBF16B.brother_nas = _TBF16A

        nas_backups.append(_TBF8A)
        nas_backups.append(_TBF8B)
        nas_backups.append(_TBF16A)
        nas_backups.append(_TBF16B)



    except Exception as e:
        logger.error(repr(e))


def init_and_pickle():
    try:
        init()
        with open(pickle_file, 'wb') as file_handle:
            pickle.dump(nas_backups, file_handle, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception as e:
        logger.error(repr(e))


def unpickle():
    with open(pickle_file, 'rb') as file_handle:
        unpickled_nas_backups = pickle.load(file_handle)

        for _nas in unpickled_nas_backups:
            assert (isinstance(_nas, Nas))

        return unpickled_nas_backups


def move_pickle_and_repickle(e):
    logger.error(repr(e))
    os.remove(pickle_file)
    init_and_pickle()


def check_if_online(_nas):
    try:
        response = None
        if os.name == 'nt':
            response = os.system("ping -n 1 " + str(_nas.ip) + " | FIND \"TTL\"")
        elif os.name == 'posix':
            response = os.system("ping -c 1 " + str(_nas.ip))
        else:
            raise Exception("Unknown operation system cant execute ping command properly.")

        _nas.online = True if response == 0 else False

        if _nas.online:
            logger.error(_nas)
            return True
        return False
    except Exception as e:
        logger.error(repr(e))
        raise


def backup_normal(_nas):
    try:
        assert (isinstance(_nas, Nas))
        today = datetime.now()

        if not _nas.backup_day == today.day:
            if _nas.last_backup_timestamp:
                delta = today - _nas.last_backup_timestamp
                if not delta.days >= 28:
                    return
            else:
                return

        now = time.time()
        while not check_if_online(_nas):
            send_magic_packets_custom(_nas)
            time.sleep(sec5)
            if time.time() - now > min6:
                # TODO: why not online ?? ERROR
                return

        now = time.time()
        while check_if_online(_nas):
            time.sleep(min10)
            print("Backup of .....")
            print(_nas)
            print("in progress since " + str(today) + ". (Limit: day10 remaining " + str(
                day10 - (time.time() - now)) + ")")
            if time.time() - now > day10:
                # TODO: why so long help help
                return

        # if forgotten_backup_catchup:
        #    _nas.last_backup_timestamp = datetime(today.year,today.month,_nas.backup_day)  # TODO: calculate predefined day of backup as timestamp
        # else:
        _nas.last_backup_timestamp = today

        _nas.command = comm.no_command
        _nas.brother_nas.command = comm.backup_normal

    except Exception as e:
        logger.error(repr(e))


def send_magic_packets_custom(_nas):
    send_magic_packet(str(_nas.mac), ip_address=str(_nas.ip), port=_nas.port)
    splits = str(_nas.ip).split(".")
    splits = splits[:-1]
    splits.append("255")
    ip_b = ".".join(splits)
    send_magic_packet(str(_nas.mac), ip_address=str(ip_b), port=_nas.port)


def no_backup_wol(_nas):
    assert (isinstance(_nas, Nas))

    try:

        now = time.time()
        while not check_if_online(_nas):
            send_magic_packets_custom(_nas)
            time.sleep(sec5)
            if time.time() - now > min6:
                # TODO: why not online ?? ERROR
                return

        buff = 14
        stop_message = "STOP"
        now = time.time()  #
        output = None
        while output is None:
            p1 = subprocess.Popen(("echo " + stop_message).split(), stdout=subprocess.PIPE, shell=True)
            p = subprocess.Popen(("ncat " + str(_nas.ip) + " " + str(_nas.netcat_port)).split(),
                                 stdin=p1.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            output, err = p.communicate()

            time.sleep(sec5)
            if time.time() - now > min6:
                # TODO: why not online ?? ERROR
                raise Exception("ncat on server not starting ? ")

            output = output.decode().strip()
            print(output)
            if output is not None:
                if output != "BACKUP STOPPED":
                    output = None

        if _nas.brother_nas.command != comm.backup_normal:
            _nas.command = comm.backup_normal

    except Exception as e:
        raise


def backup_now(_nas):
    try:
        assert (isinstance(_nas, Nas))
        today = datetime.now()

        now = time.time()
        while not check_if_online(_nas):
            send_magic_packets_custom(_nas)
            time.sleep(sec5)
            if time.time() - now > min10:
                # TODO: why not online ?? ERROR
                break

        now = time.time()
        while check_if_online(_nas):
            time.sleep(min10)
            logger.info("Backup of .....")
            logger.info(_nas)
            logger.info("in progress since " + str(today) + ". (Limit: day10 remaining " + str(
                day10 - (time.time() - now)) + ")")
            if time.time() - now > day10:
                # TODO: why so long help help
                return

        _nas.last_backup_timestamp = today

        _nas.command = comm.no_command
        _nas.brother_nas.command = comm.backup_normal

    except Exception as e:
        logger.error(repr(e))


def no_backup_wol_no_normal(_nas):
    assert (isinstance(_nas, Nas))

    try:

        now = time.time()
        while not check_if_online(_nas):
            send_magic_packets_custom(_nas)
            time.sleep(sec5)
            if time.time() - now > min6:
                # TODO: why not online ?? ERROR
                return

        stop_message = "STOP"
        now = time.time()  #
        output = None
        while output is None:
            p1 = subprocess.Popen(("echo " + stop_message).split(), stdout=subprocess.PIPE, shell=True)
            p = subprocess.Popen(("ncat " + str(_nas.ip) + " " + str(_nas.netcat_port)).split(),
                                 stdin=p1.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            output, err = p.communicate()

            time.sleep(sec5)
            if time.time() - now > min6:
                # TODO: why not online ?? ERROR
                raise Exception("ncat on server not starting ? ")

            output = output.decode().strip()
            print(output)
            if output is not None:
                if output != "BACKUP STOPPED":
                    output = None

        _nas.command = comm.no_command
    except Exception as e:
        raise


if __name__ == '__main__':

    date = datetime.now()

    #calendar_week = date.isocalendar()[1]

    if not os.path.exists(pickle_file):
        init_and_pickle()
    else:
        try:
            nas_backups = unpickle()
        except Exception as e:
            move_pickle_and_repickle(e)

    while True:
        try:
            nas_backups = unpickle()
        except Exception as e:
            move_pickle_and_repickle(e)

        try:
            for nas in nas_backups:

                if not debug:
                    nas.block_backup = True if check_if_online(nas) else False
                    if nas.block_backup:
                        continue

                if nas.command == comm.backup_normal:
                    backup_normal(nas)
                elif nas.command == comm.no_backup_wol:
                    no_backup_wol(nas)
                elif nas.command == comm.backup_now:
                    backup_now(nas)
                elif nas.command == comm.no_backup_wol_no_normal:
                    no_backup_wol_no_normal(nas)
                elif nas.command == comm.no_command:
                    pass

                time.sleep(day1)
        except Exception as e:
            logger.error(repr(e))
