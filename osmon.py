#!/usr/bin/python
# -*- coding: utf-8
#
#sudo pip install python-daemon
#sudo pip install lockfile
#sudo pip install py-zabbix
#sudo pip install paho-mqtt

import daemon, signal
from daemon import pidfile
import threading
import logging
import logging.handlers
import ConfigParser
import argparse
import os, sys, time, json
import psutil
import socket, platform
import paho.mqtt.publish as mqtt_publish

from _daemon import CDaemon

PROG=os.path.basename(sys.argv[0]).rstrip('.py')
PROG_DESC='os monitoring daemon'

CONS_FORMAT = "%(asctime)s:%(levelname)s:%(name)s:%(message)s"
SYSLOG_FORMAT = "%(levelname)s:%(name)s:%(message)s"        

DEFAULT_CONFIG={
    'name':PROG,
    'timer_cpu': 60,
    'timer_mem': 300,
}

def namedtuple_asdict(t):
    return({str(type(t).__name__) : dict(t._asdict())})

def prep_data(obj):
    root_data = namedtuple_asdict(obj)
    new_data = {}
    for root_key in root_data.keys():
        for (key, val) in root_data[root_key].items():
            new_data[root_key + '_' + key] = val
    return new_data

def timestamp_python_to_java(ts):
    return(long(ts * 1000))

def sys_info():
    disks = []
    info = {
        'hostname': socket.gethostname(),
        'fqdn': socket.getfqdn(),
        'machine': platform.machine(),
        'node': platform.node(),
        'platform': platform.platform(),
        'processor': platform.processor(),
        'system': platform.system(),
        'release': platform.release(),
        'version': platform.version(),
        'cpu_count': psutil.cpu_count(),
        'physical_cpu_count': psutil.cpu_count(logical=False),
        'virtual_memory': psutil.virtual_memory().total,
        'swap_memory': psutil.swap_memory().total,
    }
    if platform.system()=='Linux':
        (distname,version,id) = platform.linux_distribution()
        info['distribution']=distname
        info['distribution_version']=version
        info['distribution_id']=id
    # for disk in psutil.disk_partitions():
    #     d = dict(disk._asdict())
    #     d['size'] = psutil.disk_usage(disk.mountpoint).total
    #     disks.append(d)
    # info['disks'] = disks
    return(info)

def top_process_list():
    procs =[]
    for proc in psutil.process_iter():
        cpu = proc.cpu_percent()
        name = proc.name()
        mem = proc.memory_percent()
        procs.append({'name': proc.name(), 'cpu':proc.cpu_percent(), 'mem':proc.memory_percent()})
    procs = sorted(procs, key=lambda k: k['cpu'], reverse=True)
    mems = sorted(procs, key=lambda k: k['mem'], reverse=True)
    return ( { 'top_cpu': procs[0:3], 'top_mem': mems[0:3] } )

def top_process():
    procs =[]
    max_cpu = 0
    max_mem = 0
    max_cpu_name = ''
    max_mem_name = ''
    for proc in psutil.process_iter():
        cpu = proc.cpu_percent()
        mem = proc.memory_percent()
        if cpu > max_cpu:
            max_cpu = cpu
            max_cpu_name = '{0}% {1}[{2}]: {3}'.format(round(cpu,1) ,proc.name(), proc.pid, ' '.join(proc.cmdline()))

        if mem > max_mem:
            max_mem = mem
            max_mem_name = '{0}% {1}[{2}]: {3}'.format(round(mem,1) ,proc.name(), proc.pid, ' '.join(proc.cmdline()))
    return ( { 'top_cpu': max_cpu_name, 'top_mem': max_mem_name } )

class OSMON(CDaemon):
    """docstring for OSMON"""

    data_payload = {}
    thingsboard = None
    thingsboard_telemetry = 'v1/devices/me/telemetry'
    thingsboard_attributes = 'v1/devices/me/attributes'
    thingsboard_accesstoken = ''
    zabbix = None

    def on_start(self):
        self._time_cpu = time.time() - self.get_cfg('timer_cpu')
        self._time_mem = time.time() - self.get_cfg('timer_mem')
        if self.get_cfg('zabbix'):
            zb = self.get_cfg('zabbix')
            self.zabbix = zb.get('host')
            self.zabbix_name = zb.get('name') if zb.get('name') else 'SDM230'
            self.log.info('send messages to zabbix: ' + self.zabbix)
        if self.get_cfg('thingsboard'):
            tb = self.get_cfg('thingsboard')
            self.thingsboard = tb.get('host')
            if tb.get('telemetry'): self.thingsboard_telemetry = tb.get('telemetry') 
            if tb.get('attributes'): self.thingsboard_attributes = tb.get('attributes')
            if tb.get('accesstoken'): self.thingsboard_accesstoken = tb.get('accesstoken')
            self.log.info('send messages to thingsboard: ' + self.thingsboard)
            self.send_thingsboard_sysinfo()

    def on_stop(self):
        pass

    def on_run(self):
        self.timer_cpu()
        self.timer_mem()
        self.push_data()

    def timer_cpu(self):
        if time.time() - self._time_cpu > self.get_cfg('timer_cpu'):
            self._time_cpu=time.time()
            self.send(prep_data(psutil.cpu_times_percent(interval=None, percpu=False)))
            self.send(top_process())

    def timer_mem(self):
        if time.time() - self._time_mem > self.get_cfg('timer_mem'):
            self._time_mem=time.time()
            self.send(prep_data(psutil.virtual_memory()))
            self.send(prep_data(psutil.swap_memory()))

    def send(self, data):
        self.data_payload.update(data)

    def send_thingsboard(self):
        self.log.debug(self.data_payload)
        try:
            mqtt_publish.single(
                self.thingsboard_telemetry,
                payload=json.dumps(self.data_payload),
                hostname=self.thingsboard,
                auth={'username':self.thingsboard_accesstoken, 'password':""}
            )
        except Exception as err:
            self.log.error('Publish thingsboard: ' + str(err))

    def send_thingsboard_sysinfo(self):
        # print json.dumps(sys_info(), indent=2)
        try:
            mqtt_publish.single(
                self.thingsboard_attributes,
                payload=json.dumps(sys_info()),
                hostname=self.thingsboard,
                auth={'username':self.thingsboard_accesstoken, 'password':""}
            )
        except Exception as err:
            self.log.error('Publish thingsboard: ' + str(err))

    def push_data(self):
        if len(self.data_payload):
            # print json.dumps(self.data_payload, indent=2)
            if self.thingsboard:
                self.send_thingsboard()
            if self.zabbix:
                pass
                # self.send_zabbix()
            self.data_payload = {}

def run_program(foreground=False):
    if foreground:
        print "Start", PROG
    os_monitor = OSMON(DEFAULT_CONFIG)
    try:
        while True:
            time.sleep(.5)
    except KeyboardInterrupt:
        os_monitor.close()
        if foreground:
            print "Exit", PROG
            sys.exit()
    except:
        os_monitor.close()
        time.sleep(.5)

def start_daemon(pidf, logf):
    fh = logging.FileHandler(logf)
    with daemon.DaemonContext(
        working_directory='/tmp',
        umask=0o002,
        pidfile=pidfile.TimeoutPIDLockFile(pidf),
        stderr =  fh.stream,
        stdout =  fh.stream,
        ) as context:
            run_program()

def stop_daemon(pidf):
    if os.path.isfile(pidf):
        pid = int(open(pidf).read())
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError as e:
            os.remove(pidf)
        except Exception as e:
            raise
        else:
            pass
        finally:
            pass

def load_config(fname):
    if os.path.isfile(fname):
        config = ConfigParser.ConfigParser(allow_no_value=True)
        try:
            config.readfp(open(fname))
            for section in config.sections():
                for (name, value) in config.items(section):
                    if not DEFAULT_CONFIG.get(section): DEFAULT_CONFIG[section]={} 
                    DEFAULT_CONFIG[section][name] = value.strip("'\"")        
        except ConfigParser.MissingSectionHeaderError as e:
            print e
        except Exception as e:
            print e
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=PROG_DESC)
    parser.add_argument('-f', '--foreground', action='store_true', help='Run '+ PROG +' in foreground')
    parser.add_argument('-s', '--start', action='store_true', help="Start daemon")
    parser.add_argument('-t', '--stop', action='store_true', help="Stop daemon")
    parser.add_argument('-r', '--restart', action='store_true', help="Restart daemon")
    parser.add_argument('-d', '--debug', action='store_true', help="Start in debug mode")
    parser.add_argument('-p', '--pid-file', default='/tmp/'+ PROG +'.pid')
    parser.add_argument('-l', '--log-err', default='/tmp/'+ PROG +'.err')
    parser.add_argument('-c', '--config', default='/etc/'+ PROG +'.conf')
    parser.add_argument('-v', '--verbose', action='store_true', help="Verbose output")

    args = parser.parse_args()

    if args.debug:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO

    if args.foreground:
        logging.basicConfig(level=loglevel, format=CONS_FORMAT)

    logger = logging.getLogger()
    logger.setLevel(loglevel)

    if args.config: load_config(args.config)

    if args.verbose: print args.config, json.dumps(DEFAULT_CONFIG, indent=2)

    if args.start:
        start_daemon(pidf=args.pid_file, logf=args.log_err )
    elif args.stop:
        stop_daemon(pidf=args.pid_file)
    elif args.restart:
        stop_daemon(pidf=args.pid_file)
        time.sleep(1)
        start_daemon(pidf=args.pid_file, logf=args.log_err)
    elif args.foreground:
        run_program(foreground=True)
    else:
        parser.print_help()

    # print json.dumps(top_process(),indent=2)
