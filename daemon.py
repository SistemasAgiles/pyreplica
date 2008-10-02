#!/usr/bin/env python

#based on http://homepage.hispeed.ch/py430/python/index.html

# configure these paths:
LOGFILE = '/var/log/pyreplica.log'
PIDFILE = '/var/run/pyreplica.pid'
CONFPATH = '/etc/pyreplica'
DEBUG = 3	# 1: normal, 2: verbose
UID = 103   # set group first "pydaemon"
GID = 103   # set user "pydaemon"

import sys, os, time, signal
import threading
from ConfigParser import SafeConfigParser
from email.MIMEText import MIMEText
from smtplib import SMTP

if sys.platform=="win32": # just for testing
    os.fork = lambda:0
    os.setsid = lambda:1
    os.setegid = os.seteuid =lambda x:1
    signal.pause = lambda:time.sleep(30)
    LOGFILE = None # stdout
    CONFPATH = 'c:\\pyreplica'

import pyreplica

class Log:
    """file like for writes with auto flush after each write
    to ensure that everything is logged, even during an
    unexpected exit."""
    def __init__(self, f):
        self.f = f
    def write(self, s):
        self.f.write(s)
        self.f.flush()

class Replicator(threading.Thread):
    def __init__(self,config_file):
        threading.Thread.__init__(self)
        # simple signal
        self.killed = False
        configdict = SafeConfigParser()
        configdict.read(config_file)
        self.name = configdict.get('MAIN','NAME')
        # Database's connections
        self.dsn0 = configdict.get('MAIN','DSN0')
        self.dsn1 = configdict.get('MAIN','DSN1')
        # Email notification
        self.smtp_server = configdict.get('SMTP','SERVER')
        self.username = configdict.get('SMTP','USERNAME')
        self.password = configdict.get('SMTP','PASSWORD')
        self.start_subject = configdict.get('SMTP','START_SUBJECT')
        self.stop_subject = configdict.get('SMTP','STOP_SUBJECT')
        self.error_subject = configdict.get('SMTP','ERROR_SUBJECT')
        self.from_addr = configdict.get('SMTP','FROM_ADDR')
        self.to_addrs = configdict.get('SMTP','TO_ADDRS').split(";")

    def run(self):
        while not self.killed:
            self.send_mail(self.start_subject,"")
            try:
                # start replication main loop
                pyreplica.main_loop(self.dsn0,self.dsn1,lambda: self.killed,self.debug)
            except SystemExit:
                break
            except Exception,e:
                self.send_mail(self.error_subject,str(e))
            # wait 60 seconds if it fails
            time.sleep(60)
        self.send_mail(self.stop_subject,"")
        
    def send_mail(self, subject, body):
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = self.from_addr
        msg['Reply-to'] = self.from_addr
        msg['To'] = "; ".join(self.to_addrs)
        try:
            self.debug("Sending mail: %s" % subject,2)
            s = SMTP(self.smtp_server)
            s.sendmail(self.from_addr, self.to_addrs, msg.as_string())
        except Exception,e:
            self.debug("Exception while sending mail: %s" % str(e))

    def debug(self,message,level=1):
        "Print a debug message"
        if DEBUG>=level:
            print self.name,time.asctime(),message
            # flush buffers
            sys.stdout.flush()       

    def stop(self):
        self.killed = True
        
if __name__ == "__main__":
    # do the UNIX double-fork magic, see (ISBN 0201563177)
    try:
        pid = os.fork()
        if pid > 0:
            # exit first parent
            sys.exit(0)
    except OSError, e:
        print >>sys.stderr, "fork #1 failed: %d (%s)" % (e.errno, e.strerror)
        sys.exit(1)

    # decouple from parent environment
    os.chdir("/")   #don't prevent unmounting....
    os.setsid()
    os.umask(0)

    # do second fork
    try:
        pid = os.fork()
        if pid > 0:
            # exit from second parent, print eventual PID before
            #print "Daemon PID %d" % pid
            open(PIDFILE,'w').write("%d"%pid)
            sys.exit(0)
    except OSError, e:
        print >>sys.stderr, "fork #2 failed: %d (%s)" % (e.errno, e.strerror)
        sys.exit(1)

    #redirect outputs to a logfile
    if LOGFILE:
        sys.stdout = sys.stderr = Log(open(LOGFILE, 'a+'))
    #ensure the that the daemon runs a normal user
    os.setegid(UID)
    os.seteuid(GID)

    # start replication threads
    config_files = [f for f in os.listdir(CONFPATH) if f.endswith(".conf")]
    threads = []

    for config_file in config_files:
        thread = Replicator(os.path.join(CONFPATH,config_file))
        threads.append(thread)
        thread.start()

    signal.signal(signal.SIGTERM,lambda *args: 1)
    # wait for any signal
    signal.pause()
    # "kill" pending thread
    for thread in threads:
        thread.stop()
        # wait until it terminates
        thread.join()
