#!/usr/bin/env python

#based on http://homepage.hispeed.ch/py430/python/index.html

# configure these paths:
LOGFILE = '/var/log/pyreplica.log'
PIDFILE = '/var/run/pyreplica.pid'
CONFPATH = '/etc/pyreplica'
DEBUG_LEVEL = 2	#  default debug level: 1: normal, 2: verbose
UID = 103   # set group "pydaemon"
GID = 103   # set user "pydaemon"

import sys, os, time, signal, StringIO, threading, traceback
from ConfigParser import SafeConfigParser
from email.MIMEText import MIMEText
from smtplib import SMTP

if sys.platform=="win32": # just for testing
    # functions not available on windows
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
    def flush(self):
        self.f.flush()

class Replicator(threading.Thread):
    def __init__(self,config_file):
        threading.Thread.__init__(self)
        # Set default values
        self.killed = False
        # Parse configuration:
        configdict = SafeConfigParser()
        configdict.read(config_file)
        main_conf = dict([(k.upper(),v) for k,v in configdict.items('MAIN')])
        self.name = main_conf['NAME']
        # Database's connections
        self.dsn0 = main_conf['DSN0']
        self.dsn1 = main_conf['DSN1']
        self.skip_user = main_conf.get('SKIP_USER',None)
        self.keepalive = main_conf.get('KEEPALIVE', "False").lower() == 'true'
        self.slave_field = main_conf.get('SLAVE_FIELD', pyreplica.FIELD)
        self.debug_level = int(main_conf.get('DEBUG_LEVEL',DEBUG_LEVEL))
        # Email notification
        if configdict.has_section('SMTP'):
            self.smtp_conf = dict([(k.upper(),v) for k,v in configdict.items('SMTP')])
        else:
            self.smtp_conf = {}

    def run(self):
        while not self.killed:
            self.send_mail(self.smtp_conf.get('START_SUBJECT', ""),"")
            try:
                # start replication main loop
                pyreplica.main_loop(self.dsn0, self.dsn1,
                    is_killed = lambda: self.killed, 
                    skip_user = self.skip_user, 
                    slave_field = self.slave_field,
                    keepalive = self.keepalive, 
                    debug = self.debug)
            except SystemExit:
                break
            except Exception,e:
                # convert the exception to a string and send it via email
                output = StringIO.StringIO()
                traceback.print_exc(file=output)
                self.send_mail(self.smtp_conf.get('ERROR_SUBJECT', ""),output.getvalue())
            # wait 60 seconds if it fails
            time.sleep(60)
        self.send_mail(self.smtp_conf.get('STOP_SUBJECT', ""),"")
        
    def send_mail(self, subject, body):
        if not self.smtp_conf:
            return # smtp not configured
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = self.smtp_conf['FROM_ADDR']
        msg['Reply-to'] = self.smtp_conf['FROM_ADDR']
        msg['To'] = self.smtp_conf['TO_ADDRS']
        try:
            self.debug("Sending mail: %s" % subject,2)
            s = SMTP(self.smtp_conf['SERVER'])
            if 'USERNAME' in self.smtp_conf and 'PASSWORD' in self.smtp_conf:
                s.ehlo()
                s.login(self.smtp_conf['USERNAME'],self.smtp_conf['PASSWORD'])
            s.sendmail(msg['From'], msg['To'], msg.as_string())
        except Exception,e:
            self.debug("Exception while sending mail: %s" % str(e))

    def debug(self,message,level=1):
        "Print a debug message"
        if self.debug_level>=level:
            print self.name,time.asctime(),message
            # flush buffers
            sys.stdout.flush()
        if level==0: # conflict, send mail
            self.send_mail(self.smtp_conf.get('WARNING_SUBJECT', ""),message)

    def stop(self):
        "Set a flag to kill this thread"
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
    print "Got a signal, killing threads..."
    # "kill" pending thread
    for thread in threads:
        thread.stop()
    
    for thread in threads:
        # wait until it terminates
        thread.join()
    print "Threads killed ok"
