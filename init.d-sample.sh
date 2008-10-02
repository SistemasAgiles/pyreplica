#!/bin/sh -e

### BEGIN INIT INFO
# Provides:             pyreplica
# Required-Start:       $local_fs $network $time
# Required-Stop:        $local_fs $network $time
# Should-Start:         $syslog $remote_fs
# Should-Stop:          $syslog $remote_fs
# Default-Start:        2 3 4 5
# Default-Stop:         0 1 6
# Short-Description:    Simple PostgreSQL python replicator
### END INIT INFO

# WARNING: very simple approach, no error checking, pidfiles, etc.

PATH=/bin:/usr/bin:/sbin:/usr/sbin
DAEMON=/usr/local/pyreplica/daemon.py
NAME=pyreplica
DESC="postgresql python replicator"
PID_FILE=/var/run/$NAME.pid

test -f $DAEMON || exit 0

set -e

case "$1" in
    start)
        echo -n "Starting $DESC: "
        start-stop-daemon --start --quiet --pidfile $PID_FILE --exec $DAEMON
        echo "$NAME."
    ;;

    stop)
        echo -n "Stopping $DESC: "
        start-stop-daemon --stop --quiet --pidfile $PID_FILE
        echo "$NAME."
    ;;

    restart)
        $0 stop || true
        $0 start
    ;;

    *)
        echo "Usage: $0 {start|stop|restart}"    
    ;;
esac

exit 0
