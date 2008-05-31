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
DAEMON=/usr/local/pyreplica/pyreplica.sh
CONFIG_FILES=`ls /etc/pyreplica/*.conf`

case "$1" in
    start)
	echo -n "Starting postgresql python replicator: pyreplica "
	start-stop-daemon --start --name pyreplica.sh --exec $DAEMON -- $CONFIG_FILES
	echo "."
    ;;

    stop)
	echo -n "Stopping postgresql python replicator: pyreplica"
	start-stop-daemon --name pyreplica.sh  --stop
	start-stop-daemon --name pyreplica.py  --stop
	echo "."
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
