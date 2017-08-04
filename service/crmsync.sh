#!/usr/bin/env bash

### BEGIN INIT INFO
# Provides:          crmsync
# Required-Start:    $syslog $time $remote_fs
# Required-Stop:     $syslog $time $remote_fs
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Crm syncer
# Description:       Syncs data between Embyon and Crm
### END INIT INFO
#
# Author:       Adam Jakab <adam.jakab@mekit.it>
#

PATH=/bin:/usr/bin:/sbin:/usr/sbin
LOCATION="`pwd`/service"
LAUNCHER="cronlauncher.sh"
PIDFILE=/var/run/crmsync.pid
USERNAME=jack

test -x "${LOCATION}/${LAUNCHER}" || (echo "No LAUNCHER file (${LOCATION}/${LAUNCHER})!" && exit 255)

. /lib/lsb/init-functions

case "$1" in
  start)
        log_daemon_msg "Starting execution of Syncer"
        # --quiet

        /sbin/start-stop-daemon --start --nicelevel 0 --oknodo --background \
            --chuid ${USERNAME} \
            --pidfile ${PIDFILE} --make-pidfile \
            --chdir "${LOCATION}" --exec "${LAUNCHER}"
        log_end_msg $?
    ;;
  stop)
        log_daemon_msg "Stopping execution of Syncer"
        #killproc -p ${PIDFILE} ${DAEMON} --retry=SIGTERM/30/TERM/10/KILL/5
        args="--stop --oknodo --pidfile ${PIDFILE}"
        /sbin/start-stop-daemon ${args}
        log_end_msg $?
    ;;
  force-reload|restart)
    $0 stop
    $0 start
    ;;
  status)
    status_of_proc -p ${PIDFILE} ${LAUNCHER} "crmsync" && exit 0 || exit $?
    ;;
  *)
    echo "Usage: /etc/init.d/crmsync {start|stop|restart|force-reload|status}"
    exit 1
    ;;
esac

exit 0