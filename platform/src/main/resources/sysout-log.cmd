#
# usage: sysout-log.cmd
# description: writes logs to System.out
#

create /sys/out com.core.infrastructure.io.SysoutChannel
create /vm/log/channel com.core.infrastructure.log.ChannelLogSink @/sys/out
/vm/log/logSink @vm/log/channel
