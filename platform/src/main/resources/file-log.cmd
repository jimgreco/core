#
# usage: file-log.cmd vm.log
# description: writes logs to a file
#
# params
# 1: the file name
#

default append_log_file true
set log_file $1
create /log/rolling com.core.infrastructure.log.RollingLogFile $log_file $append_log_file
# create /log/file com.core.infrastructure.io.FileChannel $log_file WRITE CREATE APPEND
create /vm/log/channel com.core.infrastructure.log.ChannelLogSink @/log/rolling/file
/vm/log/logSink @vm/log/channel
