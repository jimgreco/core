#
# usage: cli-shell.cmd
# description: Creates a shell instance that reads from System.in
#

create /sys/in com.core.infrastructure.io.SysinChannel
/vm/shell/open @sys/in @sys/out true false null null
