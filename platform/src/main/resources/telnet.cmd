#
# usage: telnet.cmd <address> (e.g., telnet.cmd inet:0.0.0.0:7001)
# description: opens the telnet shell at the specified address
#

set telnet_address $1
create telnet com.core.platform.shell.TelnetShell
telnet/open $telnet_address
