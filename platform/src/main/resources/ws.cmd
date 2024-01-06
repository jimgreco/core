#
# usage: http.cmd <address> (e.g., http.cmd inet:0.0.0.0:8001)
# description: opens the http shell at the specified address
#

set ws_address $1
create ws com.core.platform.shell.WebSocketShell
ws/open $ws_address
