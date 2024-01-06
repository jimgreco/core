#
# usage: http.cmd <address> (e.g., http.cmd inet:0.0.0.0:8001)
# description: opens the http shell at the specified address
#

set http_address $1
create http com.core.platform.shell.HttpShell
http/open $http_address
