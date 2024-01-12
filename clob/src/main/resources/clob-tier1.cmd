#
# usage: clob-tier0.cmd
# description: demonstration of a clob inside the sequencer
#

source network-local.cmd
source -s sysout-log.cmd
source -s telnet.cmd inet:0.0.0.0:7002
create /bus/schema com.core.clob.schema.ClobSchema
create /bus com.core.platform.bus.mold.MoldBusClient \
    client @/bus/schema $event_channel $command_channel $discovery_channel_tier0

# Rewinder
create rewind01a com.core.platform.bus.mold.MoldRepeater REWIND01 @/bus $discovery_channel_tier1
rewind01a/start
#

/bus/start
