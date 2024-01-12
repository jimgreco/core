#
# usage: clob-tier0.cmd
# description: demonstration of a clob inside the sequencer
#

source network-local.cmd
source -s sysout-log.cmd
source -s telnet.cmd inet:0.0.0.0:7001
create /bus/schema com.core.clob.schema.ClobSchema

# Primary Sequencer
create /busServer/store com.core.platform.bus.mold.BufferChannelMessageStore
create /busServer com.core.platform.bus.mold.MoldBusServer \
    server @/bus/schema @/busServer/store $event_channel $command_channel $discovery_channel_tier0

create seq01a com.core.platform.applications.sequencer.Sequencer @/busServer SEQ01
create seq01a/handlers com.core.clob.applications.sequencer.ClobCommandHandlers @/busServer
#

/busServer/createSession AA
seq01a/start