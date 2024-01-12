#
# usage: clob.cmd
# description: demonstration of a clob inside the sequencer
#

source network-local.cmd
source -s sysout-log.cmd
source -s telnet.cmd inet:0.0.0.0:7001
create /bus/schema com.core.clob.schema.ClobSchema
create /bus com.core.platform.bus.mold.MoldBusClient \
    client @/bus/schema $event_channel $command_channel $discovery_channel

# Primary Sequencer
create /busServer/store com.core.platform.bus.mold.BufferChannelMessageStore
create /busServer com.core.platform.bus.mold.MoldBusServer \
    server @/bus @/bus/schema @/busServer/store $event_channel $command_channel $discovery_channel

create seq01a com.core.platform.applications.sequencer.Sequencer @/busServer SEQ01
create seq01a/handlers com.core.clob.applications.sequencer.ClobCommandHandlers @/busServer
#

# Backup Sequencer
create /busServer2/store com.core.platform.bus.mold.BufferChannelMessageStore
create /busServer2 com.core.platform.bus.mold.MoldBusServer \
    server2 @/bus @/bus/schema @/busServer/store $event_channel $command_channel $discovery_channel

create seq02a com.core.platform.applications.sequencer.Sequencer @/busServer2 SEQ02
create seq02a/handlers com.core.clob.applications.sequencer.ClobCommandHandlers @/busServer2
#

# Reference Data Publisher
create ref01a com.core.platform.applications.refdata.ReferenceDataPublisher @/bus REF01 equityDefinition
ref01a/setPath clob/src/main/resources
ref01a/start
ref01a/loadFiles
#

# Printer
create print01a com.core.clob.applications.printer.ClobPrinter @/bus .
#

# Injector
create inject01a com.core.clob.applications.utilities.ClobInjector @/bus INJ01
inject01a/start
#

/busServer/createSession AA
seq01a/start