set event_channel inet:239.100.100.100:10100:lo0
set command_channel inet:239.100.100.101:10101:lo0
set discovery_channel_tier0 inet:239.100.100.102:10102:lo0
set discovery_channel_tier1 inet:239.100.100.102:10103:lo0
set discovery_channel $discovery_channel_tier1
