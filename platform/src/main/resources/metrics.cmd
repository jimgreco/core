#
# usage: metrics.cmd vm01 inet:239.100.100.103:10103:lo0
# description: creates a publisher for metrics
#

set metrics_log_file $1

create /vm/metrics/publisher com.core.infrastructure.metrics.LogMetricPublisher $metrics_log_file
/vm/metrics/publisher/start
