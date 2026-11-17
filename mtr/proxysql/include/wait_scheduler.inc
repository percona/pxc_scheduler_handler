# Wait for the ProxySQL scheduler to converge to an expected state.
#
# Set before sourcing:
#   $wait_condition — SQL that returns a non-empty row when converged
#   $wait_timeout   — optional, seconds to wait (default 90)
#
# The query is executed against the current connection (must be proxysql_admin).

--disable_query_log
--disable_result_log
if (!$wait_timeout)
{
  --let $wait_timeout = 90
}
--source include/wait_condition.inc
--enable_result_log
--enable_query_log
