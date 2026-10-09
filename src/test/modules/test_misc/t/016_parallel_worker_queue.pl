
# Copyright (c) 2026, PostgreSQL Global Development Group

# Tests for parallel_worker_queue_timeout, which lets a parallel leader wait
# a bounded time for parallel worker capacity.
#
# A holder session occupies every parallel worker slot (its workers block in
# a sleeping function), and a second session then tries to launch workers.
# The assertions check observable states (wait events, worker counts, error
# messages, EXPLAIN ANALYZE output), never elapsed durations.  The timeouts
# used only choose which branch runs; the one exception is the
# statement_timeout case, where the session must still be queued when we
# poll for it within 5s (raise that value if a very slow host misses it).

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf(
	'postgresql.conf', qq(
max_worker_processes = 8
max_parallel_workers = 2
max_parallel_workers_per_gather = 2
autovacuum = off
));
$node->start;

$node->safe_psql(
	'postgres', q(
CREATE FUNCTION hold() RETURNS bool LANGUAGE plpgsql PARALLEL SAFE AS
$$BEGIN PERFORM pg_sleep(3600); RETURN true; END$$;
CREATE TABLE t AS SELECT g FROM generate_series(1, 100000) g;
ALTER TABLE t SET (parallel_workers = 2);
ANALYZE t;
CREATE TABLE t2 AS SELECT g FROM generate_series(1, 100000) g;
ALTER TABLE t2 SET (parallel_workers = 1);
ANALYZE t2;
CREATE FUNCTION inner_count() RETURNS bigint LANGUAGE plpgsql
  PARALLEL RESTRICTED AS
$$BEGIN RETURN (SELECT count(*) FROM t2); END$$;
));

# Settings that make the planner choose parallel plans for these tables.
my $force_parallel = q(
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
);

my $explain_analyze =
	'EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) '
  . 'SELECT count(*) FROM t;';

# Check that the holder's plan evaluates hold() in the workers.  If it were
# evaluated in the leader, the leader would block and the workers would
# finish, and every case below would fail confusingly.
my $holder_plan = $node->safe_psql('postgres',
		$force_parallel
	  . 'SET parallel_leader_participation = off;'
	  . 'EXPLAIN (COSTS OFF) SELECT count(*) FROM t WHERE hold();');
like(
	$holder_plan,
	qr/Gather.*Workers Planned: 2.*Parallel Seq Scan on t.*Filter: hold\(\)/s,
	'holder plan evaluates hold() in parallel workers');

# The holder session.  Its own queue timeout lets it wait out any lag in the
# worker accounting after the previous case's workers have stopped, so that
# it always obtains both workers.
my $a = $node->background_psql('postgres', on_error_stop => 0);
$a->query_safe($force_parallel);
$a->query_safe('SET parallel_leader_participation = off;');
$a->query_safe("SET parallel_worker_queue_timeout = '300s';");
my $a_pid = $a->query_safe('SELECT pg_backend_pid();');

my $count_workers = q(SELECT count(*) FROM pg_stat_activity
  WHERE backend_type = 'parallel worker');

# Start the holder query and wait until both parallel workers are running.
sub hold_workers
{
	$a->query_until(qr/holding/,
		"\\echo holding\nSELECT count(*) FROM t WHERE hold();\n");
	ok($node->poll_query_until('postgres', $count_workers, '2'),
		'holder obtained two parallel workers');
}

# Cancel the holder query and wait until it has reported the error.
sub cancel_holder
{
	$node->safe_psql('postgres', "SELECT pg_cancel_backend($a_pid);");
	pump_until($a->{run}, $a->{timeout}, \$a->{stderr},
		qr/canceling statement due to user request/);
	$a->{stdout} = '';
	$a->{stderr} = '';
}

# Wait until no parallel workers of any session are running.
sub wait_no_parallel_workers
{
	ok($node->poll_query_until('postgres', $count_workers, '0'),
		'no parallel workers running');
}

sub release_workers
{
	cancel_holder();
	wait_no_parallel_workers();
}

# Start a fresh session with the given extra settings, and return it along
# with its backend PID.
sub start_session
{
	my ($settings) = @_;
	my $b = $node->background_psql('postgres', on_error_stop => 0);

	$b->query_safe($force_parallel . $settings);
	my $pid = $b->query_safe('SELECT pg_backend_pid();');
	return ($b, $pid);
}

# Wait until the given session is waiting for parallel worker capacity.
sub wait_queued
{
	my ($pid, $msg) = @_;

	ok( $node->poll_query_until(
			'postgres', qq(SELECT count(*) FROM pg_stat_activity
  WHERE wait_event_type = 'IPC' AND wait_event = 'ParallelWorkerQueue'
    AND pid = $pid), '1'),
		$msg);
}

# Case 1: a session queues, and obtains all its workers once the holder
# releases them.
hold_workers();
my ($b, $b_pid) =
  start_session("SET parallel_worker_queue_timeout = '300s';");
$b->query_until(qr/started/,
	"\\echo started\n$explain_analyze\n\\echo done\n");
wait_queued($b_pid, 'session waits with wait event ParallelWorkerQueue');
cancel_holder();
pump_until($b->{run}, $b->{timeout}, \$b->{stdout}, qr/done/);
like(
	$b->{stdout},
	qr/Workers Launched: 2/,
	'queued session launched all workers after capacity was released');
$b->{stdout} = '';
# Wait for both A's and B's workers to go away.
wait_no_parallel_workers();
$b->quit;

# Case 2: with the default of 0, a session degrades immediately.
hold_workers();
($b, $b_pid) = start_session('SET parallel_worker_queue_timeout = 0;');
my $out = $b->query_safe($explain_analyze);
like($out, qr/Workers Planned: 2/, 'timeout 0: two workers planned');
like($out, qr/Workers Launched: 0/, 'timeout 0: no workers launched');
is($node->safe_psql('postgres', $count_workers),
	'2', 'timeout 0: holder still holds its workers, so no wait happened');
$b->quit;

# Case 3: when the timeout expires, the session proceeds without workers and
# without error.  The workers are still held from case 2.
($b, $b_pid) = start_session("SET parallel_worker_queue_timeout = '500ms';");
$out = $b->query($explain_analyze);
like(
	$out,
	qr/Workers Launched: 0/,
	'timeout expiry: proceeds with no workers');
is($b->{stderr}, '', 'timeout expiry: no error or warning');
$b->quit;
release_workers();

# Case 4: a queued session can be cancelled.
hold_workers();
($b, $b_pid) = start_session("SET parallel_worker_queue_timeout = '300s';");
$b->query_until(qr/started/,
	"\\echo started\n$explain_analyze\n\\echo done\n");
wait_queued($b_pid, 'cancel: session is queued');
$node->safe_psql('postgres', "SELECT pg_cancel_backend($b_pid);");
pump_until($b->{run}, $b->{timeout}, \$b->{stderr},
	qr/canceling statement due to user request/);
pass('cancel: queued session was cancelled');
$b->{stdout} = '';
$b->{stderr} = '';
$b->quit;

# Case 5: statement_timeout applies while queued.  The workers are still
# held from case 4.
($b, $b_pid) = start_session(
	"SET parallel_worker_queue_timeout = '300s'; SET statement_timeout = '5s';"
);
$b->query_until(qr/started/,
	"\\echo started\n$explain_analyze\n\\echo done\n");
wait_queued($b_pid, 'statement_timeout: session is queued');
pump_until($b->{run}, $b->{timeout}, \$b->{stderr},
	qr/canceling statement due to statement timeout/);
pass('statement_timeout: queued session timed out');
$b->{stdout} = '';
$b->{stderr} = '';
$b->quit;
release_workers();

# Case 6: a leader that already holds parallel workers through another
# parallel context does not wait for capacity that only it could release.
# inner_count() is evaluated in the leader, in the Aggregate above the outer
# Gather, for the single row with g = 1, and runs a parallel query of its
# own.  The outer worker is still registered at that point: it cannot finish
# its share of t until the leader drains its tuple queue.  (A LIMIT would not
# do here, because the tuple bound is passed down to the worker, which then
# exits after one row.)  With max_parallel_workers = 1 there is no capacity
# left for the inner query, so without that rule it would wait until
# statement_timeout.
my $nested_query =
  'SELECT sum(CASE WHEN g = 1 THEN inner_count() END) FROM t;';
($b, $b_pid) = start_session(
	q(SET max_parallel_workers = 1;
SET max_parallel_workers_per_gather = 1;
SET parallel_worker_queue_timeout = '300s';
SET statement_timeout = '60s';));
my $explain = $b->query_safe("EXPLAIN (COSTS OFF, VERBOSE) $nested_query");
like(
	$explain,
	qr/Aggregate\n\s+Output: sum\(CASE WHEN \(g = 1\) THEN inner_count\(\)[^\n]*\n\s+->  Gather\n\s+Output: g\n\s+Workers Planned: 1\n\s+->  Parallel Seq Scan on public\.t\n/,
	'nested: inner_count() is evaluated in the leader above the Gather');
unlike(
	$explain,
	qr/(Gather|Parallel Seq Scan on public\.t)\n\s+Output:[^\n]*inner_count/,
	'nested: inner_count() is not evaluated in the outer workers');
like(
	$b->query_safe('EXPLAIN (COSTS OFF) SELECT (SELECT count(*) FROM t2);'),
	qr/InitPlan.*Gather.*Workers Planned: 1.*Parallel Seq Scan on t2/s,
	'nested: inner query gets a parallel plan');
is($b->query_safe($nested_query),
	'100000', 'nested: leader holding workers does not wait for itself');
$b->quit;

$a->quit;
$node->stop;

done_testing();
