# Failover Configuration Guide

## Tips to Keep in Mind

### Failover Time Profiles
A failover time profile refers to a specific combination of failover parameters that determine the time in which failover should be completed and define the aggressiveness of failover. Some failover parameters include `failover_timeout_sec` and `failover_reader_connect_timeout_sec`. Failover should be completed within 5 minutes by default. If the connection is not re-established during this time, then the failover process times out and fails. Users can configure the failover parameters to adjust the aggressiveness of the failover and fulfill the needs of their specific application. For example, a user could take a more aggressive approach and shorten the time limit on failover to promote a fail-fast approach for an application that does not tolerate database outages. Examples of normal and aggressive failover time profiles are shown below. 
<br><br>
**:warning:Note**: Aggressive failover does come with its side effects. Since the time limit on failover is shorter, it becomes more likely that a problem is caused not by a failure, but rather because of a timeout.
<br><br>
#### Example of the configuration for a normal failover time profile:
| Parameter                                    | Value |
|----------------------------------------------|-------|
| `failover_timeout_sec`                       | `300` |
| `failover_writer_reconnect_interval_sec`     | `2`   |
| `failover_reader_connect_timeout_sec`        | `30`  |
| `failover_cluster_topology_refresh_rate_sec` | `2`   |

#### Example of the configuration for an aggressive failover time profile:
| Parameter                                    | Value |
|----------------------------------------------|-------|
| `failover_timeout_sec`                       | `30`  |
| `failover_writer_reconnect_interval_sec`     | `2`   |
| `failover_reader_connect_timeout_sec`        | `10`  |
| `failover_cluster_topology_refresh_rate_sec` | `2`   |

### Writer Cluster Endpoints After Failover
Connecting to a writer cluster endpoint after failover can result in a faulty connection because there can be a delay before the endpoint is updated to point to the new writer. On the AWS DNS server, this change is usually updated after 15-20 seconds, but the other DNS servers sitting between the application and the AWS DNS server may take longer to update. Using the stale DNS data will most likely cause problems for users, so it is important to keep this in mind.

### 2-Host Clusters
The failover process has limited advantages for a 2-host cluster because there are not as many instances available to replace the instance that has failed. In particular, when a reader instance fails, there are no other readers to fail over to. Instead, Aurora must revive the same instance that has failed. To improve the stability of the cluster, we recommend that your database cluster has at least 3 instances.

### Host Availability
A common misconception about failover is the expectation that only one host will be unavailable during the failover process; this is actually not true. When failover is triggered, all hosts become unavailable for a short time. This is because the control plane, which orchestrates the failover process, first shuts down all hosts, then starts the writer host, and finally starts and connects the remaining hosts to the writer. In short, failover requires each host to be reconfigured and thus, all hosts become unavailable for a short period of time. With this in mind, please note that aggressive failover configurations may cause failover to fail because some hosts may still be unavailable when your failover timeout setting is reached.

### Monitor Failures and Investigate
If you are experiencing difficulties with the failover plugin, try the following:
- Enable [logging](./UsingThePythonWrapper.md#logging) to find the cause of the failure. If it is a timeout, review the [failover time profiles](#failover-time-profiles) section and adjust the timeout values.
- For additional assistance, visit the [getting help page](../../README.md#getting-help-and-opening-issues).

### Retry behavior at the SQLAlchemy / Django pool boundary

When a fresh DBAPI connection is opened inside an SQLAlchemy pool creator
(`SqlAlchemyPooledConnectionProvider`, `AsyncPooledConnectionProvider`) or
the Django MySQL backend, the wrapper's normal plugin chain is bypassed —
the call site sits below the failover plugin in the stack. To keep
post-failover pool refills from failing on Aurora's 15–60 s
connect-rejection window, the wrapper retries transient connect errors
with an exponential backoff.

| Knob                                  | Connection property                   | Default  |
|---------------------------------------|---------------------------------------|----------|
| Max attempts                          | `connection_retry_max_attempts`       | `10`     |
| Initial backoff                       | (constant in `transient_connect.py`)  | `1.0 s`  |
| Multiplier                            | (constant in `transient_connect.py`)  | `1.5×`   |
| Max backoff per attempt               | `connection_retry_max_backoff_s`      | `30 s`   |
| Mean total wait (with equal jitter)   | —                                     | `~79 s`  |
| Worst-case total wait                 | —                                     | `~105 s` |

**Consumer-visible consequence**: during an Aurora failover, a fresh
`engine.connect()` call (or `engine.dispose()` followed by reconnect) can
sit for up to ~100 seconds before raising. This is intentional — without
it the pool refill races Aurora's promoted-writer boot and fails the
user-visible call. If your application has a tighter end-to-end budget
than ~100 s, lower `connection_retry_max_attempts` and/or
`connection_retry_max_backoff_s` via the wrapper's connection properties.
If you run a multi-region cluster with promotion windows exceeding 100 s,
raise them. Set `connection_retry_max_attempts=1` to disable retry
entirely.

The classifier
(`aws_advanced_python_wrapper/utils/transient_connect.py:is_transient_connect_error`)
matches on PG SQLSTATEs `57P01`/`57P02`/`57P03`/`08006`, the SQL-standard
`08` connection-exception class, libpq wire-level error message prefixes,
and MySQL errnos `2001`–`2055`. See the module docstring for the full
per-driver breakdown.

For Django, pass the same knobs through the `OPTIONS` dict in
`DATABASES['default']`:

```python
DATABASES = {
    "default": {
        "ENGINE": "aws_advanced_python_wrapper.django.backends.mysql_connector",
        "OPTIONS": {
            "connection_retry_max_attempts": 6,
            "connection_retry_max_backoff_s": 15.0,
            # ... other wrapper / driver options
        },
    },
}
```
