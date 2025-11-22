# RedisInsight Connection Guide

# redis://redis:6379
# Connect from this 

You are running RedisInsight and Redis Stack via docker-compose.

services:
  redis: redis/redis-stack-server:latest (port 6379 mapped to host)
  redisinsight: redis/redisinsight:latest (port 5540 mapped to host)

## Important: Hostname Inside RedisInsight Container
When you open RedisInsight in your browser at http://localhost:5540, the connection form creates a connection from **inside the RedisInsight container** to the Redis server. Therefore:

- Using `127.0.0.1` or `localhost` in the RedisInsight connection form points to the RedisInsight container itself, not your host.
- You should use the Docker service name `redis` as the host.

## Correct Connection Settings
- Host: `redis`
- Port: `6379`
- Name: (any label, e.g. `Local Redis Stack`)
- Username: (leave empty unless ACL users configured)
- Password: (leave empty unless you set a password)

Do **not** include the URI scheme (no `redis://`). Just host and port.

If you installed a native/desktop version of RedisInsight (not the container) then you would instead use:
- Host: `127.0.0.1`
- Port: `6379`

## Verifying Redis is Reachable
From host PowerShell:
```powershell
docker exec realtimemarketapplication-redis-1 redis-cli PING
```
Expected output:
```
PONG
```
Set & Get test:
```powershell
docker exec realtimemarketapplication-redis-1 redis-cli SET test:key 123
docker exec realtimemarketapplication-redis-1 redis-cli GET test:key
```

## Common Problems
1. Blank or timeout:
   - Using `127.0.0.1` inside the container: switch to `redis`.
2. Authentication error:
   - You added a password locally but not in docker-compose. Remove password or configure `requirepass`.
3. Module errors in UI:
   - Ensure you used `redis/redis-stack-server:latest` image. Check with:
     ```powershell
     docker logs realtimemarketapplication-redis-1 | Select-String timeseries
     ```
4. Firewall / VPN:
   - Rare for local Docker networks; usually not an issue. Try native RedisInsight or `host.docker.internal`.

## Using `host.docker.internal`
If you ever run Redis directly on host (without docker) and RedisInsight in container, use `host.docker.internal` as the hostname.

## Next Steps
Once connected you can:
- Filter keys with `*:price` or `*:volume`.
- Visualize time series data, change aggregation (FIRST, LAST, MAX, MIN, SUM) and bucket size (e.g. 300000 ms).

## NOTE On Your redis_config
The printed message says `decode_responses=False` but you set `decode_responses=True`. You can update the log line to avoid confusion.

Happy charting!

