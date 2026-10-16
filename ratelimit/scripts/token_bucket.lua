-- Token bucket, evaluated atomically by Redis.
-- KEYS[1]  bucket hash key
-- ARGV[1]  refill rate (tokens / second)
-- ARGV[2]  burst (capacity)
-- ARGV[3]  cost (tokens to consume)
-- Returns { allowed (0|1), remaining (int), retry_after_us (int) }
--
-- The clock is Redis' own TIME so that limiter instances with skewed clocks
-- still agree on a single timeline.
local key   = KEYS[1]
local rate  = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local cost  = tonumber(ARGV[3])

local t   = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])

local state  = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts     = tonumber(state[2])
if tokens == nil or ts == nil then
  tokens = burst
  ts = now
end

local elapsed = now - ts
if elapsed > 0 then
  tokens = math.min(burst, tokens + elapsed * rate / 1000000)
  ts = now
end

local allowed = 0
local retry   = 0
if tokens >= cost then
  tokens  = tokens - cost
  allowed = 1
else
  retry = math.ceil((cost - tokens) * 1000000 / rate)
end

redis.call('HSET', key, 'tokens', tostring(tokens), 'ts', tostring(ts))
-- Expire once the bucket would be full again: an absent key == a full bucket.
redis.call('PEXPIRE', key, math.ceil(burst / rate * 1000) + 1000)

return { allowed, math.floor(tokens), retry }
