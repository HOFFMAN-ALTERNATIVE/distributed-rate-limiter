-- Sliding window log, evaluated atomically by Redis.
-- KEYS[1]  sorted-set key (score = event time in microseconds)
-- ARGV[1]  limit (max events per window)
-- ARGV[2]  window length in microseconds
-- ARGV[3]  cost (events to record)
-- ARGV[4]  unique member prefix for this call
-- Returns { allowed (0|1), remaining (int), retry_after_us (int) }
local key    = KEYS[1]
local limit  = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local cost   = tonumber(ARGV[3])
local prefix = ARGV[4]

local t   = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])

redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
local count = redis.call('ZCARD', key)

if count + cost <= limit then
  for i = 1, cost do
    redis.call('ZADD', key, now, prefix .. ':' .. i)
  end
  redis.call('PEXPIRE', key, math.ceil(window / 1000))
  return { 1, limit - count - cost, 0 }
end

-- Denied: capacity frees up when the (count + cost - limit)-th oldest event
-- leaves the window.
local idx    = count + cost - limit - 1
local oldest = redis.call('ZRANGE', key, idx, idx, 'WITHSCORES')
local retry  = 0
if oldest[2] then
  retry = math.max(0, tonumber(oldest[2]) + window - now)
end
return { 0, limit - count, retry }
