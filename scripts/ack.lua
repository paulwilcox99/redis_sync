-- ack.lua
-- Atomically removes worker from pending_acks and returns remaining count.
--
-- KEYS[1] = sim:pending_acks
-- KEYS[2] = sim:events
-- ARGV[1] = worker_id
-- ARGV[2] = sim_time ISO string
--
-- Returns remaining count. 0 means all workers ACK'd this cycle.
-- No stream write here — controller drives all stream writes.

local removed = redis.call('SREM', KEYS[1], ARGV[1])
if removed == 0 then
  return redis.error_reply('Worker not in pending_acks: ' .. ARGV[1])
end
return redis.call('SCARD', KEYS[1])
