-- advance_clock.lua
-- Atomically advances clock, finds due workers, rebuilds ACK set,
-- writes to stream. Called by controller only.
--
-- KEYS[1] = sim:clock:time
-- KEYS[2] = sim:clock:status
-- KEYS[3] = sim:schedule
-- KEYS[4] = sim:pending_acks
-- KEYS[5] = sim:events
--
-- ARGV[1] = new_sim_time ISO string
-- ARGV[2] = new_sim_time Unix timestamp (float string)
-- ARGV[3] = sim_end Unix timestamp (float string)
-- ARGV[4] = event_type  ("SIMULATION_START" or "CLOCK_ADVANCE")
--
-- Returns:
--   "DONE"      if simulation end reached
--   <number>    count of workers due this cycle (0 means no one due)

-- Advance clock
redis.call('SET', KEYS[1], ARGV[1])

-- Check if simulation is over
if tonumber(ARGV[2]) >= tonumber(ARGV[3]) then
  redis.call('SET', KEYS[2], 'DONE')
  redis.call('XADD', KEYS[5], '*',
    'type', 'SIMULATION_DONE',
    'sim_time', ARGV[1])
  return 'DONE'
end

-- Find all workers due at or before new sim_time
local due = redis.call('ZRANGEBYSCORE', KEYS[3], 0, ARGV[2])

-- Rebuild pending_acks for this cycle
redis.call('DEL', KEYS[4])
if #due > 0 then
  redis.call('SADD', KEYS[4], unpack(due))
end

-- Write event to stream
redis.call('XADD', KEYS[5], '*',
  'type', ARGV[4],
  'sim_time', ARGV[1])

return #due
-- Returns number of workers due this cycle.
-- 0 means no one due at this time — controller must advance again.
