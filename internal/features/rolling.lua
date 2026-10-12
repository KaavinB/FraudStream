-- Four standalone Redis keys: customer history/meta and merchant history/meta.
-- SNAPSHOT reads prior history. APPLY materializes a previously committed SQL intent.
-- Equal timestamps include previously applied events. Regressions are quarantined.
local mode = ARGV[1]
local t = tonumber(ARGV[2])
local amount = tonumber(ARGV[3])
local customer = ARGV[4]
local merchant = ARGV[5]
local id = ARGV[6]
local clast = tonumber(redis.call('HGET', KEYS[2], 'last'))
local mlast = tonumber(redis.call('HGET', KEYS[4], 'last'))
if redis.call('ZCOUNT', KEYS[1], t-86400, t) > 50000 or redis.call('ZCOUNT', KEYS[3], t-3600, t) > 50000 then
    return redis.error_reply('CAPACITY_EXCEEDED')
end
if (clast and t < clast) or (mlast and t < mlast) then
    return redis.error_reply('LATE_EVENT')
end
if mode == 'APPLY' then
    local cmember = cjson.encode({id, amount, merchant})
    local mmember = cjson.encode({id, amount, customer})
    redis.call('ZADD', KEYS[1], t, cmember)
    redis.call('ZADD', KEYS[3], t, mmember)
    redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', '(' .. (t-86400))
    redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', '(' .. (t-3600))
    redis.call('HSET', KEYS[2], 'last', t)
    redis.call('HSET', KEYS[4], 'last', t)
    return 'OK'
end
local c = redis.call('ZRANGEBYSCORE', KEYS[1], t-86400, t, 'WITHSCORES')
local m = redis.call('ZRANGEBYSCORE', KEYS[3], t-3600, t)
local count5, count1, sum1, sum24 = 0,0,0,0
local merchants, customers = {},{}
for i=1,#c,2 do
    local member = cjson.decode(c[i])
    local stamp = tonumber(c[i+1])
    sum24 = sum24 + member[2]
    merchants[member[3]] = true
    if stamp >= t-3600 then count1=count1+1; sum1=sum1+member[2] end
    if stamp >= t-300 then count5=count5+1 end
end
local msum = 0
for _,value in ipairs(m) do
    local member = cjson.decode(value)
    msum=msum+member[2]; customers[member[3]]=true
end
local dc, dm = 0,0
for _ in pairs(customers) do dc=dc+1 end
for _ in pairs(merchants) do dm=dm+1 end
local n = #c/2
local mean = 0
if n>0 then mean=sum24/n end
local ratio = 0
if mean>0 then ratio=amount/mean end
local previous = 86400
if clast then previous=t-clast end
local mmean = 0
if #m>0 then mmean=msum/#m end
return cjson.encode({amount,tonumber(ARGV[7]),tonumber(ARGV[8]),count5,count1,n,sum1,sum24,mean,ratio,previous,dm,#m,mmean,dc})
