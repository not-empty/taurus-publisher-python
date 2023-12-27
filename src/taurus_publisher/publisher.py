from src.helpers.redis.connection import RedisConnection
from redis.asyncio import Redis
from typing import Optional
import json
import ulid
import time
import os


class Publisher:
    prefix = "bull"
    redis: Optional[Redis]

    def __init__(self, redis: Optional[Redis] = None):
        self.redis = redis

    async def get_connection(self) -> Redis:
        if self.redis is None:
            self.redis = await RedisConnection().create_connection(
                os.getenv("REDIS_HOST", 'localhost'),
                int(os.getenv("REDIS_PORT", '6379')),
            )

        return self.redis

    async def add_job(
        self,
        queue: str,
        data: str,
        opts: dict = {},
        name: str = "process",
    ) -> str:
        id = str(ulid.new())
        redis = await self.get_connection()
        key_prefix = f"{self.prefix}:{queue}:"
        opts = self.get_configs(opts)
        priority = opts.get("priority", 0)
        lifo = opts.get("lifo", "LPUSH")

        return await redis.eval(
            self.get_add_script(),
            6,
            key_prefix + "wait",
            key_prefix + "paused",
            key_prefix + "meta-paused",
            key_prefix + "id",
            key_prefix + "delayed",
            key_prefix + "priority",
            key_prefix,
            opts["jobId"],
            name,
            data,
            json.dumps(opts),
            opts["timestamp"],
            opts["delay"],
            opts["delay"] + opts["timestamp"],
            priority,
            lifo,
            id,
        )

    def get_configs(self, opts: dict) -> dict:
        id = str(ulid.new())

        return {
            **{
                "attempts": 3,
                "backoff": 30000,
                "delay": 0,
                "removeOnComplete": 100,
                "jobId": id,
                "timestamp": int(time.time() * 1000),
            },
            **opts,
        }

    def get_add_script(self) -> str:
        return """
            local jobId
            local jobIdKey
            local rcall = redis.call

            local jobCounter = rcall("INCR", KEYS[4])

            if ARGV[2] == "" then
                jobId = jobCounter
                jobIdKey = ARGV[1] .. jobId
            else
                jobId = ARGV[2]
                jobIdKey = ARGV[1] .. jobId
                if rcall("EXISTS", jobIdKey) == 1 then
                    return jobId .. "" -- convert to string
                end
            end

            -- Store the job.
            rcall(
                "HMSET",
                jobIdKey,
                "name",
                ARGV[3],
                "data",
                ARGV[4],
                "opts",
                ARGV[5],
                "timestamp",
                ARGV[6],
                "delay",
                ARGV[7],
                "priority",
                ARGV[9]
            )

            -- Check if job is delayed
            local delayedTimestamp = tonumber(ARGV[8])
            if(delayedTimestamp ~= 0) then
                local timestamp = delayedTimestamp * 0x1000 + bit.band(jobCounter, 0xfff)
                rcall("ZADD", KEYS[5], timestamp, jobId)
                rcall("PUBLISH", KEYS[5], delayedTimestamp)
            else
                local target

                -- Whe check for the meta-paused key to decide if we are paused or not
                -- (since an empty list and !EXISTS are not really the same)
                local paused
                if rcall("EXISTS", KEYS[3]) ~= 1 then
                    target = KEYS[1]
                    paused = false
                else
                    target = KEYS[2]
                    paused = true
                end

                -- Standard or priority add
                local priority = tonumber(ARGV[9])
                if priority == 0 then
                    -- LIFO or FIFO
                    rcall(ARGV[10], target, jobId)

                    -- Emit waiting event (wait..ing@token)
                    rcall("PUBLISH", KEYS[1] .. "ing@" .. ARGV[11], jobId)
                else
                    -- Priority add
                    rcall("ZADD", KEYS[6], priority, jobId)
                    local count = rcall("ZCOUNT", KEYS[6], 0, priority)

                    local len = rcall("LLEN", target)
                    local id = rcall("LINDEX", target, len - (count-1))
                    if id then
                    rcall("LINSERT", target, "BEFORE", id, jobId)
                    else
                    rcall("RPUSH", target, jobId)
                    end

                end
            end

            return jobId .. "" -- convert to string"""
