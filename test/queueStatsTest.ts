import { expect } from 'vitest'
import * as helper from './testHelper.ts'
import { randomUUID } from 'node:crypto'
import type { ConstructorOptions } from '../src/types.ts'
import { ctx } from './hooks.ts'

describe('queueStats', function () {
  const queue1 = `q${randomUUID().replaceAll('-', '')}`
  const queue2 = `q${randomUUID().replaceAll('-', '')}`

  async function init (config: (ConstructorOptions & { schema: string }) | (Partial<ConstructorOptions> & { testKey?: string; noDefault?: boolean }) | undefined) {
    const boss = await helper.start(config)

    await boss.createQueue(queue1)
    await boss.createQueue(queue2)

    await boss.send(queue1)
    await boss.send(queue1)
    await boss.send(queue2)
    await boss.send(queue2)

    return boss
  }

  it('should get accurate stats', async function () {
    ctx.boss = await init(ctx.bossConfig)
    const queueData = await ctx.boss.getQueueStats(queue1)
    expect(queueData).not.toBe(undefined)

    const {
      name,
      deferredCount,
      queuedCount,
      activeCount,
      totalCount
    } = queueData!

    expect(name).toBe(queue1)
    expect(deferredCount).toBe(0)
    expect(queuedCount).toBe(2)
    expect(activeCount).toBe(0)
    expect(totalCount).toBe(2)
  })

  it('should get accurate stats on an empty queue', async function () {
    ctx.boss = await init(ctx.bossConfig)
    const queue3 = randomUUID()
    await ctx.boss.createQueue(queue3)

    const queueData = await ctx.boss.getQueueStats(queue3)
    expect(queueData).not.toBe(undefined)

    const {
      name,
      deferredCount,
      queuedCount,
      activeCount,
      totalCount
    } = queueData

    expect(name).toBe(queue3)
    expect(deferredCount).toBe(0)
    expect(queuedCount).toBe(0)
    expect(activeCount).toBe(0)
    expect(totalCount).toBe(0)
  })

  it('should return priorityCounts when includePriorityCounts is set', async function () {
    ctx.boss = await init(ctx.bossConfig)

    await ctx.boss.send(queue1, null, { priority: 10 })
    await ctx.boss.send(queue1, null, { priority: 5 })
    await ctx.boss.send(queue1, null, { priority: 5 })

    const queueData = await ctx.boss.getQueueStats(queue1, { includePriorityCounts: true })

    expect(queueData.priorityCounts).toBeDefined()
    expect(queueData.priorityCounts!.length).toBe(3)

    const [first, second, third] = queueData.priorityCounts!
    expect(first.priority).toBe(10)
    expect(first.queuedCount).toBe(1)
    expect(second.priority).toBe(5)
    expect(second.queuedCount).toBe(2)
    expect(third.priority).toBe(0)
    expect(third.queuedCount).toBe(2)
  })

  it('should return empty priorityCounts array for empty queue when includePriorityCounts is set', async function () {
    ctx.boss = await helper.start(ctx.bossConfig)
    const queue = randomUUID()
    await ctx.boss.createQueue(queue)

    const queueData = await ctx.boss.getQueueStats(queue, { includePriorityCounts: true })

    expect(queueData.priorityCounts).toBeDefined()
    expect(queueData.priorityCounts!.length).toBe(0)
  })

  it('should not include priorityCounts by default', async function () {
    ctx.boss = await init(ctx.bossConfig)

    const queueData = await ctx.boss.getQueueStats(queue1)

    expect(queueData.priorityCounts).toBeUndefined()
  })

  it('should cache priorityCounts when trackPriorityStats is enabled', async function () {
    ctx.boss = await helper.start({ ...ctx.bossConfig, monitorIntervalSeconds: 1, queueCacheIntervalSeconds: 1 })
    const queue = randomUUID()
    await ctx.boss.createQueue(queue, { trackPriorityStats: true })

    await ctx.boss.send(queue, null, { priority: 10 })
    await ctx.boss.send(queue, null, { priority: 0 })
    await ctx.boss.send(queue, null, { priority: 0 })

    await ctx.boss.supervise(queue)

    const queueData = await ctx.boss.getQueue(queue)

    expect(queueData!.priorityCounts).toBeDefined()
    expect(queueData!.priorityCounts!.length).toBe(2)

    const [first, second] = queueData!.priorityCounts!
    expect(first.priority).toBe(10)
    expect(first.queuedCount).toBe(1)
    expect(second.priority).toBe(0)
    expect(second.queuedCount).toBe(2)
  })

  it('should not cache priorityCounts when trackPriorityStats is disabled', async function () {
    ctx.boss = await helper.start({ ...ctx.bossConfig, monitorIntervalSeconds: 1, queueCacheIntervalSeconds: 1 })
    const queue = randomUUID()
    await ctx.boss.createQueue(queue)

    await ctx.boss.send(queue, null, { priority: 10 })

    await ctx.boss.supervise(queue)

    const queueData = await ctx.boss.getQueue(queue)

    expect(queueData!.priorityCounts).toBeNull()
  })

  it('should properly get queue stats when all jobs are deleted', async function () {
    ctx.boss = await helper.start({ ...ctx.bossConfig, monitorIntervalSeconds: 1, queueCacheIntervalSeconds: 1 })

    const queue4 = randomUUID()
    await ctx.boss.createQueue(queue4)

    await ctx.boss.send(queue4)
    await ctx.boss.send(queue4)
    await ctx.boss.send(queue4)

    await ctx.boss.supervise(queue4)

    await ctx.boss.deleteAllJobs(queue4)

    await ctx.boss.supervise(queue4)

    // wait for a second for queueCache to update
    await new Promise(resolve => setTimeout(resolve, 1000))

    const queueData = await ctx.boss.getQueueStats(queue4)
    expect(queueData).toBeTruthy()

    expect(queueData.deferredCount).toBe(0)
    expect(queueData.queuedCount).toBe(0)
    expect(queueData.activeCount).toBe(0)
    expect(queueData.totalCount).toBe(0)
  })
})
