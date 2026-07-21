// Compare the groupConcurrency fetch plans discussed in #842:
//
//   1. original - MATERIALIZED active counts joined before and after LIMIT
//   2. array    - uncorrelated saturated-group ARRAY InitPlan, then a post-LIMIT count join
//   3. map      - the current production query from plans.fetchNextJob()
//
// Every measurement gets an isolated real pg-boss schema and executes the full UPDATE via
// EXPLAIN ANALYZE. The fixture is dropped afterward unless KEEP=1 is set.
//
// Run one core pass:
//   npx tsx examples/group-concurrency-perf/index.ts
//
// Reproduce the five-run medians reported on PR #845:
//   RUNS=5 npx tsx examples/group-concurrency-perf/index.ts
//
// Run the slower 10k/50k active-group probes:
//   MODE=large npx tsx examples/group-concurrency-perf/index.ts
//
// Select individual scenarios:
//   SCENARIOS=incident_stale,batch_1000 npx tsx examples/group-concurrency-perf/index.ts

import Db from '../../src/db.ts'
import { PgBoss } from '../../src/index.ts'
import * as plans from '../../src/plans.ts'
import * as helper from '../../test/testHelper.ts'

const TABLE = 'job_common'
const DEFAULT_GROUP_LIMIT = 2
const RUNS = Number(process.env.RUNS ?? 1)
const MODE = process.env.MODE ?? 'core'
const KEEP = process.env.KEEP === '1'
const SELECTED_SCENARIOS = new Set(
  (process.env.SCENARIOS ?? '').split(',').map(value => value.trim()).filter(Boolean)
)
const JOB_COLUMNS_MIN = 'id, name, data, expire_seconds as "expireInSeconds", heartbeat_seconds as "heartbeatSeconds", group_id as "groupId", group_tier as "groupTier"'

interface Scenario {
  id: string
  saturatedGroups: number
  otherActiveGroups: number
  pendingCount: number
  eligibleCount: number
  batchSize: number
  tierCount: number
  refreshStats: boolean
}

interface Query {
  text: string
  values: unknown[]
}

interface PlanNode {
  'Node Type': string
  'CTE Name'?: string
  'Actual Rows'?: number
  'Actual Loops'?: number
  'Relation Name'?: string
  'Subplan Name'?: string
  Plans?: PlanNode[]
}

interface ExplainReport {
  Plan: PlanNode
  'Execution Time': number
  'Planning Time': number
}

interface Result {
  scenario: string
  variant: string
  executionMs: number
  planningMs: number
  claimed: number
  queryChars: number
  activeCteMaxLoops: number
  rankingMaxLoops: number
  jobTableMaxLoops: number
  initPlans: number
  executedInitPlans: number
}

interface Variant {
  id: 'original' | 'array' | 'map'
  build: (schema: string, scenario: Scenario) => Query
}

const coreScenarios: Scenario[] = [
  { id: 'incident_stale', saturatedGroups: 1, otherActiveGroups: 114, pendingCount: 12_000, eligibleCount: 36, batchSize: 1, tierCount: 0, refreshStats: false },
  { id: 'incident_current', saturatedGroups: 1, otherActiveGroups: 114, pendingCount: 12_000, eligibleCount: 36, batchSize: 1, tierCount: 0, refreshStats: true },
  { id: 'saturated_500', saturatedGroups: 500, otherActiveGroups: 0, pendingCount: 12_000, eligibleCount: 36, batchSize: 1, tierCount: 0, refreshStats: false },
  { id: 'saturated_3000', saturatedGroups: 3_000, otherActiveGroups: 0, pendingCount: 12_000, eligibleCount: 36, batchSize: 1, tierCount: 0, refreshStats: false },
  { id: 'batch_100', saturatedGroups: 1, otherActiveGroups: 114, pendingCount: 12_000, eligibleCount: 100, batchSize: 100, tierCount: 0, refreshStats: false },
  { id: 'batch_1000', saturatedGroups: 1, otherActiveGroups: 114, pendingCount: 12_000, eligibleCount: 1_000, batchSize: 1_000, tierCount: 0, refreshStats: false },
  { id: 'tiers_1', saturatedGroups: 1, otherActiveGroups: 0, pendingCount: 12_000, eligibleCount: 36, batchSize: 1, tierCount: 1, refreshStats: false },
  { id: 'tiers_100', saturatedGroups: 100, otherActiveGroups: 0, pendingCount: 12_000, eligibleCount: 36, batchSize: 1, tierCount: 100, refreshStats: false },
  { id: 'tiers_250', saturatedGroups: 250, otherActiveGroups: 0, pendingCount: 12_000, eligibleCount: 36, batchSize: 1, tierCount: 250, refreshStats: false }
]

const largeScenarios: Scenario[] = [
  { id: 'saturated_10000', saturatedGroups: 10_000, otherActiveGroups: 0, pendingCount: 12_000, eligibleCount: 36, batchSize: 1, tierCount: 0, refreshStats: false },
  { id: 'saturated_50000', saturatedGroups: 50_000, otherActiveGroups: 0, pendingCount: 12_000, eligibleCount: 36, batchSize: 1, tierCount: 0, refreshStats: false }
]

const variants: Variant[] = [
  { id: 'original', build: buildOriginalQuery },
  { id: 'array', build: buildArrayQuery },
  { id: 'map', build: buildMapQuery }
]

async function main (): Promise<void> {
  validateOptions()
  await helper.init()

  const availableScenarios = MODE === 'large'
    ? largeScenarios
    : MODE === 'all'
      ? [...coreScenarios, ...largeScenarios]
      : coreScenarios
  const scenarios = SELECTED_SCENARIOS.size > 0
    ? availableScenarios.filter(scenario => SELECTED_SCENARIOS.has(scenario.id))
    : availableScenarios

  if (scenarios.length === 0) {
    throw new Error(`No scenarios selected. Available: ${availableScenarios.map(scenario => scenario.id).join(', ')}`)
  }

  const versionDb = new Db(helper.getConfig())
  await versionDb.open()
  const version = await versionDb.executeSql('SHOW server_version')
  await versionDb.close()

  console.log(`PostgreSQL ${version.rows[0].server_version}`)
  console.log(`mode=${MODE} runs=${RUNS} scenarios=${scenarios.map(scenario => scenario.id).join(',')}`)

  const results: Result[] = []
  let sequence = 0
  for (let run = 1; run <= RUNS; run++) {
    for (const scenario of scenarios) {
      // Rotate order to avoid consistently giving one implementation the warmest cache.
      const offset = sequence % variants.length
      const orderedVariants = [...variants.slice(offset), ...variants.slice(0, offset)]
      for (const variant of orderedVariants) {
        const result = await runBenchmark(scenario, variant, run, sequence)
        results.push(result)
        console.log(JSON.stringify({ run, ...result }))
        sequence++
      }
    }
  }

  printTimingSummary(results, scenarios)
  printPlanSummary(results, scenarios)
}

function validateOptions (): void {
  if (!Number.isSafeInteger(RUNS) || RUNS < 1) {
    throw new Error('RUNS must be a positive integer')
  }
  if (!['core', 'large', 'all'].includes(MODE)) {
    throw new Error('MODE must be core, large, or all')
  }
}

function tierConfig (scenario: Scenario): Record<string, number> {
  return Object.fromEntries(
    Array.from({ length: scenario.tierCount }, (_, tier) => [`tier-${tier}`, DEFAULT_GROUP_LIMIT])
  )
}

function queryParams (scenario: Scenario): { groupLimit: string, tiers: string, values: unknown[] } {
  if (scenario.tierCount === 0) {
    return { groupLimit: '$1::int', tiers: '', values: [DEFAULT_GROUP_LIMIT] }
  }

  return {
    groupLimit: 'COALESCE(($2::jsonb ->> group_tier)::int, $1::int)',
    tiers: '$2::jsonb',
    values: [DEFAULT_GROUP_LIMIT, JSON.stringify(tierConfig(scenario))]
  }
}

function buildOriginalQuery (schema: string, scenario: Scenario): Query {
  const params = queryParams(scenario)
  const candidateLimit = scenario.tierCount > 0
    ? 'COALESCE(($2::jsonb ->> j.group_tier)::int, $1::int)'
    : '$1::int'

  return {
    text: `
      WITH
      active_group_counts AS MATERIALIZED (
        SELECT group_id, COUNT(*)::int as active_cnt
        FROM ${schema}.${TABLE}
        WHERE name = '${schema}' AND state = 'active' AND group_id IS NOT NULL
        GROUP BY group_id
      ),
      next AS (
        SELECT j.id, j.group_id, j.group_tier
        FROM ${schema}.${TABLE} j
        LEFT JOIN active_group_counts agc ON j.group_id = agc.group_id
        WHERE j.name = '${schema}'
          AND j.state < 'active'
          AND NOT j.blocked
          AND j.start_after <= now()
          AND (j.group_id IS NULL OR agc.active_cnt IS NULL OR agc.active_cnt < ${candidateLimit})
        ORDER BY j.created_on, j.id
        LIMIT ${scenario.batchSize}
        FOR UPDATE OF j SKIP LOCKED
      ),
      group_ranking AS (
        SELECT t.id, t.group_id, t.group_tier,
          ROW_NUMBER() OVER (PARTITION BY t.group_id ORDER BY t.id) as group_rn,
          COALESCE(agc.active_cnt, 0) as active_cnt
        FROM next t
        LEFT JOIN active_group_counts agc ON t.group_id = agc.group_id
      ),
      group_filtered AS (
        SELECT id FROM group_ranking
        WHERE group_id IS NULL OR (active_cnt + group_rn) <= ${params.groupLimit}
      )
      UPDATE ${schema}.${TABLE} j SET
        state = 'active',
        started_on = now(),
        heartbeat_on = now(),
        retry_count = CASE WHEN started_on IS NOT NULL THEN retry_count + 1 ELSE retry_count END
      FROM group_filtered
      WHERE name = '${schema}' AND j.id = group_filtered.id
      RETURNING j.${JOB_COLUMNS_MIN}
    `,
    values: params.values
  }
}

function sqlLiteral (value: string): string {
  return `'${value.replaceAll("'", "''")}'`
}

function saturatedGroupsArray (limitExpression: string): string {
  return `ARRAY(
          SELECT active_group_counts.group_id
          FROM active_group_counts
          WHERE active_group_counts.active_cnt >= ${limitExpression}
        )`
}

function buildArrayQuery (schema: string, scenario: Scenario): Query {
  const params = queryParams(scenario)
  const tiers = tierConfig(scenario)
  const saturationFilter = scenario.tierCount === 0
    ? `j.group_id <> ALL (${saturatedGroupsArray('$1::int')})`
    : `CASE
        ${Object.keys(tiers).map(tier => `WHEN j.group_tier = ${sqlLiteral(tier)}
          THEN j.group_id <> ALL (${saturatedGroupsArray(`($2::jsonb ->> ${sqlLiteral(tier)})::int`)})`).join('\n        ')}
        ELSE j.group_id <> ALL (${saturatedGroupsArray('$1::int')})
      END`

  return {
    text: `
      WITH
      active_group_counts AS MATERIALIZED (
        SELECT group_id, COUNT(*)::int as active_cnt
        FROM ${schema}.${TABLE}
        WHERE name = '${schema}' AND state = 'active' AND group_id IS NOT NULL
        GROUP BY group_id
      ),
      next AS (
        SELECT j.id, j.group_id, j.group_tier
        FROM ${schema}.${TABLE} j
        WHERE j.name = '${schema}'
          AND j.state < 'active'
          AND NOT j.blocked
          AND j.start_after < now()
          AND (j.group_id IS NULL OR ${saturationFilter})
        ORDER BY j.created_on, j.id
        LIMIT ${scenario.batchSize}
        FOR UPDATE OF j SKIP LOCKED
      ),
      group_ranking AS (
        SELECT t.id, t.group_id, t.group_tier,
          ROW_NUMBER() OVER (PARTITION BY t.group_id ORDER BY t.id) as group_rn,
          COALESCE(agc.active_cnt, 0) as active_cnt
        FROM next t
        LEFT JOIN active_group_counts agc ON t.group_id = agc.group_id
      ),
      group_filtered AS (
        SELECT id FROM group_ranking
        WHERE group_id IS NULL OR (active_cnt + group_rn) <= ${params.groupLimit}
      )
      UPDATE ${schema}.${TABLE} j SET
        state = 'active',
        started_on = now(),
        heartbeat_on = now(),
        retry_count = CASE WHEN started_on IS NOT NULL THEN retry_count + 1 ELSE retry_count END
      FROM group_filtered
      WHERE name = '${schema}' AND j.id = group_filtered.id
      RETURNING j.${JOB_COLUMNS_MIN}
    `,
    values: params.values
  }
}

function buildMapQuery (schema: string, scenario: Scenario): Query {
  const groupConcurrency = scenario.tierCount > 0
    ? { default: DEFAULT_GROUP_LIMIT, tiers: tierConfig(scenario) }
    : DEFAULT_GROUP_LIMIT

  return plans.fetchNextJob({
    schema,
    table: TABLE,
    name: schema,
    policy: 'standard',
    limit: scenario.batchSize,
    priority: false,
    orderByCreatedOn: true,
    ignoreSingletons: null,
    groupConcurrency
  })
}

async function seedFixture (db: Db, schema: string, scenario: Scenario): Promise<void> {
  const firstTier = scenario.tierCount > 0 ? 'tier-0' : null

  // Analyze after one active row. Every later insert is invisible to column statistics, matching
  // the stale-estimate condition from the regression test and #842.
  await db.executeSql(`
    INSERT INTO ${schema}.${TABLE}
      (name, data, state, group_id, group_tier, start_after, created_on, started_on)
    VALUES ($1, '{}'::jsonb, 'active'::${schema}.job_state, 'saturated-0', $2,
      now() - interval '5 minutes', now() - interval '5 minutes', now() - interval '5 minutes')
  `, [schema, firstTier])

  await db.executeSql(`ANALYZE ${schema}.${TABLE}`)

  // Fill the second slot for groupConcurrency=2 after ANALYZE so saturated-0 is actually full
  // while the planner still estimates from the one-row table.
  await db.executeSql(`
    INSERT INTO ${schema}.${TABLE}
      (name, data, state, group_id, group_tier, start_after, created_on, started_on)
    VALUES ($1, '{}'::jsonb, 'active'::${schema}.job_state, 'saturated-0', $2,
      now() - interval '5 minutes', now() - interval '5 minutes', now() - interval '5 minutes')
  `, [schema, firstTier])

  if (scenario.saturatedGroups > 1) {
    await db.executeSql(`
      INSERT INTO ${schema}.${TABLE}
        (name, data, state, group_id, group_tier, start_after, created_on, started_on)
      SELECT $1, '{}'::jsonb, 'active'::${schema}.job_state,
        'saturated-' || group_number::text,
        CASE WHEN $3::int > 0 THEN 'tier-' || (group_number % $3::int)::text ELSE NULL END,
        now() - interval '5 minutes', now() - interval '5 minutes', now() - interval '5 minutes'
      FROM generate_series(1, $2::int - 1) group_number
      CROSS JOIN generate_series(1, 2)
    `, [schema, scenario.saturatedGroups, scenario.tierCount])
  }

  if (scenario.otherActiveGroups > 0) {
    await db.executeSql(`
      INSERT INTO ${schema}.${TABLE}
        (name, data, state, group_id, start_after, created_on, started_on)
      SELECT $1, '{}'::jsonb, 'active'::${schema}.job_state,
        'active-' || group_number::text,
        now() - interval '5 minutes', now() - interval '5 minutes', now() - interval '5 minutes'
      FROM generate_series(1, $2::int) group_number
    `, [schema, scenario.otherActiveGroups])
  }

  await db.executeSql(`
    INSERT INTO ${schema}.${TABLE}
      (name, data, state, group_id, group_tier, start_after, created_on)
    SELECT $1, '{}'::jsonb, 'created'::${schema}.job_state,
      'saturated-' || ((job_number - 1) % $2::int)::text,
      CASE WHEN $3::int > 0 THEN 'tier-' || (((job_number - 1) % $2::int) % $3::int)::text ELSE NULL END,
      now() - interval '5 minutes',
      now() - interval '4 minutes' + (job_number * interval '1 microsecond')
    FROM generate_series(1, $4::int) job_number
  `, [schema, scenario.saturatedGroups, scenario.tierCount, scenario.pendingCount])

  await db.executeSql(`
    INSERT INTO ${schema}.${TABLE}
      (name, data, state, group_id, group_tier, start_after, created_on)
    SELECT $1, '{}'::jsonb, 'created'::${schema}.job_state,
      'available-' || job_number::text,
      CASE WHEN $2::int > 0 THEN 'tier-0' ELSE NULL END,
      now() - interval '5 minutes',
      now() - interval '3 minutes' + (job_number * interval '1 microsecond')
    FROM generate_series(1, $3::int) job_number
  `, [schema, scenario.tierCount, scenario.eligibleCount])

  if (scenario.refreshStats) {
    await db.executeSql(`ANALYZE ${schema}.${TABLE}`)
  }
}

async function runBenchmark (
  scenario: Scenario,
  variant: Variant,
  run: number,
  sequence: number
): Promise<Result> {
  const schema = `gcperf_${process.pid}_${run}_${sequence}_${variant.id}`
  const config = helper.getConfig({ schema })
  const boss = new PgBoss(config)
  boss.on('error', console.error)
  let db: Db | undefined

  try {
    await boss.start()
    await boss.createQueue(schema)
    db = new Db(config)
    await db.open()
    await seedFixture(db, schema, scenario)

    const query = variant.build(schema, scenario)
    const explained = await db.executeSql(
      `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${query.text}`,
      query.values
    )
    const raw = explained.rows[0]['QUERY PLAN'] as string | ExplainReport[]
    const report = (typeof raw === 'string' ? JSON.parse(raw) : raw)[0] as ExplainReport
    const nodes = collectPlanNodes(report.Plan)
    const initPlans = nodes.filter(node => node['Subplan Name']?.startsWith('InitPlan'))
    const claimed = report.Plan['Actual Rows'] ?? 0

    if (claimed !== scenario.batchSize) {
      throw new Error(`${scenario.id}/${variant.id} claimed ${claimed}; expected ${scenario.batchSize}`)
    }

    return {
      scenario: scenario.id,
      variant: variant.id,
      executionMs: report['Execution Time'],
      planningMs: report['Planning Time'],
      claimed,
      queryChars: query.text.length,
      activeCteMaxLoops: maxLoops(nodes, node =>
        node['Node Type'] === 'CTE Scan' && node['CTE Name']?.startsWith('active_group_') === true
      ),
      rankingMaxLoops: maxLoops(nodes, node => node['Node Type'] === 'WindowAgg'),
      jobTableMaxLoops: maxLoops(nodes, node => node['Relation Name'] === TABLE),
      initPlans: initPlans.length,
      executedInitPlans: initPlans.filter(node => (node['Actual Loops'] ?? 0) > 0).length
    }
  } finally {
    await boss.stop({ timeout: 2000 }).catch(() => {})
    if (db) {
      if (KEEP) {
        console.log(`KEEP=1: retained schema ${schema}`)
      } else {
        await db.executeSql(`DROP SCHEMA IF EXISTS ${schema} CASCADE`)
      }
      await db.close()
    } else if (!KEEP) {
      await helper.dropSchema(schema).catch(() => {})
    }
  }
}

function collectPlanNodes (node: PlanNode, nodes: PlanNode[] = []): PlanNode[] {
  nodes.push(node)
  for (const child of node.Plans ?? []) collectPlanNodes(child, nodes)
  return nodes
}

function maxLoops (nodes: PlanNode[], predicate: (node: PlanNode) => boolean): number {
  return Math.max(0, ...nodes.filter(predicate).map(node => node['Actual Loops'] ?? 0))
}

function median (values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b)
  return sorted[Math.floor(sorted.length / 2)]
}

function groupedResults (results: Result[]): Map<string, Result[]> {
  const groups = new Map<string, Result[]>()
  for (const result of results) {
    const key = `${result.scenario}:${result.variant}`
    groups.set(key, [...(groups.get(key) ?? []), result])
  }
  return groups
}

function formatDuration (milliseconds: number): string {
  return milliseconds >= 1000
    ? `${(milliseconds / 1000).toFixed(2)} s`
    : `${milliseconds.toFixed(2)} ms`
}

function printTimingSummary (results: Result[], scenarios: Scenario[]): void {
  const groups = groupedResults(results)
  console.log('\n## Execution time\n')
  console.log('| Scenario | Original | Array | Map |')
  console.log('|---|---:|---:|---:|')
  for (const scenario of scenarios) {
    const cells = variants.map(variant => {
      const rows = groups.get(`${scenario.id}:${variant.id}`) ?? []
      return formatDuration(median(rows.map(row => row.executionMs)))
    })
    console.log(`| ${scenario.id} | ${cells.join(' | ')} |`)
  }
}

function printPlanSummary (results: Result[], scenarios: Scenario[]): void {
  const groups = groupedResults(results)
  console.log('\n## Plan loops (active-count/ranking)\n')
  console.log('| Scenario | Original | Array | Map |')
  console.log('|---|---:|---:|---:|')
  for (const scenario of scenarios) {
    const cells = variants.map(variant => {
      const rows = groups.get(`${scenario.id}:${variant.id}`) ?? []
      const active = median(rows.map(row => row.activeCteMaxLoops))
      const ranking = median(rows.map(row => row.rankingMaxLoops))
      return `${active}/${ranking}`
    })
    console.log(`| ${scenario.id} | ${cells.join(' | ')} |`)
  }
}

main().catch(error => {
  console.error(error)
  process.exitCode = 1
})
