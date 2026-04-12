import { ProfileResult } from './queryProfiler';

export interface Bottleneck {
  type: 'SEQ_SCAN' | 'HIGH_COST' | 'SLOW_EXECUTION' | 'MISSING_INDEX';
  severity: 'LOW' | 'MEDIUM' | 'HIGH';
  message: string;
  suggestion: string;
}

export function detectBottlenecks(profile: ProfileResult): Bottleneck[] {
  const bottlenecks: Bottleneck[] = [];

  if (profile.hasSeqScan) {
    bottlenecks.push({
      type: 'SEQ_SCAN',
      severity: profile.totalCost > 1000 ? 'HIGH' : 'MEDIUM',
      message: `Sequential scan — full table read on every execution`,
      suggestion: 'Add index on filter/join columns. Use CREATE INDEX CONCURRENTLY to avoid table locks.',
    });
  }

  if (profile.executionTime > 100) {
    bottlenecks.push({
      type: 'SLOW_EXECUTION',
      severity: profile.executionTime > 500 ? 'HIGH' : 'MEDIUM',
      message: `Execution ${profile.executionTime.toFixed(2)}ms exceeds 100ms threshold`,
      suggestion: 'Consider query restructuring, partial indexes, or Redis caching.',
    });
  }

  if (profile.totalCost > 10000) {
    bottlenecks.push({
      type: 'HIGH_COST',
      severity: 'HIGH',
      message: `Planner cost estimate ${profile.totalCost.toFixed(0)} — large row set scan`,
      suggestion: 'Review JOINs for missing indexes. Consider materialized views for aggregates.',
    });
  }

  if (!profile.hasIndexScan && profile.estimatedRows > 1000) {
    bottlenecks.push({
      type: 'MISSING_INDEX',
      severity: 'HIGH',
      message: `No index scan on ${profile.estimatedRows} estimated rows`,
      suggestion: 'Create composite index on WHERE columns. Run ANALYZE to update statistics.',
    });
  }

  return bottlenecks;
}

export function printBottleneckReport(name: string, profile: ProfileResult, bottlenecks: Bottleneck[]) {
  console.log('\n' + '─'.repeat(65));
  console.log(`🔍 Profile: ${name}`);
  console.log(`   Planning: ${profile.planningTime}ms | Execution: ${profile.executionTime}ms | Cost: ${profile.totalCost}`);
  console.log(`   Seq Scan: ${profile.hasSeqScan ? '❌' : '✅'} | Index Scan: ${profile.hasIndexScan ? '✅' : '❌'}`);

  if (bottlenecks.length === 0) {
    console.log('   ✅ No bottlenecks detected');
  } else {
    bottlenecks.forEach((b, i) => {
      console.log(`\n   [${i + 1}] [${b.severity}] ${b.type}`);
      console.log(`       Problem: ${b.message}`);
      console.log(`       Fix:     ${b.suggestion}`);
    });
  }
}