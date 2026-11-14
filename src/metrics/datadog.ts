import StatsD from 'hot-shots';

const dogstatsd = new StatsD({
  host: process.env.DD_AGENT_HOST || 'localhost',
  port: 8125,
  prefix: 'pgpulse.',
  errorHandler: () => {}, // agent optional locally
});

export function recordQueryLatency(queryName: string, latencyMs: number, cached: boolean) {
  dogstatsd.histogram('query.latency_ms', latencyMs, [`query:${queryName}`, `cached:${cached}`]);
  console.log(`   [DD] pgpulse.query.latency_ms  ${latencyMs}ms  query:${queryName} cached:${cached}`);
}

export function recordCacheHitRate(hits: number, total: number) {
  const rate = total > 0 ? (hits / total) * 100 : 0;
  dogstatsd.gauge('cache.hit_rate', rate);
  console.log(`   [DD] pgpulse.cache.hit_rate  ${rate.toFixed(1)}%`);
}

export function recordThroughput(queryName: string, rps: number) {
  dogstatsd.gauge('query.throughput_rps', rps, [`query:${queryName}`]);
  console.log(`   [DD] pgpulse.query.throughput_rps  ${rps.toFixed(1)} rps  query:${queryName}`);
}

export function recordBottleneck(queryName: string, type: string, severity: string) {
  dogstatsd.increment('bottleneck.detected', [`query:${queryName}`, `type:${type}`, `severity:${severity}`]);
  console.log(`   [DD] pgpulse.bottleneck.detected  query:${queryName} type:${type} severity:${severity}`);
}