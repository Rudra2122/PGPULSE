import { pool } from '../db/postgres';

export interface ProfileResult {
  query: string;
  planningTime: number;
  executionTime: number;
  totalCost: number;
  planText: string;
  hasSeqScan: boolean;
  hasIndexScan: boolean;
  estimatedRows: number;
}

export async function profileQuery(sql: string, params?: any[]): Promise<ProfileResult> {
  const explainSql = `EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT TEXT) ${sql}`;
  const result = await pool.query(explainSql, params);
  const planText: string = result.rows.map((r: any) => r['QUERY PLAN']).join('\n');

  const planningMatch = planText.match(/Planning Time:\s*([\d.]+)\s*ms/);
  const executionMatch = planText.match(/Execution Time:\s*([\d.]+)\s*ms/);
  const costMatch = planText.match(/cost=[\d.]+\.\.([\d.]+)/);
  const rowsMatch = planText.match(/rows=(\d+)/);

  return {
    query: sql,
    planningTime: parseFloat(planningMatch?.[1] || '0'),
    executionTime: parseFloat(executionMatch?.[1] || '0'),
    totalCost: parseFloat(costMatch?.[1] || '0'),
    planText,
    hasSeqScan: planText.includes('Seq Scan'),
    hasIndexScan: planText.includes('Index Scan') || planText.includes('Index Only Scan'),
    estimatedRows: parseInt(rowsMatch?.[1] || '0'),
  };
}