{
          'table': 'nps_summary',
          'schema': 'yonggu_choi_14',
          'main_sql': """
SELECT LEFT(created_at, 10) AS date,
  ROUND(SUM(CASE
    WHEN score >= 9 THEN 1 
    WHEN score <= 6 THEN -1 END)::float*100/COUNT(1), 2)
FROM yonggu_choi_14.nps
GROUP BY 1
ORDER BY 1;""",
          'input_check':
          [
            {
              'sql': 'SELECT COUNT(created_at) FROM yonggu_choi_14.nps',
              'count': 150000
            },
          ],
          'output_check':
          [
            {
              'sql': 'SELECT COUNT(date) FROM {schema}.temp_{table}',
              'count': 12
            }
          ],
}
