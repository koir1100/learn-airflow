{
      'table': 'mau_summary',
      'schema': 'yonggu_choi_14',
      'main_sql': """SELECT 
TO_CHAR(A.ts, 'YYYY-MM') AS month,
COUNT(DISTINCT B.userid) AS mau
    FROM raw_data.session_timestamp A
        INNER JOIN raw_data.user_session_channel B
        ON A.sessionid = B.sessionid
    GROUP BY month;""",
      'input_check':
      [
        {
          'sql': 'SELECT COUNT(*) FROM raw_data.session_timestamp',
          'count': 101000
        },
        {
          'sql': 'SELECT COUNT(*) FROM raw_data.user_session_channel',
          'count': 101000
        },
      ],
      'output_check':
      [
        {
          'sql': 'SELECT COUNT(month) FROM {schema}.temp_{table}',
          'count': 7
        }
      ],
}
