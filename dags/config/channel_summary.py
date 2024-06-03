{
      'table': 'channel_summary',
      'schema': 'yonggu_choi_14',
      'main_sql': """SELECT 
DISTINCT A.userid,
FIRST_VALUE(A.channel) over (partition by A.userid order by B.ts rows between unbounded preceding and unbounded following) AS First_Channel,
LAST_VALUE(A.channel) over (partition by A.userid order by B.ts rows between unbounded preceding and unbounded following) AS Second_Channel
    FROM raw_data.user_session_channel AS A
        LEFT JOIN raw_data.session_timestamp AS B
        ON A.sessionid = B.sessionid;""",
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
          'sql': 'SELECT COUNT(userid) FROM {schema}.temp_{table}',
          'count': 949
        }
      ],
}
