from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    """
    Insert query for Fact table
    """
    fact_sql = """
               INSERT INTO {} (songplay_id, 
                               start_time, 
                               userid, 
                               level, 
                               song_id, 
                               artist_id, 
                               sessionid, 
                               location, 
                               useragent)
                              (SELECT md5(events.sessionid || events.start_time) songplay_id,
                                      events.start_time, 
                                      events.userid, 
                                      events.level, 
                                      songs.song_id, 
                                      songs.artist_id, 
                                      events.sessionid, 
                                      events.location, 
                                      events.useragent
                               FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, 
                                            *
                                     FROM staging_events
                                     WHERE page = 'NextSong') events
                               LEFT JOIN staging_songs songs
                               ON events.song = songs.title
                               AND events.artist = songs.artist_name
                               AND events.length = songs.duration)
               """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 fact_sql = "",
                 target_table = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.fact_sql = fact_sql
        self.target_table = target_table

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination fact table if it exists: {}".format(self.target_table))
        fct_del_sql = """
                      DELETE FROM songplays using staging_events 
                      WHERE songplays.songplay_id = (md5(staging_events.sessionid || TIMESTAMP 'epoch' + staging_events.ts/1000 * interval '1 second'))
                      """
        redshift.run(fct_del_sql)

        self.log.info("Loading Fact Table: {}".format(self.target_table))
        formatted_sql = LoadFactOperator.fact_sql.format(
            self.target_table
        )
        redshift.run(formatted_sql)

