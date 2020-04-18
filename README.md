A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. 
The analytics team is particularly interested in understanding what songs users are listening to

The Sparkify schema has 7 tables, two are staging, one is Fact the others are dimension tables.  
Staging Tables;
events_staging --> logs from an imaginary music streaming app based on configuration settings
songs_staging --> contains metadata about a song and the artist of that song

Fact Table;
songplays --> contains when, who listen which song of an artist 

Dimension Tables;
users --> users in the app
songs --> songs in music database
artists --> artists in music database
time --> broken down into specific time units

Table Row Counts
songs_staging 385252 rows
events_staging 8056 rows
songplays: 245719 rows
(unique) users: 104 rows
songs: 384824 rows
artists: 45266 rows
time: 6813 rows

Example Queries
--Top Songs 
select a.title, count(*) as cnt
from songplays s
left join songs a on s.song_id = a.song_id
group by a.title
order by 2 desc

--Top Monhtly Artist
select t.month, a.name, count(*) as cnt
from songplays s
left join artists a on s.artist_id = s.artist_id
left join time t on t.start_time = s.start_time
group by t.month, a.name
order by 3 desc
