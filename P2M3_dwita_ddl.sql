-- Create new database named 'milestone3'
CREATE DATABASE milestone3;

-- Create new table named 'tablem3'
CREATE TABLE IF NOT EXISTS table_m3 (
	track_id text,
	track_name text,
	track_artist text,
	track_popularity int,
	track_album_id text,
	track_album_name text,
	track_album_release_date text,
	playlist_name text,
	playlist_id text,
	playlist_genre text,
	playlist_subgenre text,
	danceability float,
	energy float,
	key int,
	loudness float,
	mode int,
	speechiness float,
	acousticness float,
	instrumentalness float,
	liveness float,
	valence float,
	tempo float,
	duration_ms int
);

-- Insert File CSV of Raw Data to Table table_m3
COPY table_m3(track_id, track_name, track_artist, track_popularity, track_album_id, track_album_name, track_album_release_date, playlist_name, playlist_id, playlist_genre, playlist_subgenre, danceability, energy, key, loudness, mode, speechiness, acousticness, instrumentalness, liveness, valence, tempo, duration_ms)
FROM '/Users/ita/github-classroom/FTDS-assignment-bay/p2-ftds024-rmt-m3-dwitawin/P2M3_dwita_data_raw.csv'
DELIMITER ','
CSV HEADER;

-- Check Raw Data from table_m3
SELECT *
FROM table_m3;
