DROP TABLE IF EXISTS public.staging_events;
DROP TABLE IF EXISTS public.staging_songs;
DROP TABLE IF EXISTS public.songplay;
DROP TABLE IF EXISTS public.user;
DROP TABLE IF EXISTS public.song;
DROP TABLE IF EXISTS public.artist;
DROP TABLE IF EXISTS public."time";

CREATE TABLE public.staging_events
(
    artist              varchar(25),
    auth                varchar(20),
    firstName           text,
    gender              text,
    itemInSession       int,
    lastName            text,
    length              decimal(3),
    level               text,
    location            varchar(30),
    method              text,
    page                varchar(20),
    registration        timestamp,
    sessionId           int,
    song                varchar(25),
    status              int,
    ts                  bigint,
    userAgent           varchar(30),
    userId              int
);


CREATE TABLE public.staging_songs
(
    artist_id           varchar(25),
    artist_latitude     decimal(5),
    artist_location     varchar(30),
    artist_longitude    decimal(5),
    artist_name         varchar(25),
    duration            decimal(5),
    num_songs           int,
    song_id             varchar(20),
    title               varchar(25),
    year                int
);


CREATE TABLE public.songplay
(
    songplay_id        int,
    start_time         timestamp,
    user_id            int,
    level              text,
    song_id            int,
    artist_id          int,
    session_id         int,
    location           varchar(30),
    user_agent         varchar(30)
);


CREATE TABLE public.user
(
    user_id            int,
    first_name         text,
    last_name          text,
    gener              text,
    level              text
);


CREATE TABLE public.song
(
   song_id             int,
   title               varchar(30),
   artist_id           int,
   year                int,
   duration            decimal(5)
); 


CREATE TABLE public.artist
(
   artist_id           int,
   name                varchar(25),
   location            varchar(30),
   latitude            decimal(5),
   longitude           decimal(5)
);


CREATE TABLE public."time"
(
    start_time         timestamp,
    hour               int,
    day                text,
    week               text,
    month              text,
    year               int,
    weekday            text
);
