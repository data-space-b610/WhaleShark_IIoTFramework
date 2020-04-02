create table if not exists SENSORTAG_DATA 
(
  time Date not null,
  thingId varchar not null,
  temperature double
  CONSTRAINT PK PRIMARY KEY (time, thingId)
)