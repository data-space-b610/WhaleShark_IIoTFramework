create table if not exists FOO_DATA 
(
  dateTime Date not null,
  deviceId integer not null,
  temperature double,
  humidity double,
  message varchar
  CONSTRAINT PK PRIMARY KEY (dateTime, deviceID)
)