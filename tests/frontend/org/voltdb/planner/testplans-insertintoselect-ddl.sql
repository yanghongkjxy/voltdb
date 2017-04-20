CREATE TABLE T1 (
  ID      BIGINT NOT NULL PRIMARY KEY,
  AAA     INTEGER,
  BBB     INTEGER
);

CREATE TABLE T2 (
  ID      BIGINT NOT NULL PRIMARY KEY,
  AAA     INTEGER,
  BBB     INTEGER
);

CREATE TABLE P1 (
  ID      BIGINT NOT NULL PRIMARY KEY,
  AAA     INTEGER NOT NULL,
  BBB     INTEGER
);
PARTITION TABLE P1 ON COLUMN ID;

CREATE TABLE P2 (
  ID      BIGINT NOT NULL,
  AAA     INTEGER NOT NULL,
  BBB     INTEGER,
);
PARTITION TABLE P2 ON COLUMN ID;
