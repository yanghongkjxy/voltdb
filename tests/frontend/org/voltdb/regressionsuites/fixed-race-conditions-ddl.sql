
CREATE TABLE ENG_13725_T1 (
  ID      INTEGER  DEFAULT 0,
  VARBIN  VARBINARY(100)  DEFAULT x'00',
  PRIMARY KEY (ID)
);

CREATE TABLE ENG_13725_T2 (
  VARBIN  VARBINARY(100)
);

CREATE INDEX IDX_ENG_13725_T2_VBIN ON ENG_13725_T2 (VARBIN);
