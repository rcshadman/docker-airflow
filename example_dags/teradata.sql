DROP TABLE syslib.testData;

CREATE TABLE syslib.testData (
  nam varchar(255) ,
  mail varchar(255) ,
  created_at varchar(255),
  rut varchar(15) ,
  dv varchar(15) ,
  prob varchar(100)
)
primary index(rut, prob);