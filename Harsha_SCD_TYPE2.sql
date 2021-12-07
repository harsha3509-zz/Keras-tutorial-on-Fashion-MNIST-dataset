SRC:

create table Patient_Details (pk bigint,phone_no bigint,name varchar(50))

insert into patient_details values (123,987654321,'Jhoney');
insert into patient_details values(234,876543210,'Stuart');
insert into patient_details values(567,345678901,'Max Stuart');
insert into patient_details values(456,765432101,'Jeff');


TGT:

create table patient_details_tgt (pk bigint,phone_no bigint,name varchar(50),eff_from_dt timestamp
								  ,eff_to_dt timestamp,flag numeric)
								  

insert into patient_details_tgt values (345,345678901,'Jhoney Stuart','2016-06-01',NULL,1);
insert into patient_details_tgt values (234,234567890,'Stuart','2016-06-01',NULL,1);
insert into patient_details_tgt values (123,123456789,'Jhoney','2016-06-01',NULL,1);
insert into patient_details_tgt values (567,345678901,'Max Stuart','2016-06-01',NULL,1);

TEMP:

create table patient_details_temp as select * from patient_details_tgt where 1=2


Step 1: Load expired records from TGT to TEMP table: (Inactive records from target)

INSERT INTO patient_details_temp
SELECT * FROM patient_details_tgt
WHERE FLAG = 0 and eff_to_dt is not null;

Step 2: Get all records which are going to expire (The records that are going to be updated with the new data from source)

INSERT into patient_details_temp
SELECT TGT.pk ,
TGT.PHONE_NO,
TGT.NAME,
eff_from_dt,
now()::date -1 as eff_to_dt,
0 as flag
FROM patient_details_tgt TGT
WHERE TGT.flag = 1
AND EXISTS (SELECT 1 FROM
patient_details SRC
WHERE TGT.pk = src.pk
and (
TGT.PHONE_NO <> src.PHONE_NO
or TGT.NAME <> src.NAME )
);

Step 3: Copy active records from TGT to TEMP table


These are the active records present only in the target table or those records with the same values as in the source. Hence, target is going to be truncate and load, copy these records into the temp table.

INSERT INTO patient_details_temp
SELECT TGT.pk ,
TGT.PHONE_NO,
TGT.NAME,
eff_from_dt::DATE,
EFF_TO_DT,
FLAG
FROM patient_details_tgt TGT
WHERE FLAG = 1
AND NOT EXISTS (SELECT 1 FROM
patient_details SRC
WHERE TGT.pk = SRC.pk )
UNION ALL --Include unchaged records
SELECT TGT.pk ,
TGT.PHONE_NO,
TGT.NAME,
eff_from_dt::DATE,
EFF_TO_DT,
FLAG
FROM patient_details_tgt TGT
WHERE TGT.FLAG = 1
AND EXISTS (SELECT 1 FROM
patient_details SRC
WHERE TGT.pk = SRC.pk
AND (
TGT.PHONE_NO = SRC.PHONE_NO
AND TGT.NAME = SRC.NAME )
);


Step 4: Copy only updated records from LOAD table (Updated records from source)

INSERT INTO patient_details_temp

SELECT SRC.pk ,

SRC.PHONE_NO,
SRC.NAME,

now()::date,

null,

1

FROM patient_details src

WHERE EXISTS (SELECT 1

FROM   patient_details_temp TEMP1
  WHERE  src.pk = TEMP1.pk

AND    flag = 0

AND NOT EXISTS

(SELECT 1

FROM   patient_details_temp TEMP2

WHERE  TEMP1.pk = TEMP2.pk

AND   flag = 1 ));


Step 5: Copy fresh records from LOAD to TEMP (Insert/new records from source)

This will get only the new records that have come in source but not present in target.

INSERT INTO patient_details_temp

SELECT SRC.pk ,

SRC.PHONE_NO,
SRC.NAME,

NOW()::DATE,

NULL,

1

FROM patient_details SRC

WHERE NOT EXISTS     (SELECT 1

FROM   patient_details_temp INT
 WHERE  SRC.pk = INT.pk);

Step 6: Truncate and load TGT table:

TRUNCATE TABLE patient_details_tgt;

INSERT INTO patient_details_tgt

SELECT * FROM patient_details_temp;

 