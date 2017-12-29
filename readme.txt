================TEJ BASE===============

To run successfully,
1) Run SetUp.java file
2) Run tejBase.java file

Please note that you need to manually create directories
+-data
----catalog
----user_data

Operations allowed:
1) Create a new table with 1 Primary key that is the first column
2) Insert values into table
3) Delete a row from table
4) Drop table
5) Display the tables
6) Query tables

Description:

Type of datatypes allowed: 
INT
SMALLINT
TINYINT
BIGINT
REAL
DOUBLE
TEXT
===================================================

Note that in query every word should be separated by space.
eg insert into employee ( id , name , ssn , salary , dept ) values ( 1 , Tej , 123456789 , 100000.5379 , 2 );
See sample query file for more.

===================================================

Query Formats

1) Create a new table
query: CREATE TABLE tablename ( columnname int primary key , columnname2 datatype , columnname3 datatype [not null] , ... );

2) Insert value in table
query: INSERT INTO tablename ( columns ) VALUES ( value1 , ... );

User should write [not null] to not allow null values. By default its null so dont add [null].
Value should be added in same order as table is created.

3) Delete from table
query: DELETE FROM tablename WHERE columnname = uservalue ;
Delete specific rows from table.

4) Query Table
query : SELECT column_names FROM TABLE WHERE columnname = uservalue;
column names can be any number but should be there in table.
place * in place of column names to display all columns.

Only one condition statement is supported so no AND, ORs etc is supported.

Note that you need to provide one condition. i.e "select * from employee;" will not work.

eg: select * from employee where id < 2;
eg: select dept from employee where salary = 10000;

5) Drop table
query: DROP TABLE tablename;

6) Show Tables;
query: SHOW TABLES;




