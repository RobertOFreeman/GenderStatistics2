---- PROJECT 2 ---- 

--- *SOME SQOOP COMMANDS* ---

sqoop export --connect jdbc:mysql://localhost/testdb --username root --password cloudera --table genderStatsRaw --export-dir /user/cloudera/HData/Gender_StatsData.csv --optionally-enclosed-by '\"' --direct

sqoop export 
    --connect jdbc:mysql://localhost/testdb 
    --username root 
    --password cloudera 
    --table genderStatsRaw 
    --export-dir /user/cloudera/HData/Gender_StatsData.csv 
    --optionally-enclosed-by '\"' --direct

---- create table genderStatsNorm as select countryabbr, infoabbr, y1960, y1961, y1962, y1963, y1964, y1965, y1966, y1967, y1968, y1969, y1970, y1971, y1972, y1973, y1974, y1975, y1976, y1977, y1978, y1979, y1980, y1981, y1982, y1983, y1984, y1985, y1986, y1987, y1988, y1989, y1990, y1991, y1992, y1993, y1994, y1995, y1996, y1997, y1998, y1999, y2000, y2001, y2002, y2003, y2004, y2005, y2006, y2007, y2008, y2009, y2010, y2011, y2012, y2013, y2014, y2015, y2016 from genderStats;

--- STEPS FOR GETTING SETTING UP THE DB AND THE TABLE ---
-- (1) Move datafile into HDFS: hdfs dfs -copyFromLocal [path to local source file] [path to target directory]
-- (2) In hive, create the database: create database [database name];
-- (3) In hive, create an empty table with columns appropriate for whatever data you plan to store.
--- For example:
---- create table genderStats(countryname string, countryabbr string, info string, infoabbr string, y1960 string, y1961 string, 
----                          y1962 string, y1963 string, y1964 string, y1965 string, y1966 string, y1967 string, y1968 string, 
----                          y1969 string, y1970 string, y1971 string, y1972 string, y1973 string, y1974 string, y1975 string, 
----                          y1976 string, y1977 string, y1978 string, y1979 string, y1980 string, y1981 string, y1982 string, 
----                          y1983 string, y1984 string, y1985 string, y1986 string, y1987 string, y1988 string, y1989 string, 
----                          y1990 string, y1991 string, y1992 string, y1993 string, y1994 string, y1995 string, y1996 string, 
----                          y1997 string, y1998 string, y1999 string, y2000 string, y2001 string, y2002 string, y2003 string, 
----                          y2004 string, y2005 string, y2006 string, y2007 string, y2008 string, y2009 string, y2010 string, 
----                          y2011 string, y2012 string, y2013 string, y2014 string, y2015 string, y2016 string) 
----                          ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
----                          WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"") 
----                          STORED AS TEXTFILE;
-- (4) Load the file into the empty table: 
--- For loading from HDFS: load data inpath '[path in HDFS]' into table genderStats;
--- For loading from local: load data local inpath '[path in local file system]' into table genderStats;
-- (5) You're done!

/*
create view genderStatsView as select countryname, countryabbr, info, infoabbr, 
                                      CAST(y1960 as double), CAST(y1961 as double), CAST(y1962 as double), CAST(y1963 as double), 
                                      CAST(y1964 as double), CAST(y1965 as double), CAST(y1966 as double), CAST(y1967 as double), 
                                      CAST(y1968 as double), CAST(y1969 as double), CAST(y1970 as double), CAST(y1971 as double), 
                                      CAST(y1972 as double), CAST(y1973 as double), CAST(y1974 as double), CAST(y1975 as double), 
                                      CAST(y1976 as double), CAST(y1977 as double), CAST(y1978 as double), CAST(y1979 as double),
                                      CAST(y1980 as double), CAST(y1981 as double), CAST(y1982 as double), CAST(y1983 as double),
                                      CAST(y1984 as double), CAST(y1985 as double), CAST(y1986 as double), CAST(y1987 as double),
                                      CAST(y1988 as double), CAST(y1989 as double), CAST(y1990 as double), CAST(y1991 as double),
                                      CAST(y1992 as double), CAST(y1993 as double), CAST(y1994 as double), CAST(y1995 as double), 
                                      CAST(y1996 as double), CAST(y1997 as double), CAST(y1998 as double), CAST(y1999 as double),
                                      CAST(y2000 as double), CAST(y2001 as double), CAST(y2002 as double), CAST(y2003 as double),
                                      CAST(y2004 as double), CAST(y2005 as double), CAST(y2006 as double), CAST(y2007 as double),
                                      CAST(y2008 as double), CAST(y2009 as double), CAST(y2010 as double), CAST(y2011 as double),
                                      CAST(y2012 as double), CAST(y2013 as double), CAST(y2014 as double), CAST(y2015 as double), 
                                      CAST(y2016 as double) from genderStats;
*/

--- QUERY 1 --
-- Identify the countries where % of female graduates is less than 30;

select countryname, femalegraduates from (select countryname, coalesce(y2016, y2015, y2014, y2014, y2013, y2012, y2011, y2010, y2009, y2008, y2007, y2006, y2005, y2004, y2003, y2002, y2001, y2000, 31) as femalegraduates from genderstatsview where infoabbr = 'SE.TER.CMPL.FE.ZS') tb1 where tb1.femalegraduates < 30;

select 
    countryname, 
    femalegraduates 
    from 
        (select 
            countryname, 
            coalesce(y2016, y2015, y2014, y2014, y2013, y2012, y2011, y2010, y2009, y2008, y2007, y2006, y2005, y2004, y2003, y2002, y2001, y2000, 31) as femalegraduates 
            from genderstatsview 
            where infoabbr = 'SE.TER.CMPL.FE.ZS') tb1 
    where tb1.femalegraduates < 30;

--- QUERY 2 --
-- List the average increase in female education in the US from the year 2000;

select countryname, (coalesce(y2016, y2015, y2014, y2013,0) - coalesce(y2000, y1999, y1998, y1997, 0))/2 as increase from genderstatsview where countryname = 'United States' and infoabbr = 'SE.TER.CUAT.BA.FE.ZS';

select 
    countryname,
    (coalesce(y2016,y2015,y2014,y2013,0) 
    - coalesce(y2000,y1999,y1998,y1997,0))/2 as increase 
    from genderstatsview 
    where countryname = 'United States' and infoabbr = 'SE.TER.CUAT.BA.FE.ZS';

--- QUERY 3 --
-- List the % change in male employment from the year 2000;

select * from 
    (select 
        countryname, 
        coalesce(y2016,y2015,y2014,y2013,0) 
        - coalesce(y2000,y1999,y1998,y1997,0) as difference 
        from genderstatsview 
        where infoabbr = 'SL.EMP.MPYR.MA.ZS') tb1 
    where tb1.difference != 0;

-- old version --
select countryname, difference
    from (select
            countryname,
            coalesce(y2016,y2015,y2014,y2013,0) 
            - coalesce(y2000,y1999,y1998,y1997,0) as difference
            from genderstatsview where = infoabbr = 'SL.EMP.MPYR.MA.ZS');
--

--- QUERY 4 --
-- List the % change in female employment from the year 2000;
select * from 
    (select 
        countryname, 
        coalesce(y2016,y2015,y2014,y2013,0) 
        - coalesce(y2000,y1999,y1998,y1997,0) as difference 
        from genderstatsview 
        where infoabbr = 'SL.EMP.MPYR.FE.ZS') tb1 
    where tb1.difference != 0;


--- QUERY 5 --
-- Getting the number of days required to start a business for males and females in every country;
with fe as (select countryname, coalesce(y2016,y2015,y2014,y2013,y2012,2011,2010,0) as daysForStartUpFe from genderstatsview where infoabbr = 'IC.REG.DURS.FE'), ma as (select countryname, coalesce(y2016,y2015,y2014,y2013,y2012,2011,2010,0) as daysForStartUpMa from genderstatsview where infoabbr = 'IC.REG.DURS.MA') select fe.countryname, daysForStartUpMa - daysForStartUpFe from fe join ma on fe.countryname = ma.countryname;

with 
    fe as 
        (select 
            countryname, 
            coalesce(y2016,y2015,y2014,y2013,y2012,2011,2010,0) as daysForStartUpFe from genderstatsview where infoabbr = 'IC.REG.DURS.FE'), 
    ma as 
        (select 
            countryname, 
            coalesce(y2016,y2015,y2014,y2013,y2012,2011,2010,0) as daysForStartUpMa from genderstatsview where infoabbr = 'IC.REG.DURS.MA') 
    select 
        fe.countryname, 
        daysForStartUpMa - daysForStartUpFe 
        from fe join ma on fe.countryname = ma.countryname;


/*select ... ();

WITH <alias_name> AS (sql_subquery_statement)
SELECT column_list FROM <alias_name>[,table_name]
[WHERE <join_condition>]

-- worked
with fe as (select countryname, coalesce(y2016,y2015,y2014,y2013,y2012,2011,2010,0) as daysForStartUpFe from genderstatsview where infoabbr = 'IC.REG.DURS.FE'), ma as (select countryname, coalesce(y2016,y2015,y2014,y2013,y2012,2011,2010,0) as daysForStartUpFe from genderstatsview where infoabbr = 'IC.REG.DURS.MA') select fe.countryname, daysForStartUpFe from fe; CAST(9.5 AS decimal(6,4))

with fe as (select countryname, coalesce(y2016,y2015,y2014,y2013,y2012,2011,2010,0) as daysForStartUpFe from genderstatsview where infoabbr = 'IC.REG.DURS.FE'), ma as (select countryname, coalesce(y2016,y2015,y2014,y2013,y2012,2011,2010,0) as daysForStartUpMa from genderstatsview where infoabbr = 'IC.REG.DURS.MA') select fe.countryname, cast(daysForStartUpMa - daysForStartUpFe as decimal(6,4)) from fe join ma on fe.countryname = ma.countryname;


with fe as (select countryname, coalesce(y2016,y2015,y2014,y2013,y2012,2011,2010,0) as daysForStartUpFe from genderstatsview where infoabbr = 'IC.REG.DURS.FE'), ma as (select countryname, coalesce(y2016,y2015,y2014,y2013,y2012,2011,2010,0) as daysForStartUpMa from genderstatsview where infoabbr = 'IC.REG.DURS.MA') select fe.countryname, daysForStartUpMa - daysForStartUpFe from fe join ma on fe.countryname = ma.countryname;


-- worked
select countryname, coalesce(y2016,y2015,y2014,y2013,y2012,2011,2010,0) as daysForStartUpFe from genderstatsview where infoabr = 'IC.REG.DURS.FE' fe
*/