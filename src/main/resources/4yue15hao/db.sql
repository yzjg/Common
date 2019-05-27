--创建数据库
create database ext_source;

--切换数据库
use ext_source;

--创建表
DROP TABLE IF EXISTS `ExDataSource`;
CREATE TABLE `ExDataSource` (
  `ID` varchar(255) NOT NULL,
  `URL` varchar(64) DEFAULT NULL,
  `DATASOURCE_TYPE` varchar(10) DEFAULT NULL,
  `TARGET_TABLE` varchar(50) DEFAULT NULL,
  `USERNAME` varchar(32) DEFAULT NULL,
  `SOURCE_TABLE` varchar(50) DEFAUL   T NULL,
  `PASSWORD` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


--dept.json数据如下
{"deptno":10, "dname":"ACCOUNTING", "loc":"NEW YORK"}
{"deptno":20, "dname":"RESEARCH", "loc":"DALLAS"}
{"deptno":30, "dname":"SALES", "loc":"CHICAGO"}
{"deptno":40, "dname":"OPERATIONS", "loc":"BOSTON"}


--JSON测试数据准备
INSERT INTO `ExDataSource`
(ID,URL,DATASOURCE_TYPE,TARGET_TABLE,USERNAME,SOURCE_TABLE,PASSWORD)
VALUES
('4028b0814ecd938f014ece1fef230001',
'hdfs://localhost:8020/data/dept.json',
'5',
'json_dept',
'',
'',
'');


--MySQL测试数据准备
INSERT INTO `ExDataSource`
(ID,URL,DATASOURCE_TYPE,TARGET_TABLE,USERNAME,SOURCE_TABLE,PASSWORD)
VALUES
('4028b0814ecd938f014ece1fef230002',
'jdbc:mysql://localhost:3306/sqoop',
'2',
'mysql_emp',
'root',
'EMP',
'root');


--MySQL表数据测试准备
create database sqoop;
use sqoop;

DROP TABLE IF EXISTS `EMP`;
CREATE TABLE `EMP` (
  `EMPNO` int(4) NOT NULL,
  `ENAME` varchar(10) DEFAULT NULL,
  `JOB` varchar(9) DEFAULT NULL,
  `MGR` int(4) DEFAULT NULL,
  `HIREDATE` date DEFAULT NULL,
  `SAL` int(7) DEFAULT NULL,
  `COMM` int(7) DEFAULT NULL,
  `DEPTNO` int(2) DEFAULT NULL,
  PRIMARY KEY (`EMPNO`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO EMP VALUES (7369,'SMITH','CLERK',7902,'1980-12-17',800,NULL,20);
INSERT INTO EMP VALUES (7499,'ALLEN','SALESMAN',7698,'1981-2-20',1600,300,30);
INSERT INTO EMP VALUES (7521,'WARD','SALESMAN',7698,'1981-2-22',1250,500,30);
INSERT INTO EMP VALUES (7566,'JONES','MANAGER',7839,'1981-4-2',2975,NULL,20);
INSERT INTO EMP VALUES (7654,'MARTIN','SALESMAN',7698,'1981-9-28',1250,1400,30);
INSERT INTO EMP VALUES (7698,'BLAKE','MANAGER',7839,'1981-5-1',2850,NULL,30);
INSERT INTO EMP VALUES (7782,'CLARK','MANAGER',7839,'1981-6-9',2450,NULL,10);
INSERT INTO EMP VALUES (7788,'SCOTT','ANALYST',7566,'87-7-13',3000,NULL,20);
INSERT INTO EMP VALUES (7839,'KING','PRESIDENT',NULL,'1981-11-17',5000,NULL,10);
INSERT INTO EMP VALUES (7844,'TURNER','SALESMAN',7698,'1981-9-8',1500,0,30);
INSERT INTO EMP VALUES (7876,'ADAMS','CLERK',7788,'87-7-13',1100,NULL,20);
INSERT INTO EMP VALUES (7900,'JAMES','CLERK',7698,'1981-12-3',950,NULL,30);
INSERT INTO EMP VALUES (7902,'FORD','ANALYST',7566,'1981-12-3',3000,NULL,20);
INSERT INTO EMP VALUES (7934,'MILLER','CLERK',7782,'1982-1-23',1300,NULL,10);