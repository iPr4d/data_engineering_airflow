DROP TABLE trump_rt;
DROP TABLE trump_not_rt;
CREATE TABLE trump_rt (id varchar(255) primary key, text varchar(1000), author varchar(255), date varchar(255));
CREATE TABLE trump_not_rt (id varchar(255) primary key, text varchar(1000), author varchar(255), date varchar(255));