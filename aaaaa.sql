create database Python;
create role Admin with password 'Abcd4321';
alter role Admin LOGIN;
Grant connect on database "Python" to Admin;
SELECT boot_val,reset_val
FROM pg_settings
WHERE name='listen_addresses';
SELECT *
FROM pg_settings
WHERE name = 'port';
select inet_server_addr();
Create table Data 
( 
    ID_Data serial not null constraint PK_Data primary key, 
	Data_Address_ID varchar(20) not null,
	Data_Date_Time timestamp not null,
	Data_Request_Method varchar not null,
	Data_Status_Code varchar(25) not null,
	Data_Response_Size varchar(25) not null,
	Data_Referer varchar(20) not null,
	Data_User_Agent varchar not null
);
create table Costumer
(
	Id_User serial not null constraint PK_User primary key,
	User_Login varchar not null constraint UQ_User unique,
	User_Password varchar not null constraint CH_Password_User check(User_Password similar to 
																								'%[a-z]%' and User_Password similar to 
																								'%[A-Z]%' and User_Password similar to '%[!@#$%^&*()-]%')

)
insert into Costumer(User_Login,User_Password)
values('Vasya_P','i@34T')
insert into Costumer(User_Login,User_Password)
values('Vasya_T','I@34t')

