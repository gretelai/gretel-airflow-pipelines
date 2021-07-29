---
-- setup source database
---
drop database if exists userbookings;
create database userbookings;

\c userbookings;

drop table if exists sessions;
drop table if exists users;

--- users

create table users (
	id varchar(10) primary key,
    date_account_created date,
    timestamp_first_active varchar(255), -- hack this column is an odd format
    date_first_booking timestamp,
    gender varchar(32),
    age numeric,
    signup_method varchar(255),
    signup_flow varchar(255),
    "language" varchar(255),
    affiliate_channel varchar(255),
    affiliate_provider varchar(255),
    first_affiliate_tracked varchar(255),
    signup_app varchar(255),
    first_device_type varchar(255),
    first_browser varchar(255)
);


--- sessions

create table sessions (
    user_id varchar(10),
    "action" varchar(255),
    action_type varchar(255),
    action_detail varchar(255),
    device_type varchar(255),
    secs_elapsed numeric,
    foreign key(user_id) references users(id)
);

---
--- import data
---

-- users
\copy users from program 'zcat /data/users.csv.zip' with(format csv, header);

-- sessions
create temp table sessions_tmp as select * from sessions limit 0;
\copy sessions_tmp from program 'zcat /data/sessions.csv.zip' with(format csv, header);
insert into sessions (select * from sessions_tmp where user_id in (select id from users));
