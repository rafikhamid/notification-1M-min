# Notification-1M-min
This is a journey to attempt sending 1 Million notifications/emails per minute. The solution will start as simple as possible, then iterations will follow to detect bottleneck and improve them untill we reach the goal of 1M per minute.

# Functional Requirements
* System need to send 1 Million notification per 1 minute
* Handling Failure : if execution crashes, rerun the application should continue from where it stopped and not re-process the already processed data again.
* Application should require minimum cost of infrastructure

# Non Functional Requirements
* Application need to be designed with (horizontal) scalability in mind

# Technology Stack
* Spring boot
* Postgres

# Database
1. install Postgress
2. create database named 'Notifications'
3. create table named 'Notifications'
4. initialize 'Notification' table with 1 million rows
- Run script : src/main/resources/static/data-generator-1M.sh
```
$ cd src/main/resources/static
$ ./data-generator-1M.sh > data.csv
```
5. Import data to Postgres
```
COPY notifications(id, message)
FROM 'PATH_TO_/src/main/resources/static/data.csv'
DELIMITER ','
CSV HEADER; 
```




# Solution
# PART 1 : MVP simple solution
