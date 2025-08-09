--Problem
/**
You’re given two tables: Employees and Attendence_Logs. 
The Attendence_Logs table contains timestamped punch-in and punch-out records for each employee, along with a punch_type field (IN or OUT).
Calculate daily attendance metrics for each employee, including:

1. First punch-in time

2. Last punch-out time

3. Total hours worked that day
 
You can use the data provided after creation of the tables.
**/


create table Employees (
	emp_id int primary key,
	emp_name varchar(50) not null)

create table Attendence_Logs (
	log_id int identity(1,1) primary key,
	emp_id int references Employees(emp_id),
	punch_time datetime not null,
	punch_type varchar(10) check(punch_type in ('IN', 'OUT')),
	work_date as cast(punch_time as date) persisted )

--Insert employees
INSERT INTO Employees (emp_id, emp_name) VALUES
(101, 'Alice'),
(102, 'Bob'),
(103, 'Charlie');

--Insert attendance logs
INSERT INTO Attendence_Logs (emp_id, punch_time, punch_type) VALUES
(101, '2025-08-08 09:00:00', 'IN'),
(101, '2025-08-08 17:00:00', 'OUT'),
(102, '2025-08-08 08:30:00', 'IN'),
(102, '2025-08-08 16:30:00', 'OUT'),
(103, '2025-08-08 10:00:00', 'IN'),
(103, '2025-08-08 18:00:00', 'OUT');


with PunchDuration as (
select e.emp_name, a.emp_id, a.work_date , a.punch_time, a.punch_type,
	   lag(a.punch_time) over (partition by a.emp_id, a.work_date order by a.punch_time) as previous_punch
	   from Attendence_Logs a join Employees e on a.emp_id = e.emp_id) 
select emp_name , emp_id, work_date,   
	   min(case when punch_type = 'IN' then punch_time end) as First_in,
	   max(case when punch_type = 'OUT' then punch_time end) as Last_Out,
	   round(sum(case when punch_type = 'OUT' and previous_punch is not null then datediff(second, previous_punch, punch_time) / 3600.0 
	   else 0
	   end), 2) as Hours_at_work
	   from PunchDuration
group by emp_id, emp_name, work_date
order by work_date, emp_name
 
GO

--UpdatedLogic
/**
Instead of relying on punch_type, we’ll:

Assign a sequence number to each punch per employee per day.

Pair odd-numbered punches with the next even-numbered punch.

Calculate time spent between each odd-even pair.

Sum those durations to get total hours at work.
**/
with SequencedPunches as (
select a.emp_id, e.emp_name, cast(a.punch_time as date) as work_date, a.punch_time,
	   ROW_NUMBER() over(partition by a.emp_id, cast(a.punch_time as Date)
	   order by a.punch_time)
	   as punch_seq from Attendence_Logs a
	   join Employees e on a.emp_id = e.emp_id),
	PunchPairs as (
select p1.emp_id, p1.emp_name, p1.work_date, p1.punch_time as punch_in, p2.punch_time as punch_out,
	   DATEDIFF(second, p1.punch_time, p2.punch_time) / 3600.0 as Duration_Hours
	   from SequencedPunches p1 join SequencedPunches p2 on p1.emp_id = p2.emp_id
	   and p1.work_date = p2.work_date
	   and p2.punch_seq = p1.punch_seq + 1
	   where p1.punch_seq % 2 = 1 )
select emp_name, emp_id, work_date, min(punch_in) as First_in, max(punch_out) as Last_out, round(sum(Duration_Hours),2) as Hours_at_Work
from PunchPairs
group by emp_id, emp_name, work_date
order by work_date, emp_name
