
select * from global_layoffs.layoffs;
#The data set. Returns all columns of the import, layoffs.
#The columns: company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions

/*
Steps in the Data Cleaning:
1. Remove Duplicate Entries
2. Standardized the Data
3. If possible, populate null and/or blank values
4. Remove columns that are not needed
*/





-- Remove Duplicate Entries --

#It's best practice to keep original data
#create a copy of the layoffs Table
create table global_layoffs.layoffs2 like global_layoffs.layoffs;

insert global_layoffs.layoffs2
select * from global_layoffs.layoffs;
#A copy of the 'layoffs' Table called 'layoffs2' is now up


#Let us create a query to count how many copies of an entry has in the data set using row_number() window function
#To really check if the entry is unique we have to return all columns of the data set as well as partition the row_number() function with these columns
select *,
	   row_number() over (partition by company, location, industry, 
                                       total_laid_off, percentage_laid_off, `date`, 
                                       stage, country, funds_raised_millions) as row_num
from global_layoffs.layoffs;
#`date` is inside back ticks because writing just date means a different thing in mysql since it's a keyword or something

#Duplicates have a row_num that is greater than 1.
#To return these duplicates, Use WHERE statement and place the previous query inside a subquery
#Every derived table must have alias
select *
from(select company, location, industry,
		    total_laid_off, percentage_laid_off, `date`,
            stage, country, funds_raised_millions,
		    row_number() over (partition by company, location, industry, 
										   total_laid_off, percentage_laid_off, `date`, 
										   stage, country, funds_raised_millions) as row_num
	from global_layoffs.layoffs) as duplicates #'duplicates' is the derived table alias
where row_num > 1;

#add row_number() as a new column to table 'layoffs2' using alter
alter table global_layoffs.layoffs2 add column row_num int;

select * from global_layoffs.layoffs2;

#Create a copy of layoffs2 so we can continue working on this copy and delete the duplicates off of this copied table

/*
Another way to create table (non-query):
-right click on layoffs2 table
-Select Copy to Clipboard (Create Statement) and paste
*/

CREATE TABLE `layoffs3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  row_num int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

#insert `layoffs2' values into this newly created table
insert into global_layoffs.layoffs3
select company, location, industry, 
	   total_laid_off, percentage_laid_off, `date`, 
	   stage, country, funds_raised_millions,
       row_number() over (partition by company, location, industry, 
									   total_laid_off, percentage_laid_off, `date`, 
									   stage, country, funds_raised_millions)
from global_layoffs.layoffs2;

select * from global_layoffs.layoffs3;

#Duplicates are those that have more than 1 row_num
delete
from global_layoffs.layoffs3
where row_num > 1;





-- Standardized the Data --

#We're working on 'layoffs3' table now
select * from global_layoffs.layoffs3;
#Let's take a look at each column at a time and find what to standardize

#COMPANY
#Issue: spaces left and right
#Solution: trim() function

select distinct company
from global_layoffs.layoffs3
order by company asc;

update global_layoffs.layoffs3
set company = trim(company);

#LOCATION
#Issue: Some countires with special case letters are displayed wrong
#Solution: Use WHERE Statement with LIKE
select distinct location
from global_layoffs.layoffs3
order by location asc;

update global_layoffs.layoffs3
set location = 'Dusseldorf'
where location like 'DÃ¼sseldorf';

update global_layoffs.layoffs3
set location = 'Florianópolis'
where location like 'FlorianÃ³polis';

update global_layoffs.layoffs3
set location = 'Malmo'
where location like 'MalmÃ¶';

#INDUSTRY
#Issue: Null Values. 'Crypto', 'Crypto Currency' & 'CryptoCurrency' all fall in the same industry, Crypto.
#Solution: Use WHERE Sttement with is null.Use WHERE Statement with LIKE and wildcard '%'
select distinct industry
from global_layoffs.layoffs3
order by industry asc;

-- update global_layoffs.layoffs3
-- set industry = ''
-- where industry is null;

update global_layoffs.layoffs3
set industry = 'Crypto'
where industry like 'Crypto%';

#DATE
#Issue: Date is not in date format and is a string data type not in date data type
#Solution: Use str_to_date() following the format of '%m/%d/%Y', standard date format in MySQL (it is in yyyy-mm-dd). We can use alter & modify to convert date data type
select distinct `date`
from global_layoffs.layoffs3;

update global_layoffs.layoffs3
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table global_layoffs.layoffs3
modify column `date` date; 
#Go to Schemas > layoffs3 > columns > date and check to see if date has now a date data type

#COUNTRY
#Issue: Some countries have a dot in the end
#Solution: Use trim(trailing '.' from country) and Where Statement with like
select distinct country
from global_layoffs.layoffs3
order by country asc;

update global_layoffs.layoffs3
set country = trim(trailing '.' from country)
where country like 'United States%';

#Self Join
#some companies have entries with incomplete details (missing/null values)
select * from global_layoffs.layoffs3
where company = 'Airbnb';
#The other 'Airbnb' result has 'Travel' value missing under industry column on its row
#A solution to populate the missing value is using self join  
select t1.industry, t2.industry
from global_layoffs.layoffs3 as t1
join global_layoffs.layoffs3 as t2
	on t1.company = t2.company
    and t1.location = t2.location;

#apply changes from the self join result via updating
update global_layoffs.layoffs3 as t1
join global_layoffs.layoffs3 as t2
	on t1.company = t2.company
    and t1.location = t2.location
set t1.industry = t2.industry
where t1.industry is null
  and t2.industry is not null;
  
#check if there's no longer null values in industry column
select industry
from global_layoffs.layoffs3
where industry is null
   or industry like '';





-- If possible, populate null and/or blank values --
#there are columns with null values that we cannot populate because we lack data especially 





-- Remove columns that are not needed --
#total_laid_off and percentage_laid_off have null values. We got to delete them
select *
from global_layoffs.layoffs3
where total_laid_off is null
  and percentage_laid_off is null;

delete
from global_layoffs.layoffs3
where total_laid_off is null
  and percentage_laid_off is null;
  
select total_laid_off, percentage_laid_off
from global_layoffs.layoffs3;
  

#row_num is no longer needed. Delete it
select * from global_layoffs.layoffs3;

alter table global_layoffs.layoffs3
drop column row_num;


