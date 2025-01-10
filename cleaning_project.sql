-- Data Cleaning
-- 1. Remove Duplicates
-- 2. Standarize the Data
-- 3. Null / blank values
-- 4. Remove unnessary rows/columns

-- 1. Remove Duplicates

SELECT 
    *
FROM
    layoffs;

-- we create 'layoffs_staging' table to not perform changes to raw data (layoffs)
CREATE TABLE layoffs_staging LIKE layoffs;  


insert into layoffs_staging (select * from layoffs);


SELECT 
    *
FROM
    layoffs_staging;

-- we use CTE to create temporary result
with duplicate_CTE as (select *,row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num from layoffs_staging
)

select * from duplicate_CTE where row_num >1;


SELECT 
    *
FROM
    layoffs_staging
WHERE
    company = 'casper';

SELECT 
    *
FROM
    layoffs_staging
WHERE
    company = 'Cazoo';

SELECT 
    *
FROM
    layoffs_staging
WHERE
    company = 'hibob';

-- we create another table `layoffs_staging2` to delete records from `layoffs_staging` (we could delete the records without creating another table)
CREATE TABLE `layoffs_staging2` (
    `company` TEXT,
    `location` TEXT,
    `industry` TEXT,
    `total_laid_off` INT DEFAULT NULL,
    `percentage_laid_off` TEXT,
    `date` TEXT,
    `stage` TEXT,
    `country` TEXT,
    `funds_raised_millions` INT DEFAULT NULL,
    `Row_num` INT
)  ENGINE=INNODB DEFAULT CHARSET=UTF8MB4 COLLATE = UTF8MB4_0900_AI_CI;

    
    
    insert into layoffs_staging2 (select *,row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num from layoffs_staging
) ;

SELECT 
    *
FROM
    layoffs_staging2
WHERE
    Row_num > 1;


DELETE FROM layoffs_staging2 
WHERE
    Row_num > 1;


SELECT 
    *
FROM
    layoffs_staging2;
    
    alter table layoffs_staging2 drop column Row_num;

    
-- 2. Standarize the Data (remove white space and standarize words in capital or small letter)
    
SELECT 
    *
FROM
    layoffs_staging2;


SELECT DISTINCT
    company, TRIM(company)
FROM
    layoffs_staging2;


UPDATE layoffs_staging2 
SET 
    company = TRIM(company);


SELECT DISTINCT
    industry
FROM
    layoffs_staging2
ORDER BY 1;


SELECT 
    *
FROM
    layoffs_staging2
WHERE
    industry LIKE 'Crypto%';

UPDATE layoffs_staging2 
SET 
    industry = 'Crypto'
WHERE
    industry LIKE 'Crypto%';

SELECT DISTINCT
    country
FROM
    layoffs_staging2
WHERE
    country LIKE 'United States%'
ORDER BY 1;


SELECT DISTINCT
    company, TRIM(TRAILING '.' FROM country)
FROM
    layoffs_staging2;

SELECT DISTINCT
    country
FROM
    layoffs_staging2;

SELECT 
    `date`
FROM
    layoffs_staging2;


UPDATE layoffs_staging2 
SET 
    `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

alter table layoffs_staging2 modify column `date` date;


-- 3. remove null and blank 

SELECT 
    *
FROM
    layoffs_staging2
WHERE
    total_laid_off IS NULL
        AND percentage_laid_off IS NULL;
        
SELECT 
    *
FROM
    layoffs_staging2
WHERE
    industry IS NULL OR industry = '';
    
        
UPDATE layoffs_staging2 
SET 
    industry = NULL
WHERE
    industry = '';

    
    
SELECT 
    *
FROM
    layoffs_staging2
WHERE
    company = 'Airbnb';


SELECT 
    *
FROM
    layoffs_staging2 st1
        JOIN
    layoffs_staging2 st2 ON st1.company = st2.company
        AND st1.location = st2.location
WHERE
    (st1.industry IS NULL
        OR st1.industry = '')
        AND st2.industry IS NOT NULL;


SELECT 
    st1.industry, st2.industry
FROM
    layoffs_staging2 st1
        JOIN
    layoffs_staging2 st2 ON st1.company = st2.company
        AND st1.location = st2.location
WHERE
    (st1.industry IS NULL
        OR st1.industry = '')
        AND st2.industry IS NOT NULL;
        
UPDATE layoffs_staging2 
SET 
    industry = NULL
WHERE
    industry = '';
        
        
UPDATE layoffs_staging2 st1
        JOIN
    layoffs_staging2 st2 ON st1.company = st2.company 
SET 
    st1.industry = st2.industry
WHERE
    st1.industry IS NULL
        AND st2.industry IS NOT NULL;
        
SELECT 
    *
FROM
    layoffs_staging2;

