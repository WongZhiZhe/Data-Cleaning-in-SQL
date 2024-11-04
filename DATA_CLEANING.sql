-- Data Cleaning

Select * 
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize Data
-- 3. Null values pr Blank values
-- 4. Remove Any Columns

# create backup table to do cleaning
CREATE TABLE layoffs_staging
Like layoffs;

Select *
From layoffs_staging;

INSERT layoffs_staging
Select *
From layoffs;


-- 1. Remove Duplicates -------------------------------------------------------

Select *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date` ) AS row_num
From layoffs_staging;

WITH duplicate_cte AS
(Select *,
ROW_NUMBER() 
OVER( PARTITION BY company, 
industry, total_laid_off, percentage_laid_off, 
`date`, stage, country, funds_raised_millions ) AS row_num
From layoffs_staging)

Select *
From duplicate_cte
WHERE row_num > 1;


Select *
From layoffs_staging
WHERE company = 'Casper';

#Delete can't be worked in mysql here since it is like updating 
WITH duplicate_cte AS
(
Select *,
ROW_NUMBER() 
OVER( PARTITION BY company, 
industry, total_laid_off, percentage_laid_off, 
`date`, stage, country, funds_raised_millions ) AS row_num
From layoffs_staging
)
DELETE
From duplicate_cte
WHERE row_num > 1;

Select *,
ROW_NUMBER() 
OVER( PARTITION BY company, 
industry, total_laid_off, percentage_laid_off, 
`date`, stage, country, funds_raised_millions ) AS row_num
From layoffs_staging;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
Select *,
ROW_NUMBER() 
OVER( PARTITION BY company, 
industry, total_laid_off, percentage_laid_off, 
`date`, stage, country, funds_raised_millions ) AS row_num
From layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;


-- 2. Standardizing Data-----------------------------------------
#find duplicates
SELECT company, trim( company ) trim_co
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

#remove duplicates
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

update layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT `date`,
str_to_date(`date`, '%m/%d/%y')
FROM layoffs_staging2;

# Y is better
SELECT str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

SELECT `date`
from layoffs_staging2;

#cHANGE `DATE` FROM TEXT TO DATE
ALTER TABLE layoffs_staging2
MODIFY column `date` DATE;	

SELECT *
from layoffs_staging2;


-- 3. NULL and Blanks Value ------------------------------------

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

UPDATE layoffs_staging2
SET industry = null
WHERE industry = '';


SELECT  *
from layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
from layoffs_staging2
WHERE company = 'Airbnb';

SELECT t1.industry, t2.industry
FROM layoffs_staging2 T1
JOIN layoffs_staging2 T2
	on t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry is null or t1.industry = '')
and (t2.industry is not null or t2.industry != '');

UPDATE layoffs_staging2 T1
JOIN layoffs_staging2 T2
	on t1.company = t2.company
set t1.industry = t2.industry
WHERE t1.industry is null 
and t2.industry is not null;

# only one left null
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

SELECT *
FROM layoffs_staging2;

#Useless data? Delete!!
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- delete the support column or useless column
SELECT *
FROM layoffs_staging2;
