-- ===========================
-- SECTION 1: Data Ingestion
-- This section imports the necessary tables from /private/tmp/
-- ===========================

DROP SCHEMA IF EXISTS young_company;

SET foreign_key_checks = 0;

CREATE SCHEMA IF NOT EXISTS young_company;
USE young_company;

DROP TABLE IF EXISTS badges;
CREATE TABLE badges (
    RowNumber INT, -- VARCHAR(10)
    CompanyId INT,
    CompanyBadge VARCHAR(50)
);
TRUNCATE badges;
LOAD DATA INFILE '/private/tmp/badges.csv' 
INTO TABLE badges
FIELDS TERMINATED BY ','
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES -- to remove the header of the csv
(@RowNumber, @CompanyId, @CompanyBadge)

SET
RowNumber = NULLIF(@RowNumber, ''),
CompanyId = NULLIF(@CompanyId, ''),
CompanyBadge = NULLIF(@CompanyBadge, '');

-- #########################################

CREATE SCHEMA IF NOT EXISTS young_company;
USE young_company;

DROP TABLE IF EXISTS company;
CREATE TABLE company (
    RowNumber INT, -- VARCHAR(10)
    CompanyId INT PRIMARY KEY,
    CompanyName VARCHAR(50),
    slug VARCHAR(50) UNIQUE,
    website VARCHAR (200),
    smallLogoUrl VARCHAR(200),
    oneLiner VARCHAR(100),
    longDescription VARCHAR (5000),
    teamSize INT,
    url VARCHAR (200),
    batch VARCHAR (50),
    status VARCHAR (50)
);

TRUNCATE TABLE company;
LOAD DATA INFILE '/private/tmp/companies.csv'
INTO TABLE company
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(@RowNumber, @CompanyId, @CompanyName, @slug, @website, @smallLogoUrl, @oneLiner, @longDescription, @teamSize, @url, @batch, @status)
SET
  RowNumber = NULLIF(@RowNumber, ''), 
  CompanyId = NULLIF(@CompanyId, ''), 
  CompanyName = NULLIF(@CompanyName, ''), 
  slug = NULLIF(@slug, ''), 
  website = NULLIF(@website, ''), 
  smallLogoUrl = NULLIF(@smallLogoUrl, ''), 
  oneLiner = NULLIF(@oneLiner, ''), 
  longDescription = NULLIF(@longDescription, ''), 
  teamSize = NULLIF(@teamSize, ''), 
  url = NULLIF(@url, ''),
  batch = NULLIF(@batch, ''), 
  status = NULLIF(@status, ''); 

-- #########################################

CREATE SCHEMA IF NOT EXISTS young_company;
USE young_company;

DROP TABLE IF EXISTS founders;
CREATE TABLE founders (
    first_name VARCHAR(30),
    last_name VARCHAR (40),
    hnid VARCHAR (15) PRIMARY KEY,
    avartar_thumb VARCHAR(200),
    current_company VARCHAR(100),
    current_title VARCHAR (200),
    company_slug VARCHAR(100),
    -- top_company VARCHAR (5) -- BOOLEAN
    top_company TINYINT(1),
    FOREIGN KEY (company_slug) REFERENCES company(slug) ON DELETE CASCADE
);

TRUNCATE TABLE founders;
LOAD DATA INFILE '/private/tmp/founders.csv'
INTO TABLE founders
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(@first_name, @last_name, @hnid, @avartar_thumb, @current_company, @current_title, @company_slug, @top_company)
SET
  first_name = NULLIF(@first_name, ''), 
  last_name = NULLIF(@last_name, ''), 
  hnid = NULLIF(@hnid, ''), 
  avartar_thumb = NULLIF(@avartar_thumb, ''), 
  current_company = NULLIF(@current_company, ''), 
  current_title = NULLIF(@current_title, ''), 
  company_slug = NULLIF(@company_slug, ''), 
  top_company = CASE 
                    WHEN UPPER(TRIM(@top_company)) LIKE 'TRUE%' THEN 1 
                    WHEN UPPER(TRIM(@top_company)) LIKE 'FALSE%' THEN 0 
                    ELSE NULL 
                END
-- top_company = NULLIF(@top_company, '')
;
#########################################

CREATE SCHEMA IF NOT EXISTS young_company;
USE young_company;

DROP TABLE IF EXISTS industries;
CREATE TABLE industries (
    RowNumber INT,
    id INT,
    industry VARCHAR(200)
);

TRUNCATE TABLE industries;
LOAD DATA INFILE '/private/tmp/industries.csv'
INTO TABLE industries
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(@RowNumber, @id, @industry)
SET
    RowNumber = NULLIF(@RowNumber, ''),  
    id = NULLIF(@id, ''),                
    industry = LEFT(NULLIF(@industry, ''), 200);  

  
-- #########################################

CREATE SCHEMA IF NOT EXISTS young_company;
USE young_company;

DROP TABLE IF EXISTS prior_companies;
CREATE TABLE prior_companies (
    hnid VARCHAR(15),
    company VARCHAR (300),
    FOREIGN KEY (hnid) REFERENCES founders(hnid) ON DELETE CASCADE
);

TRUNCATE TABLE prior_companies;
LOAD DATA INFILE '/private/tmp/prior_companies.csv'
INTO TABLE prior_companies
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(@hnid, @company)
SET
  hnid = NULLIF(@hnid, ''),
  company = LEFT(NULLIF(@company, ''), 300);
  
-- #########################################

CREATE SCHEMA IF NOT EXISTS young_company;
USE young_company;

DROP TABLE IF EXISTS regions;
CREATE TABLE regions (
	RowNumber INT,
    id INT,
    region VARCHAR (100),
    country VARCHAR (100),
    address VARCHAR (100),
    FOREIGN KEY (id) REFERENCES company(CompanyId) ON DELETE CASCADE
);

TRUNCATE TABLE regions;
LOAD DATA INFILE '/private/tmp/regions.csv'
INTO TABLE regions
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(@RowNumber, @id, @region, @country, @address)
SET
  RowNumber = NULLIF(@RowNumber, ''), 
  id = NULLIF(@id, ''), 
  region = NULLIF(@region, ''),
  country = NULLIF(@country, ''),
  address = LEFT(NULLIF(@address, ''), 500);
  
-- #########################################

CREATE SCHEMA IF NOT EXISTS young_company;
USE young_company;

DROP TABLE IF EXISTS schools;
CREATE TABLE schools (
	hnid VARCHAR(15),
    school VARCHAR(300),
    field_of_study VARCHAR (300),
    year INT
);

TRUNCATE TABLE schools;
LOAD DATA INFILE '/private/tmp/schools.csv'
INTO TABLE schools
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(@hnid, @school, @field_of_study, @year)
SET
  hnid = NULLIF(@hnid, ''),
  school = NULLIF(@school, ''),
  field_of_study = NULLIF(@field_of_study, ''),
  year = NULLIF(@year, '')
  ;
  
-- #########################################

CREATE SCHEMA IF NOT EXISTS young_company;
USE young_company;

DROP TABLE IF EXISTS tags;
CREATE TABLE tags (
	RowNumber INT,
    id INT,
    tag VARCHAR (1000)
);

TRUNCATE TABLE tags;
LOAD DATA INFILE '/private/tmp/tags.csv'
INTO TABLE tags
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(@RowNumber, @id, @tag)
SET
  RowNumber = NULLIF(@RowNumber, ''),
  id = NULLIF(@id, ''),
  tag = LEFT(NULLIF(@tag, ''), 1000);

SET foreign_key_checks = 1;

-- #########################################


CREATE SCHEMA IF NOT EXISTS young_company;
USE young_company;

DROP TABLE IF EXISTS qs_rank;
CREATE TABLE qs_rank (
	RowNumber INT,
    rank_2024 INT,
    institution VARCHAR (100),
    location VARCHAR (100)
);

TRUNCATE TABLE qs_rank;
LOAD DATA INFILE '/private/tmp/qs_rank_2024.csv'
INTO TABLE qs_rank
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@RowNumber, @rank_2024, @institution, @location)
SET
  RowNumber = NULLIF(@RowNumber, ''),
  rank_2024 = NULLIF(@rank_2024, ''),
  institution = LEFT(NULLIF(@institution, ''), 100),
  location = LEFT(NULLIF(@location, ''), 100);

SET foreign_key_checks = 1;

SET foreign_key_checks = 0;

CREATE SCHEMA IF NOT EXISTS young_company;
USE young_company;

DROP TABLE IF EXISTS program_category;
CREATE TABLE program_category (
	RowNumber INT,
    program_name VARCHAR (100),
    program_category VARCHAR (100)
);

TRUNCATE TABLE program_category;
LOAD DATA INFILE '/private/tmp/mapping_program_category.csv'
INTO TABLE program_category
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@RowNumber, @program_name, @program_category)
SET
  RowNumber = NULLIF(@RowNumber, ''),
  program_name = LEFT(NULLIF(@program_name, ''), 100),
  program_category = LEFT(NULLIF(@program_category, ''), 100);




-- ===========================
-- SECTION 2: Data Normalization
-- ===========================

USE young_company;

DROP PROCEDURE IF EXISTS AlterColumnNames;
DELIMITER //
CREATE PROCEDURE AlterColumnNames()
BEGIN
    IF EXISTS (SELECT * 
               FROM information_schema.COLUMNS 
               WHERE TABLE_NAME = 'industries' AND COLUMN_NAME = 'id') THEN
        ALTER TABLE industries CHANGE id CompanyId INT;
    END IF;

    IF EXISTS (SELECT * 
               FROM information_schema.COLUMNS 
               WHERE TABLE_NAME = 'regions' AND COLUMN_NAME = 'id') THEN
        ALTER TABLE regions CHANGE id CompanyId INT;
    END IF;

    IF EXISTS (SELECT * 
               FROM information_schema.COLUMNS 
               WHERE TABLE_NAME = 'tags' AND COLUMN_NAME = 'id') THEN
        ALTER TABLE tags CHANGE id CompanyId INT;
    END IF;
END //

DELIMITER ;

CALL AlterColumnNames();

-- normalization 

SET foreign_key_checks = 0;

DROP TABLE IF EXISTS institutions;
CREATE TABLE institutions (
    institution_id INT AUTO_INCREMENT PRIMARY KEY,
    institution_name VARCHAR(255)
);

DROP TABLE IF EXISTS program;
CREATE TABLE program (
    program_id INT AUTO_INCREMENT PRIMARY KEY,
    program_name VARCHAR(255)
);

DROP TABLE IF EXISTS enrollment;
CREATE TABLE enrollment (
    enrollment_id INT AUTO_INCREMENT PRIMARY KEY,
    institution_id INT,
    program_id INT,
    year INT,
    hnid VARCHAR(15),
    CONSTRAINT fk_institution FOREIGN KEY (institution_id) REFERENCES institutions(institution_id) ON DELETE CASCADE,
    CONSTRAINT fk_program FOREIGN KEY (program_id) REFERENCES program(program_id) ON DELETE CASCADE,
    CONSTRAINT fk_hnid FOREIGN KEY (hnid) REFERENCES founders(hnid) ON DELETE CASCADE
);

SET foreign_key_checks = 1;

INSERT INTO institutions (institution_name)
SELECT DISTINCT school
FROM schools;

INSERT INTO program (program_name)
SELECT DISTINCT field_of_study
FROM schools;

INSERT INTO enrollment (institution_id, program_id, year, hnid)
SELECT i.institution_id, p.program_id, o.year, o.hnid
FROM schools o
JOIN institutions i ON o.school = i.institution_name
JOIN program p ON o.field_of_study = p.program_name;
DROP table if exists schools;

SET foreign_key_checks = 0;

DROP TABLE IF EXISTS badge; 
CREATE TABLE badge (
    BadgeId INT AUTO_INCREMENT PRIMARY KEY,  
    Badge VARCHAR(50) UNIQUE  
);
INSERT INTO badge (Badge)
SELECT DISTINCT CompanyBadge
FROM badges;


DROP TABLE IF EXISTS companybadge;
CREATE TABLE companybadge (
    CompanyBadgeId INT AUTO_INCREMENT PRIMARY KEY,  
    CompanyId INT,
    BadgeId INT,
    CONSTRAINT fk_company FOREIGN KEY (CompanyId) REFERENCES company(CompanyId) ON DELETE CASCADE,
    CONSTRAINT fk_badge FOREIGN KEY (BadgeId) REFERENCES badge(BadgeId) ON DELETE CASCADE
);

INSERT INTO companybadge (CompanyId, BadgeId)
SELECT b.CompanyId, bd.BadgeId 
FROM badges AS b
JOIN badge AS bd ON b.CompanyBadge = bd.Badge; 
SET foreign_key_checks = 1;
DROP TABLE IF EXISTS badges; 
SET foreign_key_checks = 0;

DROP TABLE IF EXISTS industry; 
CREATE TABLE industry (
    IndustryId INT AUTO_INCREMENT PRIMARY KEY,  
    IndustryName VARCHAR(200) UNIQUE  
);
INSERT INTO industry (IndustryName)
SELECT DISTINCT industry
FROM industries;

DROP TABLE IF EXISTS company_industry;  

CREATE TABLE IF NOT EXISTS company_industry (
    CompanyId INT,
    IndustryId INT,
    PRIMARY KEY (CompanyId, IndustryId), 
    CONSTRAINT fk_company2 FOREIGN KEY (CompanyId) REFERENCES company(CompanyId) ON DELETE CASCADE,
    CONSTRAINT fk_industry FOREIGN KEY (IndustryId) REFERENCES industry(IndustryId) ON DELETE CASCADE
);

INSERT INTO company_industry (CompanyId, IndustryId)
SELECT i.CompanyId, ind.IndustryId 
FROM industries AS i
JOIN industry AS ind ON i.industry = ind.IndustryName;
SET foreign_key_checks = 1;

DROP TABLE IF EXISTS industries; 


SET foreign_key_checks = 0;

DROP TABLE IF EXISTS tag; 
CREATE TABLE tag (
    tagId INT AUTO_INCREMENT PRIMARY KEY,  
    tagName VARCHAR(200) UNIQUE  
);
INSERT INTO tag (tagName)
SELECT DISTINCT tag
FROM tags;

DROP TABLE IF EXISTS company_tag;

CREATE TABLE company_tag (
    CompanyId INT,
    tagId INT,
    PRIMARY KEY (CompanyId, tagId),  
    CONSTRAINT fk_company3 FOREIGN KEY (CompanyId) REFERENCES company(CompanyId) ON DELETE CASCADE,
    CONSTRAINT fk_tag FOREIGN KEY (tagId) REFERENCES tag(tagId) ON DELETE CASCADE
);

INSERT INTO company_tag (CompanyId, tagId)
SELECT t.CompanyId, tg.tagId 
FROM tags AS t
JOIN tag AS tg ON t.tag = tg.tagName;

SET foreign_key_checks = 1;

DROP TABLE IF EXISTS tags;


SET SQL_SAFE_UPDATES = 0;

ALTER TABLE program ADD COLUMN program_category VARCHAR(255);
UPDATE program SET program_category = NULL;
UPDATE program AS p
JOIN program_category AS pc ON p.program_name = pc.program_name
SET p.program_category = pc.program_category;
DROP TABLE program_category;
SET SQL_SAFE_UPDATES = 1;

SET foreign_key_checks = 1;

-- ============================================================================================================
-- SECTION 3: QS-Ranking Mapping
-- This section maps the different university names to the QS standard name with cosine similarity method done in Python
-- ============================================================================================================

USE young_company;
SET SQL_SAFE_UPDATES = 0;
UPDATE institutions SET institution_name = 'High School' WHERE LOWER(institution_name) LIKE '%high school%';

INSERT INTO qs_rank VALUES (1499, 1402, 'High School', "");

UPDATE young_company.institutions SET institution_name = 'The London School of Economics and Political Science' WHERE institution_name = 'London School of Economics and Political Science';
UPDATE young_company.institutions SET institution_name = 'Indian Institute of Technology Guwahati' WHERE institution_name = 'Indian Institute of Technology, Guwahati';
UPDATE young_company.institutions SET institution_name = 'University of Minnesota Twin Cities' WHERE institution_name = 'University of Minnesota';
UPDATE young_company.institutions SET institution_name = 'Harvard University' WHERE institution_name = 'Harvard Business School Online';
UPDATE young_company.institutions SET institution_name = 'Universidad Iberoamericana IBERO' WHERE institution_name = 'Universidad Iberoamericana';
UPDATE young_company.institutions SET institution_name = 'Stanford University' WHERE institution_name = 'Stanford University Graduate School of Business';
UPDATE young_company.institutions SET institution_name = 'University of California, Berkeley' WHERE institution_name = 'UC Berkeley College of Engineering';
UPDATE young_company.institutions SET institution_name = 'Indian Institute of Technology Bombay' WHERE institution_name = 'IIT Bombay';
UPDATE young_company.institutions SET institution_name = 'Birla Institute of Technology and Science, Pilani' WHERE institution_name = 'Birla Institute of Technology';
UPDATE young_company.institutions SET institution_name = 'Harvard University' WHERE institution_name = 'Harvard Business School';
UPDATE young_company.institutions SET institution_name = 'Indian Institute of Technology Delhi' WHERE institution_name = 'IIT Delhi';
UPDATE young_company.institutions SET institution_name = 'National Institute of Technology Tiruchirappalli' WHERE institution_name = 'National Institute of Technology, Kurukshetra';
UPDATE young_company.institutions SET institution_name = 'Carnegie Mellon University' WHERE institution_name = 'Carnegie Mellon University College of Engineering';
UPDATE young_company.institutions SET institution_name = 'Western University' WHERE institution_name = 'Ivey Business School at Western University';
UPDATE young_company.institutions SET institution_name = 'University of British Columbia' WHERE institution_name = 'The University of British Columbia';
UPDATE young_company.institutions SET institution_name = 'École Normale Supérieure de Lyon' WHERE institution_name = 'Ecole normale supérieure';
UPDATE young_company.institutions SET institution_name = 'Indian Institute of Technology Kanpur' WHERE institution_name = 'IIT Kanpur';
UPDATE young_company.institutions SET institution_name = 'Pondicherry University' WHERE institution_name = 'Pondicherry Engineering College';
UPDATE young_company.institutions SET institution_name = 'Stanford University' WHERE institution_name = 'Stanford Continuing Studies';
UPDATE young_company.institutions SET institution_name = 'University of Nottingham' WHERE institution_name = 'Nottingham';
UPDATE young_company.institutions SET institution_name = 'Massachusetts Institute of Technology ' WHERE institution_name = 'Massachusetts Institute of Technology';
UPDATE young_company.institutions SET institution_name = 'Pennsylvania State University' WHERE institution_name = 'Penn State University';
UPDATE young_company.institutions SET institution_name = 'George Washington University' WHERE institution_name = 'The George Washington University';
UPDATE young_company.institutions SET institution_name = 'Harvard University' WHERE institution_name = 'Harvard Law School';
UPDATE young_company.institutions SET institution_name = 'Cornell University' WHERE institution_name = 'Cornell University College of Engineering';
UPDATE young_company.institutions SET institution_name = 'Harvard University' WHERE institution_name = 'Harvard Graduate School of Arts and Sciences';
UPDATE young_company.institutions SET institution_name = 'University of Delhi' WHERE institution_name = 'Delhi Public School, R.K.Puram, New Delhi';
UPDATE young_company.institutions SET institution_name = 'National Institute of Technology Tiruchirappalli' WHERE institution_name = 'National Institute of Technology, Tiruchirappalli';
UPDATE young_company.institutions SET institution_name = 'Universidad del Norte ' WHERE institution_name = 'Universidad del Norte';
UPDATE young_company.institutions SET institution_name = 'Université de Montpellier' WHERE institution_name = 'Montpellier Business School';
UPDATE young_company.institutions SET institution_name = 'Harvard University' WHERE institution_name = 'Harvard University Kennedy School of Government';
UPDATE young_company.institutions SET institution_name = 'Banaras Hindu University' WHERE institution_name = 'Banha University';
UPDATE young_company.institutions SET institution_name = 'Massachusetts Institute of Technology ' WHERE institution_name = 'Massachusetts Institute of Techonology';
UPDATE young_company.institutions SET institution_name = 'Indian Institute of Technology Madras' WHERE institution_name = 'IIT Madras';
UPDATE young_company.institutions SET institution_name = 'Northumbria University at Newcastle' WHERE institution_name = 'Northumbria University';
UPDATE young_company.institutions SET institution_name = 'University of Delhi' WHERE institution_name = 'Delhi Public School - R. K. Puram';
UPDATE young_company.institutions SET institution_name = 'Northumbria University at Newcastle' WHERE institution_name = 'Northumbria University (UK)';
UPDATE young_company.institutions SET institution_name = 'University of Mumbai' WHERE institution_name = 'St. Xavier''s College, Mumbai';
UPDATE young_company.institutions SET institution_name = 'Rutgers University–Newark' WHERE institution_name = 'Rutgers University';
UPDATE young_company.institutions SET institution_name = 'Birla Institute of Technology and Science, Pilani' WHERE institution_name = 'Birla Institute of Technology and Science, Pilani (BITS Pilani)';
UPDATE young_company.institutions SET institution_name = 'Indian Institute of Technology Kharagpur' WHERE institution_name = 'IIT Kharagpur';
UPDATE young_company.institutions SET institution_name = 'University of Massachusetts Amherst' WHERE institution_name = 'Amherst College';
UPDATE young_company.institutions SET institution_name = 'Damascus University' WHERE institution_name = 'Aleppo University';
UPDATE young_company.institutions SET institution_name = 'Jawaharlal Nehru University' WHERE institution_name = 'Mahatma Gandhi University';
UPDATE young_company.institutions SET institution_name = 'Universidade Federal de Minas Gerais      ' WHERE institution_name = 'Universidade Federal de Minas Gerais';
UPDATE young_company.institutions SET institution_name = 'Universidade Federal de Minas Gerais      ' WHERE institution_name = 'Instituto Federal de Educação, Ciência e Tecnologia de Minas Gerais - IFMG';
UPDATE young_company.institutions SET institution_name = 'Università Cattolica del Sacro Cuore' WHERE institution_name = 'Università Cattolica del Sacro Cuore';
UPDATE young_company.institutions SET institution_name = 'University of California, Berkeley' WHERE institution_name = 'University of California, Berkeley, Haas School of Business';
UPDATE young_company.institutions SET institution_name = 'Universidad de Talca' WHERE institution_name = 'Universidad Francisco de Vitoria';
UPDATE young_company.institutions SET institution_name = 'Universidade de São Paulo' WHERE institution_name = 'Graded School of São Paulo';
UPDATE young_company.institutions SET institution_name = 'University of Maribor' WHERE institution_name = 'Univerza v Mariboru';
UPDATE young_company.institutions SET institution_name = 'Ludwig-Maximilians-Universität München' WHERE institution_name = 'Ludwig Maximilian University of Munich';
UPDATE young_company.institutions SET institution_name = 'KTH Royal Institute of Technology ' WHERE institution_name = 'KTH Royal Institute of Technology';
UPDATE young_company.institutions SET institution_name = 'Sciences Po ' WHERE institution_name = 'Sciences Po';
UPDATE young_company.institutions SET institution_name = 'Linköping University' WHERE institution_name = 'Linköping University (Sweden)';
UPDATE young_company.institutions SET institution_name = 'University of Brighton' WHERE institution_name = 'Brighton Secondary College';
UPDATE young_company.institutions SET institution_name = 'Duke University' WHERE institution_name = 'Duke Medical School';
UPDATE young_company.institutions SET institution_name = 'Universidad Nacional Autónoma de México' WHERE institution_name = 'Universidad Nacional Autonoma de Mexico (UNAM)';
UPDATE young_company.institutions SET institution_name = 'Cornell University' WHERE institution_name = 'Cornell University - Johnson Graduate School of Management';
UPDATE young_company.institutions SET institution_name = 'Bilkent University' WHERE institution_name = 'Bilkent Üniversitesi / Bilkent University';
UPDATE young_company.institutions SET institution_name = 'Indiana State University' WHERE institution_name = 'Indiana University';
UPDATE young_company.institutions SET institution_name = 'Yeshiva University' WHERE institution_name = 'Benjamin N. Cardozo School of Law, Yeshiva University';
UPDATE young_company.institutions SET institution_name = 'University of San Diego' WHERE institution_name = 'College of San Mateo';
UPDATE young_company.institutions SET institution_name = 'Northwest Agriculture and Forestry University' WHERE institution_name = 'Northeast Agricultural University';
UPDATE young_company.institutions SET institution_name = 'University of Navarra' WHERE institution_name = 'Universida de Navarra';
UPDATE young_company.institutions SET institution_name = 'Harvard University' WHERE institution_name = 'Harvard University Graduate School of Education';
UPDATE young_company.institutions SET institution_name = 'Universidad Tecnológica de la Habana José Antonio Echeverría, Cujae' WHERE institution_name = 'Universidade Estadual Paulista Júlio de Mesquita Filho';
UPDATE young_company.institutions SET institution_name = 'Jamia Hamdard' WHERE institution_name = 'Jamia hamdard';
UPDATE young_company.institutions SET institution_name = 'The Hebrew University of Jerusalem' WHERE institution_name = 'The Hebrew University';
UPDATE young_company.institutions SET institution_name = 'The University of Sydney' WHERE institution_name = 'University of Sydney';
UPDATE young_company.institutions SET institution_name = 'Yale University' WHERE institution_name = 'Yale School of Management';
UPDATE young_company.institutions SET institution_name = 'University of California, Irvine' WHERE institution_name = 'UC Irvine Heath';
UPDATE young_company.institutions SET institution_name = 'Chalmers University of Technology' WHERE institution_name = 'Chalmers University';
UPDATE young_company.institutions SET institution_name = 'Universitas Indonesia' WHERE institution_name = 'University of Indonesia';
UPDATE young_company.institutions SET institution_name = 'Samara National Research University' WHERE institution_name = 'Samara State Aerospace University';
UPDATE young_company.institutions SET institution_name = 'Universidad del Valle' WHERE institution_name = 'Universidad del Valle (CO)';
UPDATE young_company.institutions SET institution_name = 'Universidad del Pacífico' WHERE institution_name = 'Universidad del Pacífico (PE)';
UPDATE young_company.institutions SET institution_name = 'The University of Tennessee, Knoxville' WHERE institution_name = 'Tennessee State University';
UPDATE young_company.institutions SET institution_name = 'University of International Business and Economics' WHERE institution_name = 'Lazaridis School of Business & Economics at Wilfrid Laurier University';
UPDATE young_company.institutions SET institution_name = 'University of Delhi' WHERE institution_name = 'Delhi Private School, Sharjah';
UPDATE young_company.institutions SET institution_name = 'University of Denver' WHERE institution_name = 'Community College of Denver';
UPDATE young_company.institutions SET institution_name = 'University of Delhi' WHERE institution_name = 'Delhi University';
UPDATE young_company.institutions SET institution_name = 'Harvard University' WHERE institution_name = 'Harvard College';
UPDATE young_company.institutions SET institution_name = 'École Normale Supérieure de Lyon' WHERE institution_name = 'Ecole Centrale de Lyon';
UPDATE young_company.institutions SET institution_name = 'University of Delhi' WHERE institution_name = 'Delhi College of Engineering';
UPDATE young_company.institutions SET institution_name = 'University of Basel' WHERE institution_name = 'International School Basel, Switzerland';
UPDATE young_company.institutions SET institution_name = 'University of Delhi' WHERE institution_name = 'Delhi Public School, Vasant Kunj, New Delhi';
UPDATE young_company.institutions SET institution_name = 'University of Colorado, Denver ' WHERE institution_name = 'University of Colorado Denver';
UPDATE young_company.institutions SET institution_name = 'Stanford University' WHERE institution_name = 'Stanford Law School';
UPDATE young_company.institutions SET institution_name = 'Alexandria University ' WHERE institution_name = 'Alexandria University';
UPDATE young_company.institutions SET institution_name = 'University of Nebraska - Lincoln' WHERE institution_name = 'University of Nebraska at Omaha';
UPDATE young_company.institutions SET institution_name = 'Imperial College London' WHERE institution_name = 'Imperial College Business School';
UPDATE young_company.institutions SET institution_name = 'Stellenbosch University' WHERE institution_name = 'University of Stellenbosch';
UPDATE young_company.institutions SET institution_name = 'University of Hyderabad' WHERE institution_name = 'The Hyderabad Public School';
UPDATE young_company.institutions SET institution_name = 'Sharif University of Technology' WHERE institution_name = 'Jaypee University of Information Technology';
UPDATE young_company.institutions SET institution_name = 'Indian Institute of Technology Guwahati' WHERE institution_name = 'Dhirubhai Ambani Institute of Information and Communication Technology';
UPDATE young_company.institutions SET institution_name = 'Carleton University' WHERE institution_name = 'Carleton College';
UPDATE young_company.institutions SET institution_name = 'The University of New South Wales' WHERE institution_name = 'University of New South Wales';
UPDATE young_company.institutions SET institution_name = 'The University of Sydney' WHERE institution_name = 'University of NSW, Australia';
UPDATE young_company.institutions SET institution_name = 'Case Western Reserve University' WHERE institution_name = 'Case Western Reserve Univeristy';
UPDATE young_company.institutions SET institution_name = 'Harvard University' WHERE institution_name = 'Harvard School of Engineering and Applied Sciences';
UPDATE young_company.institutions SET institution_name = 'Kazan Federal University' WHERE institution_name = 'Kazan State University';
UPDATE young_company.institutions SET institution_name = 'Durham University' WHERE institution_name = 'Durham University Business School';
UPDATE young_company.institutions SET institution_name = 'Beijing Foreign Studies University' WHERE institution_name = 'Beijing Language and Culture University';
UPDATE young_company.institutions SET institution_name = 'Vellore Institute of Technology' WHERE institution_name = 'VIT University, Vellore';
UPDATE young_company.institutions SET institution_name = 'Stanford University' WHERE institution_name = 'Stanford University School of Medicine';
UPDATE young_company.institutions SET institution_name = 'Universidad Autonoma de Yucatan' WHERE institution_name = 'Autonomous University of Yucatan';
UPDATE young_company.institutions SET institution_name = 'Università degli studi Roma Tre' WHERE institution_name = 'Università degli Studi di Roma ''La Sapienza''';
UPDATE young_company.institutions SET institution_name = 'Vanderbilt University' WHERE institution_name = 'Vanderbilt University Law School';
UPDATE young_company.institutions SET institution_name = 'Instituto Tecnológico Autónomo de México' WHERE institution_name = 'Instituto Tecnologico Autonomo de Mexico (ITAM)';
UPDATE young_company.institutions SET institution_name = 'Wesleyan University' WHERE institution_name = 'Wesleyan School';
UPDATE young_company.institutions SET institution_name = 'Georgetown University' WHERE institution_name = 'Georgetown Prep';
UPDATE young_company.institutions SET institution_name = 'Kingston University, London' WHERE institution_name = 'Kingston University';
UPDATE young_company.institutions SET institution_name = 'Lund University' WHERE institution_name = 'Lunds tekniska högskola / The Faculty of Engineering at Lund University';
UPDATE young_company.institutions SET institution_name = 'V. N. Karazin Kharkiv National University' WHERE institution_name = 'Kharkiv V.N. Karazin National University';
UPDATE young_company.institutions SET institution_name = 'Ben-Gurion University of The Negev' WHERE institution_name = 'Universitat Ben Gurion Ba';
UPDATE young_company.institutions SET institution_name = 'Indian Institute of Technology Guwahati' WHERE institution_name = 'IIT Guwahati';
UPDATE young_company.institutions SET institution_name = 'Université de Liège' WHERE institution_name = 'University of Liège';
UPDATE young_company.institutions SET institution_name = 'Birla Institute of Technology and Science, Pilani' WHERE institution_name = 'Birla Institute of Technology & Science, Pilani';
UPDATE young_company.institutions SET institution_name = 'Aix-Marseille University' WHERE institution_name = 'Faculté des sciences Aix-Marseille';
UPDATE young_company.institutions SET institution_name = 'The "Gheorghe Asachi" Technical University of Iasi' WHERE institution_name = 'The \Gheorghe Asachi\" Technical University of Iasi"';
UPDATE young_company.institutions SET institution_name = 'University of San Francisco' WHERE institution_name = 'City College of San Francisco';
UPDATE young_company.institutions SET institution_name = 'McGill University' WHERE institution_name = 'McGill University - Desautels Faculty of Management';
UPDATE young_company.institutions SET institution_name = 'Columbia University' WHERE institution_name = 'Teachers College of Columbia University';
UPDATE young_company.institutions SET institution_name = 'Delft University of Technology' WHERE institution_name = 'Technische Universiteit Delft';
UPDATE young_company.institutions SET institution_name = 'University of Texas Dallas' WHERE institution_name = 'The University of Texas at Dallas';
UPDATE young_company.institutions SET institution_name = 'Vilnius University ' WHERE institution_name = 'Vilniaus universitetas / Vilnius University';
UPDATE young_company.institutions SET institution_name = 'University of International Business and Economics' WHERE institution_name = 'International Business School';
UPDATE young_company.institutions SET institution_name = 'Stanford University' WHERE institution_name = 'Stanford University, Graduate School of Business';
UPDATE young_company.institutions SET institution_name = 'University of Delhi' WHERE institution_name = 'Delhi Public School';
UPDATE young_company.institutions SET institution_name = 'Pennsylvania State University' WHERE institution_name = 'Penn State';
UPDATE young_company.institutions SET institution_name = 'Louisiana State University' WHERE institution_name = 'University of Louisiana at Monroe';
UPDATE young_company.institutions SET institution_name = 'Ohio University' WHERE institution_name = 'ohio university';
UPDATE young_company.institutions SET institution_name = 'Durham University' WHERE institution_name = 'University of Durham, St John''s College';
UPDATE young_company.institutions SET institution_name = 'V. N. Karazin Kharkiv National University' WHERE institution_name = 'Harkivs''kij Nacional''nij Universitet im. V.N. Karazina';
UPDATE young_company.institutions SET institution_name = 'Bina Nusantara University' WHERE institution_name = 'Universitas Bina Nusantara (Binus) International';
UPDATE young_company.institutions SET institution_name = 'University of Arkansas Fayetteville ' WHERE institution_name = 'Arkansas State University';
UPDATE young_company.institutions SET institution_name = 'University of the Pacific' WHERE institution_name = 'Azusa Pacific University';
UPDATE young_company.institutions SET institution_name = 'Columbia University' WHERE institution_name = 'Columbia University - School of International and Public Affairs';
UPDATE young_company.institutions SET institution_name = 'Columbia University' WHERE institution_name = 'Columbia College, Columbia University';
UPDATE young_company.institutions SET institution_name = 'Stanford University' WHERE institution_name = 'Stanford University School of Engineering';
UPDATE young_company.institutions SET institution_name = 'California Polytechnic State University' WHERE institution_name = 'California Polytechnic State University-San Luis Obispo';
UPDATE young_company.institutions SET institution_name = 'New York University' WHERE institution_name = 'New York University\
\
2007';
UPDATE young_company.institutions SET institution_name = 'Texas A&M University' WHERE institution_name = 'Texas A&amp;M University';
UPDATE young_company.institutions SET institution_name = 'Technological University Dublin' WHERE institution_name = 'Dublin Institute of Technology';
UPDATE young_company.institutions SET institution_name = 'Lund University' WHERE institution_name = 'The Faculty of Engineering at Lund University';
UPDATE young_company.institutions SET institution_name = 'Tampere University' WHERE institution_name = 'Tampere University of Technology';
UPDATE young_company.institutions SET institution_name = 'Technical University of Denmark' WHERE institution_name = 'DTU - Technical University of Denmark';
UPDATE young_company.institutions SET institution_name = 'Universidade Federal de Pernambuco ' WHERE institution_name = 'Universidade de Pernambuco';
UPDATE young_company.institutions SET institution_name = 'Institut Polytechnique de Paris' WHERE institution_name = 'Ecole Polytechnique, Paris';
UPDATE young_company.institutions SET institution_name = 'University of the Sunshine Coast ' WHERE institution_name = 'University of the Sunshine Coast';
UPDATE young_company.institutions SET institution_name = 'KU Leuven' WHERE institution_name = 'Imec / KU Leuven';
UPDATE young_company.institutions SET institution_name = 'Georgetown University' WHERE institution_name = 'Georgetown University Law Center';
UPDATE young_company.institutions SET institution_name = 'Imperial College London' WHERE institution_name = 'Royal College of Art | Imperial College London';
UPDATE young_company.institutions SET institution_name = 'Universidad de los Andes' WHERE institution_name = 'Universidad de Los Andes';
UPDATE young_company.institutions SET institution_name = 'University of Nebraska - Lincoln' WHERE institution_name = 'University of Nebraska-Lincoln';
UPDATE young_company.institutions SET institution_name = 'Manchester Metropolitan University' WHERE institution_name = 'The Manchester Metropolitan University';
UPDATE young_company.institutions SET institution_name = 'Tecnológico de Monterrey' WHERE institution_name = 'Instituto Tecnológico y de Estudios Superiores de Monterrey / ITESM\
Preparatoria';
UPDATE young_company.institutions SET institution_name = 'Tecnológico de Monterrey' WHERE institution_name = 'Instituto Tecnológico y de Estudios Superiores de Monterrey / ITESM';
UPDATE young_company.institutions SET institution_name = 'Universidad de los Andes' WHERE institution_name = 'University of the Andes';
UPDATE young_company.institutions SET institution_name = 'Universidad de Santiago de Chile' WHERE institution_name = 'Santiago College';
UPDATE young_company.institutions SET institution_name = 'Universidade da Coruña ' WHERE institution_name = 'Universidad de A Coruna, Spain';
UPDATE young_company.institutions SET institution_name = 'University of Florida' WHERE institution_name = 'University of S. Florida';
UPDATE young_company.institutions SET institution_name = 'University of Notre Dame' WHERE institution_name = 'Notre Dame';
UPDATE young_company.institutions SET institution_name = 'University of Pennsylvania' WHERE institution_name = 'University of Pennsylvania Law School';
UPDATE young_company.institutions SET institution_name = 'University of Texas at San Antonio' WHERE institution_name = 'The University of Texas at San Antonio';
UPDATE young_company.institutions SET institution_name = 'North Carolina State University' WHERE institution_name = 'North Carolina Agricultural and Technical State University';
UPDATE young_company.institutions SET institution_name = 'Goldsmiths, University of London' WHERE institution_name = 'Goldsmiths College, U. of London';
UPDATE young_company.institutions SET institution_name = 'The University of Hong Kong' WHERE institution_name = 'The University of Hong Kong, Faculty of Law';
UPDATE young_company.institutions SET institution_name = 'University of Toronto' WHERE institution_name = 'University of Toronto, Faculty of Law';
UPDATE young_company.institutions SET institution_name = 'Utah State University ' WHERE institution_name = 'Utah State University';
UPDATE young_company.institutions SET institution_name = 'KIT, Karlsruhe Institute of Technology' WHERE institution_name = 'Karlsruhe Institute of Technology (KIT)';
UPDATE young_company.institutions SET institution_name = 'Oklahoma State University ' WHERE institution_name = 'Oklahoma State University';
UPDATE young_company.institutions SET institution_name = 'Maastricht University ' WHERE institution_name = 'Maastricht University';
UPDATE young_company.institutions SET institution_name = 'Maastricht University ' WHERE institution_name = 'Universiteit Maastricht';
UPDATE young_company.institutions SET institution_name = 'Universidade Federal de Pernambuco ' WHERE institution_name = 'Universidade Federal de Pernambuco';
UPDATE young_company.institutions SET institution_name = 'University of St.Gallen' WHERE institution_name = 'University of St. Gallen (HSG)';
UPDATE young_company.institutions SET institution_name = 'Ecole des Ponts ParisTech' WHERE institution_name = 'École des Ponts ParisTech';
UPDATE young_company.institutions SET institution_name = 'University of Wisconsin Milwaukee ' WHERE institution_name = 'University of Wisconsin-Milwaukee';
UPDATE young_company.institutions SET institution_name = 'Washington University in St. Louis' WHERE institution_name = 'Washington University School of Medicine in St. Louis';
UPDATE young_company.institutions SET institution_name = 'University of St Andrews' WHERE institution_name = 'St. Andrews Episcopal School';
UPDATE young_company.institutions SET institution_name = 'The University of Adelaide' WHERE institution_name = 'University of Adelaide';
UPDATE young_company.institutions SET institution_name = 'Columbia University' WHERE institution_name = 'Barnard College, Columbia University';
UPDATE young_company.institutions SET institution_name = 'University of Missouri, Kansas City' WHERE institution_name = 'University of Missouri-Kansas City';
UPDATE young_company.institutions SET institution_name = 'University of Westminster' WHERE institution_name = 'Westminster College';
UPDATE young_company.institutions SET institution_name = 'College of William and Mary' WHERE institution_name = 'St. Mary''s College of Maryland';
UPDATE young_company.institutions SET institution_name = 'Trinity College Dublin, The University of Dublin' WHERE institution_name = 'Trinity College, Dublin';
UPDATE young_company.institutions SET institution_name = 'University of San Francisco' WHERE institution_name = 'University of San Francisco School of Law';
UPDATE young_company.institutions SET institution_name = 'Tecnológico de Monterrey' WHERE institution_name = 'Tecnologico de estudios superiores de monterrey';
UPDATE young_company.institutions SET institution_name = 'Tecnológico de Monterrey' WHERE institution_name = 'Tecnológico de monterrey campus GDA';
UPDATE young_company.institutions SET institution_name = 'King''s College London' WHERE institution_name = 'The King''s College';
UPDATE young_company.institutions SET institution_name = 'Binghamton University SUNY' WHERE institution_name = 'Binghamton University';
UPDATE young_company.institutions SET institution_name = 'Stony Brook University, State University of New York' WHERE institution_name = 'Stony Brook University';
UPDATE young_company.institutions SET institution_name = 'University of Kansas' WHERE institution_name = 'The University of Kansas';
UPDATE young_company.institutions SET institution_name = 'Yonsei University' WHERE institution_name = 'Yonsei University 연세대학교';
UPDATE young_company.institutions SET institution_name = 'University of Haifa' WHERE institution_name = 'HAIFA UNIVERSITY';
UPDATE young_company.institutions SET institution_name = 'University at Buffalo SUNY' WHERE institution_name = 'State University of New York at Buffalo';
UPDATE young_company.institutions SET institution_name = 'Xi’an Jiaotong University' WHERE institution_name = 'Xi''an Jiaotong University';
UPDATE young_company.institutions SET institution_name = 'San Diego State University' WHERE institution_name = 'San Diego State University-California State University';
UPDATE young_company.institutions SET institution_name = 'Auburn University' WHERE institution_name = 'Auburn University, Samuel Ginn College of Engineering';
UPDATE young_company.institutions SET institution_name = 'The University of Sheffield' WHERE institution_name = 'Sheffield University';
UPDATE young_company.institutions SET institution_name = 'University at Albany SUNY' WHERE institution_name = 'University at Albany';
UPDATE young_company.institutions SET institution_name = 'University of Cape Town' WHERE institution_name = 'University of Cape Town (Graduate School of Business)';
UPDATE young_company.institutions SET institution_name = 'Tel Aviv University' WHERE institution_name = 'Tel-Aviv University Secondary School';
UPDATE young_company.institutions SET institution_name = 'Erasmus University Rotterdam ' WHERE institution_name = 'Erasmus University Rotterdam';
UPDATE young_company.institutions SET institution_name = 'Saint Petersburg State University' WHERE institution_name = 'Saint Petersburg State Art-Industrial Academy';
UPDATE young_company.institutions SET institution_name = 'Ateneo de Manila University' WHERE institution_name = 'De La Salle University - Manila';
UPDATE young_company.institutions SET institution_name = 'Pennsylvania State University' WHERE institution_name = 'Pennsylvania State University, Schreyer Honors College';
UPDATE young_company.institutions SET institution_name = 'Southern Federal University' WHERE institution_name = 'Southern Federal University (former Rostov State University)';
UPDATE young_company.institutions SET institution_name = 'SRM INSTITUTE OF SCIENCE AND TECHNOLOGY' WHERE institution_name = 'SRM University';
UPDATE young_company.institutions SET institution_name = 'University of Zurich' WHERE institution_name = 'Universität  Zürich | University of Zurich';
UPDATE young_company.institutions SET institution_name = 'Queen''s University at Kingston' WHERE institution_name = 'Queen''s College';
UPDATE young_company.institutions SET institution_name = 'Brigham Young University' WHERE institution_name = 'Brigham Young University - Hawaii';
UPDATE young_company.institutions SET institution_name = 'College of William and Mary' WHERE institution_name = 'William & Mary';
UPDATE young_company.institutions SET institution_name = 'Universidad La Salle' WHERE institution_name = 'La Salle BCN';
UPDATE young_company.institutions SET institution_name = 'Indian Institute of Science' WHERE institution_name = 'Indian Institute of Science (IISc)';
UPDATE young_company.institutions SET institution_name = 'Universidad Pontificia Bolivariana ' WHERE institution_name = 'Universidad Pontificia Bolivariana';
UPDATE young_company.institutions SET institution_name = 'University of Göttingen' WHERE institution_name = 'Georg-August-Universität Göttingen';
UPDATE young_company.institutions SET institution_name = 'University of British Columbia' WHERE institution_name = 'British Columbia Institute of Technology';
UPDATE young_company.institutions SET institution_name = 'Temple University' WHERE institution_name = 'Temple University College of Engineering';
UPDATE young_company.institutions SET institution_name = 'Ghent University' WHERE institution_name = 'Ghent University, Belgium';
UPDATE young_company.institutions SET institution_name = 'Universita'' degli Studi di Ferrara' WHERE institution_name = 'Università degli Studi di Trieste';
UPDATE young_company.institutions SET institution_name = 'Universidade de São Paulo' WHERE institution_name = 'Escola Politécnica da Universidade de São Paulo';
UPDATE young_company.institutions SET institution_name = 'Duke University' WHERE institution_name = 'Duke University School of Law';
UPDATE young_company.institutions SET institution_name = 'Saint Petersburg State University' WHERE institution_name = 'St. Petersburg State University';
UPDATE young_company.institutions SET institution_name = 'Queen Mary University of London' WHERE institution_name = 'Queen Mary, U. of London';
UPDATE young_company.institutions SET institution_name = 'Lomonosov Moscow State University' WHERE institution_name = 'Lomonosov Moscow State University (MSU)';
UPDATE young_company.institutions SET institution_name = 'KAIST - Korea Advanced Institute of Science & Technology' WHERE institution_name = 'KAIST(Korea Advanced Institute of Science and Technology)';
UPDATE young_company.institutions SET institution_name = 'University of California, Berkeley' WHERE institution_name = 'UC Berkeley and UCSF';
UPDATE young_company.institutions SET institution_name = 'University of Electronic Science and Technology of China' WHERE institution_name = 'Institute of Computing Technology, Chinese Academy of Science';
UPDATE young_company.institutions SET institution_name = 'Kansas State University ' WHERE institution_name = 'Kansas State University';
UPDATE young_company.institutions SET institution_name = 'Kenyatta University' WHERE institution_name = 'Kenyatta university';
UPDATE young_company.institutions SET institution_name = 'Bilkent University' WHERE institution_name = 'Bilkent Üniversitesi';
UPDATE young_company.institutions SET institution_name = 'College of William and Mary' WHERE institution_name = 'William & Mary Law School';
UPDATE young_company.institutions SET institution_name = 'The University of Melbourne' WHERE institution_name = 'University of Melbourne';
UPDATE young_company.institutions SET institution_name = 'De La Salle University' WHERE institution_name = 'La Salle (Ramon Llull University)';
UPDATE young_company.institutions SET institution_name = 'Singapore Management University' WHERE institution_name = 'Singapore Institute of Management';
UPDATE young_company.institutions SET institution_name = 'University of Toronto' WHERE institution_name = 'University of Toronto at Scarborough';
UPDATE young_company.institutions SET institution_name = 'Lomonosov Moscow State University' WHERE institution_name = 'Lomonosov Moscow University';
UPDATE young_company.institutions SET institution_name = 'Stony Brook University, State University of New York' WHERE institution_name = 'State University of New York at Stony Brook';
UPDATE young_company.institutions SET institution_name = 'Georgia Institute of Technology' WHERE institution_name = 'Georgia Institute of Technology (Atlanta)';
UPDATE young_company.institutions SET institution_name = 'Management and Science University' WHERE institution_name = 'College of Management';
UPDATE young_company.institutions SET institution_name = 'Indian Institute of Technology Indore' WHERE institution_name = 'Indian Institute of Technology';
UPDATE young_company.institutions SET institution_name = 'Universidad Panamericana' WHERE institution_name = 'Universidad Panamericana Preparatoria';
UPDATE young_company.institutions SET institution_name = 'Robert Gordon University' WHERE institution_name = 'Robert Gordon''s College';
UPDATE young_company.institutions SET institution_name = 'Wesleyan University' WHERE institution_name = 'Oklahoma Wesleyan University';
UPDATE young_company.institutions SET institution_name = 'University of Ottawa' WHERE institution_name = 'University of Ottawa Law';
UPDATE young_company.institutions SET institution_name = 'Victoria University ' WHERE institution_name = 'Victoria Park Collegiate Institute';
UPDATE young_company.institutions SET institution_name = 'Umea University' WHERE institution_name = 'Umeå University';
UPDATE young_company.institutions SET institution_name = 'Universidad de Puerto Rico' WHERE institution_name = 'University of Puerto Rico';
UPDATE young_company.institutions SET institution_name = 'Wesleyan University' WHERE institution_name = 'Ohio Wesleyan University';
UPDATE young_company.institutions SET institution_name = 'The American University in Cairo' WHERE institution_name = 'American University of Cairo';
UPDATE young_company.institutions SET institution_name = 'Friedrich-Alexander-Universität Erlangen-Nürnberg' WHERE institution_name = 'University of Erlangen-Nuremberg';
UPDATE young_company.institutions SET institution_name = 'Université Paris 1 Panthéon-Sorbonne ' WHERE institution_name = 'Université Paris 1 Panthéon-Sorbonne';
UPDATE young_company.institutions SET institution_name = 'Brigham Young University' WHERE institution_name = 'Brigham Young University Marriott School of Business';
UPDATE young_company.institutions SET institution_name = 'University of Washington' WHERE institution_name = 'Washington and Lee University';
UPDATE young_company.institutions SET institution_name = 'University of California, Santa Cruz' WHERE institution_name = 'UC Santa Cruz';
UPDATE young_company.institutions SET institution_name = 'University of Texas at Austin' WHERE institution_name = 'The University of Texas at Austin - Red McCombs School of Business';
UPDATE young_company.institutions SET institution_name = 'St. Louis University' WHERE institution_name = 'Saint Louis University';
UPDATE young_company.institutions SET institution_name = 'Aix-Marseille University' WHERE institution_name = 'Aix-Marseille Graduate School of Management - IAE';
UPDATE young_company.institutions SET institution_name = 'Nanyang Technological University, Singapore' WHERE institution_name = 'Nanyang Polytechnic';
UPDATE young_company.institutions SET institution_name = 'National Research Nuclear University MEPhI' WHERE institution_name = 'National Research Nuclear University MEPhI (Moscow Engineering Physics Institute)';
UPDATE young_company.institutions SET institution_name = 'Stockholm University' WHERE institution_name = 'SAE Stockholm';
UPDATE young_company.institutions SET institution_name = 'London Metropolitan University' WHERE institution_name = 'City University London';
UPDATE young_company.institutions SET institution_name = 'University of Notre Dame' WHERE institution_name = 'Notre Dame College';
UPDATE young_company.institutions SET institution_name = 'Durham University' WHERE institution_name = 'University of Durham';
UPDATE young_company.institutions SET institution_name = 'The University of Warwick' WHERE institution_name = 'Warwick Business School';
UPDATE young_company.institutions SET institution_name = 'International Islamic University Islamabad' WHERE institution_name = 'International Islamic University';
UPDATE young_company.institutions SET institution_name = 'Universidade Federal de Santa Maria' WHERE institution_name = 'Universidad ''Santa María''';
UPDATE young_company.institutions SET institution_name = 'Eberhard Karls Universität Tübingen' WHERE institution_name = 'University of Tübingen';
UPDATE young_company.institutions SET institution_name = 'Universidad de Buenos Aires' WHERE institution_name = 'Universidad de Belgrano, Buenos Aires, Argentina';
UPDATE young_company.institutions SET institution_name = 'Sakarya University' WHERE institution_name = 'Sakarya Üniversitesi';
UPDATE young_company.institutions SET institution_name = 'Poznań University of Technology' WHERE institution_name = 'Politechnika Wrocławska / Wroclaw University of Technology';
UPDATE young_company.institutions SET institution_name = 'Bielefeld University' WHERE institution_name = 'Fachhochschule Bielefeld, University of Applied Sciences, Germany';
UPDATE young_company.institutions SET institution_name = 'Universidade de São Paulo' WHERE institution_name = 'Universidade de São Paulo (USP)';
UPDATE young_company.institutions SET institution_name = 'Saint Petersburg State University' WHERE institution_name = 'St. Petersburg College';
UPDATE young_company.institutions SET institution_name = 'Rutgers, The State University of New Jersey, Camden' WHERE institution_name = 'Rutgers, The State University of New Jersey';
UPDATE young_company.institutions SET institution_name = 'Instituto Tecnológico de Buenos Aires' WHERE institution_name = 'Instituto Tecnológico de Buenos Aires (ITBA)';
UPDATE young_company.institutions SET institution_name = 'Universita'' degli Studi di Ferrara' WHERE institution_name = 'Università degli Studi di Ferrara';
UPDATE young_company.institutions SET institution_name = 'Tecnológico de Monterrey' WHERE institution_name = 'Tec de Monterrey';
UPDATE young_company.institutions SET institution_name = 'Sapienza University of Rome' WHERE institution_name = 'University of Rome \La Sapienza\""';
UPDATE young_company.institutions SET institution_name = 'Harvard University' WHERE institution_name = 'Harvard Extension School';
UPDATE young_company.institutions SET institution_name = 'Universität Stuttgart' WHERE institution_name = 'University of Stuttgart';
UPDATE young_company.institutions SET institution_name = 'University of Arkansas Fayetteville ' WHERE institution_name = 'University of Arkansas';
UPDATE young_company.institutions SET institution_name = 'Shanghai International Studies University' WHERE institution_name = 'ESAI Shanghai';
UPDATE young_company.institutions SET institution_name = 'Yale University' WHERE institution_name = 'Yale Law School';
UPDATE young_company.institutions SET institution_name = 'Yale University' WHERE institution_name = 'Yale College';
UPDATE young_company.institutions SET institution_name = 'Universidade Federal de São Carlos' WHERE institution_name = 'Universidade São Francisco';
UPDATE young_company.institutions SET institution_name = 'Universidade Estadual de Campinas' WHERE institution_name = 'University of Campinas';
UPDATE young_company.institutions SET institution_name = 'Fudan University' WHERE institution_name = 'Fudan University, Shanghai (PRC) & University Assas (Paris II)';
UPDATE young_company.institutions SET institution_name = 'Universidad Nacional Autónoma de México' WHERE institution_name = 'Universidad Nacional Autonoma de Mexico';
UPDATE young_company.institutions SET institution_name = 'George Washington University' WHERE institution_name = 'The George Washington University Law School';
UPDATE young_company.institutions SET institution_name = 'University of Westminster' WHERE institution_name = 'Westminster Schools';
UPDATE young_company.institutions SET institution_name = 'University of Delhi' WHERE institution_name = 'Delhi Public School, Vasant Kunj';
UPDATE young_company.institutions SET institution_name = 'University of North Carolina at Charlotte' WHERE institution_name = 'University of North Carolina at Wilmington';
UPDATE young_company.institutions SET institution_name = 'University of Hawaiʻi at Mānoa' WHERE institution_name = 'University of Hawaii at Manoa';
UPDATE young_company.institutions SET institution_name = 'Università di Padova' WHERE institution_name = 'Università degli Studi di Padova';
UPDATE young_company.institutions SET institution_name = 'Alma Mater Studiorum - University of Bologna' WHERE institution_name = 'University of Bologna';
UPDATE young_company.institutions SET institution_name = 'Ecole des Ponts ParisTech' WHERE institution_name = 'ENSTA ParisTech - École Nationale Supérieure de Techniques Avancées';
UPDATE young_company.institutions SET institution_name = 'Universitat de Barcelona' WHERE institution_name = 'ESADE Business School, Barcelona';
UPDATE young_company.institutions SET institution_name = 'Universidad La Salle' WHERE institution_name = 'Instituto La Salle Florida';
UPDATE young_company.institutions SET institution_name = 'University of Salamanca' WHERE institution_name = 'Universidad Pontificia de Salamanca';
UPDATE young_company.institutions SET institution_name = 'University of Deusto' WHERE institution_name = 'Universidad de Deusto';
UPDATE young_company.institutions SET institution_name = 'Universidad de Buenos Aires' WHERE institution_name = 'University of Buenos Aires';
UPDATE young_company.institutions SET institution_name = 'Colorado State University' WHERE institution_name = 'Colorado College';
UPDATE young_company.institutions SET institution_name = 'University of Washington' WHERE institution_name = 'University of California, Washington Center';
UPDATE young_company.institutions SET institution_name = 'Lehigh University' WHERE institution_name = 'Lehigh University College of Business';
UPDATE young_company.institutions SET institution_name = 'Tampere University' WHERE institution_name = 'Tampere University of Applied Sciences';
UPDATE young_company.institutions SET institution_name = 'Amrita Vishwa Vidyapeetham' WHERE institution_name = 'AMRITA VISHWA VIDYAPEETHAM';
UPDATE young_company.institutions SET institution_name = 'ETH Zurich - Swiss Federal Institute of Technology' WHERE institution_name = 'ETH Zürich - Swiss Federal Institute of Technology Zurich';
UPDATE young_company.institutions SET institution_name = 'University of Brighton' WHERE institution_name = 'Brighton College';
UPDATE young_company.institutions SET institution_name = 'KAIST - Korea Advanced Institute of Science & Technology' WHERE institution_name = 'Korea Advanced Institute of Science and Technology';
UPDATE young_company.institutions SET institution_name = 'Trinity College Dublin, The University of Dublin' WHERE institution_name = 'university of dublin, trinity college';
UPDATE young_company.institutions SET institution_name = 'Paul Valéry University Montpellier' WHERE institution_name = 'Université Paul Valéry';
UPDATE young_company.institutions SET institution_name = 'Technical University of Crete' WHERE institution_name = 'Technical University of Crete / Πολυτεχνείο Κρήτης';
UPDATE young_company.institutions SET institution_name = 'City, University of London' WHERE institution_name = 'City of London School';
UPDATE young_company.institutions SET institution_name = 'Universiti Teknologi Malaysia ' WHERE institution_name = 'Universiti Teknologi Malaysia';
UPDATE young_company.institutions SET institution_name = 'Universiti Teknologi MARA - UiTM' WHERE institution_name = 'Universiti Teknologi MARA';
UPDATE young_company.institutions SET institution_name = 'The University of Alabama' WHERE institution_name = 'University of Alabama';
UPDATE young_company.institutions SET institution_name = 'Università degli Studi di Perugia' WHERE institution_name = 'Università per Stranieri di Perugia';
UPDATE young_company.institutions SET institution_name = 'Xiamen University' WHERE institution_name = 'Xiamen University, Xiamen, Fujian, P.R. China';
UPDATE young_company.institutions SET institution_name = 'Middlesex University' WHERE institution_name = 'Middlesex School';
UPDATE young_company.institutions SET institution_name = 'City University of New York' WHERE institution_name = 'Baruch College, City University of New York';
UPDATE young_company.institutions SET institution_name = 'Pontificia Universidad Católica del Perú' WHERE institution_name = 'Pontificia Universidad Catolica de Perú';
UPDATE young_company.institutions SET institution_name = 'Universidad Nacional de Ingeniería Peru' WHERE institution_name = 'Universidad Nacional de Ingenieria (INICTEL)';
UPDATE young_company.institutions SET institution_name = 'Harvard University' WHERE institution_name = 'Harvard Medical School';
UPDATE young_company.institutions SET institution_name = 'University of Bergen' WHERE institution_name = 'Universitetet i Bergen';
UPDATE young_company.institutions SET institution_name = 'Yale University' WHERE institution_name = 'Yale-NUS College';
UPDATE young_company.institutions SET institution_name = 'Universidade Federal do Paraná - UFPR' WHERE institution_name = 'Universidade Tecnológica Federal do Paraná';
UPDATE young_company.institutions SET institution_name = 'Sungkyunkwan University' WHERE institution_name = 'SKK GSB Sungkyunkwan University';
UPDATE young_company.institutions SET institution_name = 'Université Laval' WHERE institution_name = 'Université Laval / Laval University';
UPDATE young_company.institutions SET institution_name = 'University of Southern California' WHERE institution_name = 'University of Southern California - Marshall School of Business';
UPDATE young_company.institutions SET institution_name = 'University of Salford' WHERE institution_name = 'The University of Salford';
UPDATE young_company.institutions SET institution_name = 'University of Washington' WHERE institution_name = 'Washington International School';
UPDATE young_company.institutions SET institution_name = 'University of Washington' WHERE institution_name = 'University of Washington Information School';
UPDATE young_company.institutions SET institution_name = 'University of California, Berkeley' WHERE institution_name = 'UC Berkeley MS ME';
UPDATE young_company.institutions SET institution_name = 'University of Michigan-Ann Arbor' WHERE institution_name = 'University of Michigan-Dearborn';
UPDATE young_company.institutions SET institution_name = 'Universidad Metropolitana' WHERE institution_name = 'Universidad Metropolitana (VE)';
UPDATE young_company.institutions SET institution_name = 'Institut National des Sciences Appliquées de Lyon' WHERE institution_name = 'INSA Lyon - Institut National des Sciences Appliquées de Lyon';
UPDATE young_company.institutions SET institution_name = 'Novosibirsk State Technical University' WHERE institution_name = 'Novosibirsk State Technical University (NSTU)';
UPDATE young_company.institutions SET institution_name = 'Ajou University ' WHERE institution_name = 'Ajou University';
UPDATE young_company.institutions SET institution_name = 'Albert-Ludwigs-Universitaet Freiburg' WHERE institution_name = 'Université de Fribourg - Universität Freiburg';
UPDATE young_company.institutions SET institution_name = 'Texas Tech University' WHERE institution_name = 'Texas State University';
UPDATE young_company.institutions SET institution_name = 'Washington University in St. Louis' WHERE institution_name = 'Washington University, St. Louis';
UPDATE young_company.institutions SET institution_name = 'Universidad de Puerto Rico' WHERE institution_name = 'University of Puerto Rico-Mayaguez';
UPDATE young_company.institutions SET institution_name = 'University of St.Gallen' WHERE institution_name = 'Universität St.Gallen';
UPDATE young_company.institutions SET institution_name = 'George Mason University' WHERE institution_name = 'George Mason University School of Law';
UPDATE young_company.institutions SET institution_name = 'University of Brighton' WHERE institution_name = 'Brighton University';
UPDATE young_company.institutions SET institution_name = 'Tecnológico de Costa Rica -TEC' WHERE institution_name = 'Instituto Tecnológico de Costa Rica';
UPDATE young_company.institutions SET institution_name = 'Universidad de Costa Rica' WHERE institution_name = 'Centro Educativo Adventista de Costa Rica';
UPDATE young_company.institutions SET institution_name = 'Portland State University' WHERE institution_name = 'University of Portland';
UPDATE young_company.institutions SET institution_name = 'Auburn University' WHERE institution_name = 'Auburn University Harbert College of Business';
UPDATE young_company.institutions SET institution_name = 'Utrecht University' WHERE institution_name = 'Utrecht School of Arts';
UPDATE young_company.institutions SET institution_name = 'Universidad de Zaragoza' WHERE institution_name = 'University of Zaragoza, Spain';
UPDATE young_company.institutions SET institution_name = 'University of Glasgow' WHERE institution_name = 'The University of Glasgow';
UPDATE young_company.institutions SET institution_name = 'Instituto Tecnológico de Buenos Aires' WHERE institution_name = 'Cultural Inglesa de Buenos Aires';
UPDATE young_company.institutions SET institution_name = 'Instituto Tecnológico de Buenos Aires' WHERE institution_name = 'ITBA (Instituto Tecnologico de Buenos Aires)';
UPDATE young_company.institutions SET institution_name = 'Griffith University' WHERE institution_name = 'Griffith University (Australia)';
UPDATE young_company.institutions SET institution_name = 'Queen''s University at Kingston' WHERE institution_name = 'Queens University';
UPDATE young_company.institutions SET institution_name = 'Universidad Católica Andres Bello' WHERE institution_name = 'UCAB - Universidad Católica Andrés Bello';
UPDATE young_company.institutions SET institution_name = 'The University of Melbourne' WHERE institution_name = 'The Univeristy of Melbourne';
UPDATE young_company.institutions SET institution_name = 'Tufts University' WHERE institution_name = 'Tufts University School of Medicine';
UPDATE young_company.institutions SET institution_name = 'University of Bath' WHERE institution_name = 'The University of Bath';
UPDATE young_company.institutions SET institution_name = 'University of Minnesota Twin Cities' WHERE institution_name = 'University of Minnesota, Twin Cities';
UPDATE young_company.institutions SET institution_name = 'National Yang Ming Chiao Tung University' WHERE institution_name = 'National Yang Ming University';
UPDATE young_company.institutions SET institution_name = 'The University of Tennessee, Knoxville' WHERE institution_name = 'The University of Tennessee Knoxville';
UPDATE young_company.institutions SET institution_name = 'Jagiellonian University' WHERE institution_name = 'Jagiellonian University\
Master of Science';
UPDATE young_company.institutions SET institution_name = 'Florida State University' WHERE institution_name = 'Florida State University College of Law';
UPDATE young_company.institutions SET institution_name = 'University of Bristol' WHERE institution_name = 'Bristol University';
UPDATE young_company.institutions SET institution_name = 'Princeton University' WHERE institution_name = 'The Hun School of Princeton';
UPDATE young_company.institutions SET institution_name = 'Universidad Externado de Colombia ' WHERE institution_name = 'Universidad Externado de Colombia';
UPDATE young_company.institutions SET institution_name = 'University of Texas Dallas' WHERE institution_name = 'The University of Dallas';
UPDATE young_company.institutions SET institution_name = 'Universidade Federal de Santa Catarina ' WHERE institution_name = 'Universidade Federal de Santa Catarina';
UPDATE young_company.institutions SET institution_name = 'Loyola University Chicago' WHERE institution_name = 'Loyola College';
UPDATE young_company.institutions SET institution_name = 'Pontificia Universidad Católica Argentina' WHERE institution_name = 'Pontificia Universidad Católica de Córdoba';
UPDATE young_company.institutions SET institution_name = 'Ateneo de Manila University' WHERE institution_name = 'Ateneo de Manila University (dropped out)';
UPDATE young_company.institutions SET institution_name = 'The Ohio State University' WHERE institution_name = 'The Ohio State University\
\
2011';
UPDATE young_company.institutions SET institution_name = 'Gwangju Institute of Science and Technology' WHERE institution_name = 'Gwangju institute of science and technology';
UPDATE young_company.institutions SET institution_name = 'Georgetown University' WHERE institution_name = 'Georgetown';
UPDATE young_company.institutions SET institution_name = 'Université du Québec' WHERE institution_name = 'Université du Québec à Rimouski';
UPDATE young_company.institutions SET institution_name = 'ITESO, Universidad Jesuita de Guadalajara' WHERE institution_name = 'ITESO Universidad Jesuita de Guadalajara';
UPDATE young_company.institutions SET institution_name = 'Auburn University' WHERE institution_name = 'Auburn University, College of Business';
UPDATE young_company.institutions SET institution_name = 'Universidad de Carabobo' WHERE institution_name = 'Universidad de Carabobo (VE)';
UPDATE young_company.institutions SET institution_name = 'Universität Mannheim' WHERE institution_name = 'University of Mannheim';
UPDATE young_company.institutions SET institution_name = 'New York University' WHERE institution_name = 'New York University School of Law';
UPDATE young_company.institutions SET institution_name = 'Universidad Nacional de Ingeniería Peru' WHERE institution_name = 'Universidad Nacional de Ingenieria';
UPDATE young_company.institutions SET institution_name = 'University at Buffalo SUNY' WHERE institution_name = 'University at Buffalo';
UPDATE young_company.institutions SET institution_name = 'Tallinn University ' WHERE institution_name = 'Tallinna Reaalkool';
UPDATE young_company.institutions SET institution_name = 'Clark University' WHERE institution_name = 'Clark Atlanta University';
UPDATE young_company.institutions SET institution_name = 'Brown University' WHERE institution_name = 'Brown University School of Engineering';
UPDATE young_company.institutions SET institution_name = 'National University of Modern Languages, Islamabad' WHERE institution_name = 'National University of Modern Languages (NUML)';
UPDATE young_company.institutions SET institution_name = 'Beijing Normal University ' WHERE institution_name = 'Beijing Normal University';
UPDATE young_company.institutions SET institution_name = 'Indian Institute of Technology Hyderabad' WHERE institution_name = 'IIIT Hyderabad';
UPDATE young_company.institutions SET institution_name = 'Uppsala University' WHERE institution_name = 'University of Uppsala, Sweden';
UPDATE young_company.institutions SET institution_name = 'Indian Institute of Technology Hyderabad' WHERE institution_name = 'Indian Institute of Technology, Hyderabad';
UPDATE young_company.institutions SET institution_name = 'Universiti Malaya' WHERE institution_name = 'University of Malaya';
UPDATE young_company.institutions SET institution_name = 'Tulane University' WHERE institution_name = 'Tulane University School of Medicine';
UPDATE young_company.institutions SET institution_name = 'Duke University' WHERE institution_name = 'Duke University School of Medicine';
UPDATE young_company.institutions SET institution_name = 'The University of New South Wales' WHERE institution_name = 'University of Wales, Aberystwyth';
UPDATE young_company.institutions SET institution_name = 'TU Dortmund University' WHERE institution_name = 'Technical University Dortmund';
UPDATE young_company.institutions SET institution_name = 'Ruhr-Universität Bochum' WHERE institution_name = 'Ruhr University Bochum';
UPDATE young_company.institutions SET institution_name = 'Novosibirsk State Technical University' WHERE institution_name = 'Novosibirsk State University of Economics and Management';
UPDATE young_company.institutions SET institution_name = 'University of Tehran' WHERE institution_name = 'Tehran University of Medical Sciences';
UPDATE young_company.institutions SET institution_name = 'Dalian University of Technology' WHERE institution_name = 'Dalian University of Technology, China';
UPDATE young_company.institutions SET institution_name = 'Ateneo de Manila University' WHERE institution_name = 'Ateneo De Manila University School of Law';
UPDATE young_company.institutions SET institution_name = 'Perm State National Research University' WHERE institution_name = 'Perm State University (PSU)';
UPDATE young_company.institutions SET institution_name = 'Duke University' WHERE institution_name = 'Duke Graduate School';
UPDATE young_company.institutions SET institution_name = 'Lancaster University' WHERE institution_name = 'University of Lancaster';
UPDATE young_company.institutions SET institution_name = 'Aristotle University of Thessaloniki' WHERE institution_name = 'Aristotle University of Thessaloniki (AUTH)';
UPDATE young_company.institutions SET institution_name = 'Universität Duisburg-Essen' WHERE institution_name = 'Universität Duisburg-Essen, Standort Duisburg';
UPDATE young_company.institutions SET institution_name = 'University Paris 2 Panthéon-Assas' WHERE institution_name = 'Université Panthéon Assas (Paris II)';
UPDATE young_company.institutions SET institution_name = 'University of Milano-Bicocca ' WHERE institution_name = 'Università degli Studi di Milano-Bicocca';
UPDATE young_company.institutions SET institution_name = 'Università degli studi di Bergamo' WHERE institution_name = 'Università degli Studi di Bergamo';
UPDATE young_company.institutions SET institution_name = 'University Paris 2 Panthéon-Assas' WHERE institution_name = 'Université Panthéon Assas';
UPDATE young_company.institutions SET institution_name = 'Freie Universitaet Berlin' WHERE institution_name = 'Freie Universität Berlin';
UPDATE young_company.institutions SET institution_name = 'Financial University under the Government of the Russian Federation' WHERE institution_name = 'Finance University under the Government of the Russian Federation';
UPDATE young_company.institutions SET institution_name = 'Pontificia Universidad Católica de Chile' WHERE institution_name = 'Pontificia Universidad Católica of Chile';
UPDATE young_company.institutions SET institution_name = 'Ben-Gurion University of The Negev' WHERE institution_name = 'Ben-Gurion University of the Negev';
UPDATE young_company.institutions SET institution_name = 'University of Illinois at Urbana-Champaign' WHERE institution_name = 'Univeristy of Illinois Cahmpaign-Urbana';
UPDATE young_company.institutions SET institution_name = 'Indian Institute of Technology Kanpur' WHERE institution_name = 'National Institute of Technology Raipur';
UPDATE young_company.institutions SET institution_name = 'Universität Hamburg' WHERE institution_name = 'University of Hamburg';
UPDATE young_company.institutions SET institution_name = 'University of Toronto' WHERE institution_name = 'University of Toronto - University of Trinity College';
UPDATE young_company.institutions SET institution_name = 'York University' WHERE institution_name = 'York College';
UPDATE young_company.institutions SET institution_name = 'Universidad Pontificia Comillas' WHERE institution_name = 'Universidad Pontificia de Comillas (ICADE)';
UPDATE young_company.institutions SET institution_name = 'Athens University of Economics and Business' WHERE institution_name = 'Stockholm School of Economics';
UPDATE young_company.institutions SET institution_name = 'Stockholm University' WHERE institution_name = 'Stockholms universitet';
UPDATE young_company.institutions SET institution_name = 'Comenius University Bratislava' WHERE institution_name = 'Comenius University in Bratislava';
UPDATE young_company.institutions SET institution_name = 'University of Texas at Austin' WHERE institution_name = 'The University of Texas at Austin - The Red McCombs School of Business';
UPDATE young_company.institutions SET institution_name = 'Pontificia Universidad Catolica Madre y Maestra' WHERE institution_name = 'Pontificia Universidad Católica Madre y Maestra';
UPDATE young_company.institutions SET institution_name = 'Universidad Tecnica Particular De Loja' WHERE institution_name = 'Universidad Tecnologica de Jalisco';
UPDATE young_company.institutions SET institution_name = 'Pontificia Universidad Catolica Madre y Maestra' WHERE institution_name = 'Pontificia Universidad Católica Madre y Maestra (PUCMM)';
UPDATE young_company.institutions SET institution_name = 'Instituto Tecnológico de Buenos Aires' WHERE institution_name = 'Instituto tecnologico de las Americas (ITLA)';
UPDATE young_company.institutions SET institution_name = 'University of the Philippines' WHERE institution_name = 'University of the Philippines Diliman';
UPDATE young_company.institutions SET institution_name = 'University of Ljubljana' WHERE institution_name = 'Biotechnical faculty, University of Ljubljana';
UPDATE young_company.institutions SET institution_name = 'Texas A&M University' WHERE institution_name = 'Texas A&amp;M University - Dwight Look College of Engineering';
UPDATE young_company.institutions SET institution_name = 'Complutense University of Madrid' WHERE institution_name = 'Universidad Complutense de Madrid';
UPDATE young_company.institutions SET institution_name = 'Stanford University' WHERE institution_name = 'Stanford University Graduate School of Education';
UPDATE young_company.institutions SET institution_name = 'Washington University in St. Louis' WHERE institution_name = 'Washington University in St Louis';
UPDATE young_company.institutions SET institution_name = 'Umea University' WHERE institution_name = 'Umeå universitet';
UPDATE young_company.institutions SET institution_name = 'Université de Tunis' WHERE institution_name = 'National Polytechnical School, Algiers, Algeria';
UPDATE young_company.institutions SET institution_name = 'Université de Sherbrooke' WHERE institution_name = 'University of sherbrooke, QC, Canada';
UPDATE young_company.institutions SET institution_name = 'University of Ljubljana' WHERE institution_name = 'University of Ljubljana, Biotechnical faculty';
UPDATE young_company.institutions SET institution_name = 'Christian-Albrechts-University zu Kiel' WHERE institution_name = 'Christian-Albrechts-Universität zu Kiel';
UPDATE young_company.institutions SET institution_name = 'Jawaharlal Nehru University' WHERE institution_name = 'Jawaharlal Nehru Technological University';
UPDATE young_company.institutions SET institution_name = 'San Francisco State University' WHERE institution_name = 'San Francisco State University. College of Extended Learning.';
UPDATE young_company.institutions SET institution_name = 'Universidad Politécnica de Madrid' WHERE institution_name = 'Universidad Politécnica de Madrid\
.';
UPDATE young_company.institutions SET institution_name = 'Universidad de La Sabana' WHERE institution_name = 'Universidad de la Sabana';
UPDATE young_company.institutions SET institution_name = 'Curtin University' WHERE institution_name = 'Curtin University of Technology';
UPDATE young_company.institutions SET institution_name = 'University of Basel' WHERE institution_name = 'Universität Basel';
UPDATE young_company.institutions SET institution_name = 'Lund University' WHERE institution_name = 'Lunds universitet';
UPDATE young_company.institutions SET institution_name = 'Goethe-University Frankfurt am Main' WHERE institution_name = 'Johann Wolfgang Goethe-Universität Frankfurt am Main';
UPDATE young_company.institutions SET institution_name = 'Universidade de Brasília' WHERE institution_name = 'Universidade Católica de Brasília';
UPDATE young_company.institutions SET institution_name = 'Universidad Autonoma de Yucatan' WHERE institution_name = 'Universidad Autónoma de Yucatán';
UPDATE young_company.institutions SET institution_name = "King Mongkut\''s Institute of Technology Ladkrabang" WHERE institution_name = "King Mongkut''s Institute of Technology Ladkrabang";
UPDATE young_company.institutions SET institution_name = 'Albert-Ludwigs-Universitaet Freiburg' WHERE institution_name = 'The University of Freiburg';
UPDATE young_company.institutions SET institution_name = 'Graz University of Technology' WHERE institution_name = 'Technical University Graz';
UPDATE young_company.institutions SET institution_name = 'Perm State National Research University' WHERE institution_name = 'Perm State Technical University';
UPDATE young_company.institutions SET institution_name = 'University of Siena' WHERE institution_name = 'Università di Siena';
UPDATE young_company.institutions SET institution_name = 'The New School' WHERE institution_name = 'New economic school';
UPDATE young_company.institutions SET institution_name = 'Purdue University' WHERE institution_name = 'Purdue University Krannert School of Management';
UPDATE young_company.institutions SET institution_name = 'Université de Montréal ' WHERE institution_name = 'University of Montreal';
UPDATE young_company.institutions SET institution_name = 'California Polytechnic State University' WHERE institution_name = 'California State Polytechnic University-Pomona';
UPDATE young_company.institutions SET institution_name = 'University of Witwatersrand' WHERE institution_name = 'University of the Witwatersrand';
UPDATE young_company.institutions SET institution_name = 'Delft University of Technology' WHERE institution_name = 'Technical university Delft';
UPDATE young_company.institutions SET institution_name = 'Belarusian State University of Informatics and Radioelectronics' WHERE institution_name = 'Belarussian State University of Informatics and Radioelectronics';
UPDATE young_company.institutions SET institution_name = 'Michigan State University' WHERE institution_name = 'University of Michigan Law School';
UPDATE young_company.institutions SET institution_name = 'University of Hawaiʻi at Mānoa' WHERE institution_name = 'University of Hawai‘i System';
UPDATE young_company.institutions SET institution_name = 'University of Delhi' WHERE institution_name = 'Delhi Public School Pune';
UPDATE young_company.institutions SET institution_name = 'University of British Columbia' WHERE institution_name = 'The University of British Columbia / UBC (Okanagan)';
UPDATE young_company.institutions SET institution_name = 'University of Pisa' WHERE institution_name = 'Università di Pisa / University of Pisa';
UPDATE young_company.institutions SET institution_name = 'Syracuse University' WHERE institution_name = 'SUNY ESF/Syracuse University';
UPDATE young_company.institutions SET institution_name = 'University of Colorado, Denver ' WHERE institution_name = 'University of Colorado at Denver';
UPDATE young_company.institutions SET institution_name = 'University of California, Berkeley' WHERE institution_name = 'UC Berkeley School of Information';
UPDATE young_company.institutions SET institution_name = 'Qatar University' WHERE institution_name = 'Georgetown University & Qatar University';
UPDATE young_company.institutions SET institution_name = 'Universitas Pendidikan Indonesia' WHERE institution_name = 'UNIVERSITAS PENDIDIKAN INDONESIA';
UPDATE young_company.institutions SET institution_name = 'University of Colorado Boulder' WHERE institution_name = 'University of Colorado at Boulder - Leeds School of Business';
UPDATE young_company.institutions SET institution_name = 'University of Notre Dame' WHERE institution_name = 'Notre Dame of Maryland University';
UPDATE young_company.institutions SET institution_name = 'Dartmouth College' WHERE institution_name = 'Thayer School of Engineering at Dartmouth';
UPDATE young_company.institutions SET institution_name = 'Ben-Gurion University of The Negev' WHERE institution_name = 'Ben Gurion university';
UPDATE young_company.institutions SET institution_name = 'California Polytechnic State University' WHERE institution_name = 'California State University, Stanislaus';
UPDATE young_company.institutions SET institution_name = 'Tecnológico de Monterrey' WHERE institution_name = 'Instituto Tecnológico y de Estudios Superiores de Monterrey';
UPDATE young_company.institutions SET institution_name = 'University of the Pacific' WHERE institution_name = 'University of Asia and the Pacific';
UPDATE young_company.institutions SET institution_name = 'Cornell University' WHERE institution_name = 'Cornell Law School';
UPDATE young_company.institutions SET institution_name = 'University of Maryland, College Park' WHERE institution_name = 'University of Maryland College Park\
BS \
2006';
UPDATE young_company.institutions SET institution_name = 'University of Ljubljana' WHERE institution_name = 'University of Ljubljana, Faculty of Arts';
UPDATE young_company.institutions SET institution_name = 'Wageningen University & Research' WHERE institution_name = 'Wageningen University';
UPDATE young_company.institutions SET institution_name = 'Tel Aviv University' WHERE institution_name = 'The Academic College of Tel-Aviv, Yaffo';
UPDATE young_company.institutions SET institution_name = 'Birla Institute of Technology and Science, Pilani' WHERE institution_name = 'Birla Institute of Technology and Science';
UPDATE young_company.institutions SET institution_name = 'University of Warsaw ' WHERE institution_name = 'University of Warsaw';
UPDATE young_company.institutions SET institution_name = 'The New School' WHERE institution_name = 'Modern School';
UPDATE young_company.institutions SET institution_name = 'Universidade Estadual de Campinas' WHERE institution_name = 'Universidade Estadual de Campinas (UNICAMP)';
UPDATE young_company.institutions SET institution_name = 'Universidade Federal do Rio de Janeiro' WHERE institution_name = 'Universidade Federal do Rio De Janeiro';
UPDATE young_company.institutions SET institution_name = 'Sorbonne University' WHERE institution_name = 'Sorbonne Université';
UPDATE young_company.institutions SET institution_name = 'The University of Exeter' WHERE institution_name = 'Exeter College, Oxford';
UPDATE young_company.institutions SET institution_name = 'Universidad de Antioquia' WHERE institution_name = 'Universidad de Antioquía';
UPDATE young_company.institutions SET institution_name = 'Universidad de San Andrés - UdeSA' WHERE institution_name = 'Universidad de ''San Andrés''';
UPDATE young_company.institutions SET institution_name = 'Universidad Austral' WHERE institution_name = 'Universidad Austral, Argentina';
UPDATE young_company.institutions SET institution_name = 'University of Madras' WHERE institution_name = 'Vidya Mandir Senior Secondary School';
UPDATE young_company.institutions SET institution_name = 'University of Hyderabad' WHERE institution_name = 'The Hyderabad Public School Ramanthapur - HPS (R)';
UPDATE young_company.institutions SET institution_name = 'Moscow Institute of Physics and Technology' WHERE institution_name = 'National Research University of Information Technologies, Mechanics and Optics. St. Petersburg';
UPDATE young_company.institutions SET institution_name = 'Birla Institute of Technology and Science, Pilani' WHERE institution_name = 'Birla Institute of Technology and Science - Pilani, Goa';
UPDATE young_company.institutions SET institution_name = 'University of Delhi' WHERE institution_name = 'Faculty of Management Studies - University of Delhi';
UPDATE young_company.institutions SET institution_name = 'Singapore Management University' WHERE institution_name = 'Management Development Institute of Singapore';
UPDATE young_company.institutions SET institution_name = 'Peter the Great St. Petersburg Polytechnic University' WHERE institution_name = 'St Petersburg State Polytechnic University';
UPDATE young_company.institutions SET institution_name = 'Tufts University' WHERE institution_name = 'Tufts';
UPDATE young_company.institutions SET institution_name = 'Universität Bremen' WHERE institution_name = 'Jacobs University Bremen / International University Bremen';
UPDATE young_company.institutions SET institution_name = 'Kaunas University of Technology' WHERE institution_name = 'Kaunas University of Technology Gymnasium';
UPDATE young_company.institutions SET institution_name = 'Istanbul University' WHERE institution_name = 'Istanbul Sehir University';
UPDATE young_company.institutions SET institution_name = 'Ain Shams University ' WHERE institution_name = 'Ain Shams University';
UPDATE young_company.institutions SET institution_name = 'Cornell University' WHERE institution_name = 'Cornell University Graduate School';
UPDATE young_company.institutions SET institution_name = 'Purdue University' WHERE institution_name = 'Purdue University Fort Wayne';
UPDATE young_company.institutions SET institution_name = 'Ben-Gurion University of The Negev' WHERE institution_name = 'Ben Gurion University (BGU)';
UPDATE young_company.institutions SET institution_name = 'Royal Holloway University of London' WHERE institution_name = 'Royal Holloway, University of London';
UPDATE young_company.institutions SET institution_name = 'Bauman Moscow State Technical University' WHERE institution_name = 'Bauman Moscow State Technical University (BMSTU)';
UPDATE young_company.institutions SET institution_name = 'Siberian Federal University, SibFU' WHERE institution_name = 'Siberian State University';
UPDATE young_company.institutions SET institution_name = 'University of Greenwich' WHERE institution_name = 'University of Greenwich - United Kingdom';
UPDATE young_company.institutions SET institution_name = 'Southwest University' WHERE institution_name = 'Southwestern University';
UPDATE young_company.institutions SET institution_name = 'Technion - Israel Institute of Technology' WHERE institution_name = 'Technion Israel Institute of Technology';
UPDATE young_company.institutions SET institution_name = 'Harbin Institute of Technology' WHERE institution_name = 'Harbin Institute of Technology at Weihai';
UPDATE young_company.institutions SET institution_name = 'Hacettepe University ' WHERE institution_name = 'Hacettepe University';
UPDATE young_company.institutions SET institution_name = 'KU Leuven' WHERE institution_name = 'KULeuven';
UPDATE young_company.institutions SET institution_name = 'University of Rome "Tor Vergata"' WHERE institution_name = 'University of Rome Tor Vergata';
UPDATE young_company.institutions SET institution_name = 'Universidad Pontificia Comillas' WHERE institution_name = 'Universidad Pontificia de Comillas';
UPDATE young_company.institutions SET institution_name = 'Universidade Federal de Pelotas ' WHERE institution_name = 'Federal University of Pelotas';
UPDATE young_company.institutions SET institution_name = 'University of Ottawa' WHERE institution_name = 'Ottawa University';
UPDATE young_company.institutions SET institution_name = 'Wesleyan University' WHERE institution_name = 'Indiana Wesleyan University';
UPDATE young_company.institutions SET institution_name = 'SOAS University of London ' WHERE institution_name = 'SOAS University of London';
UPDATE young_company.institutions SET institution_name = 'University of Murcia' WHERE institution_name = 'Universidad de Murcia';
UPDATE young_company.institutions SET institution_name = 'Complutense University of Madrid' WHERE institution_name = 'University of Madrid (UPM)';
UPDATE young_company.institutions SET institution_name = 'University of New Brunswick' WHERE institution_name = 'Brunswick School';
UPDATE young_company.institutions SET institution_name = 'Universidade de São Paulo' WHERE institution_name = 'University of Sao Paulo';
UPDATE young_company.institutions SET institution_name = 'University of Minho' WHERE institution_name = 'Universidade do Minho';
UPDATE young_company.institutions SET institution_name = 'Johns Hopkins University' WHERE institution_name = 'Hopkins School';
UPDATE young_company.institutions SET institution_name = 'University of Arkansas Fayetteville ' WHERE institution_name = 'University of Arkansas at Fayetteville';
UPDATE young_company.institutions SET institution_name = 'University of Cambridge' WHERE institution_name = 'Trinity College, University of Cambridge';
UPDATE young_company.institutions SET institution_name = 'Saint Petersburg Electrotechnical University ETU-LETI' WHERE institution_name = 'Saint Petersburg Electrotechnical University \LETI\""';
UPDATE young_company.institutions SET institution_name = 'Colorado State University' WHERE institution_name = 'Colorado State University-Global Campus';
UPDATE young_company.institutions SET institution_name = 'Taras Shevchenko National University of Kyiv' WHERE institution_name = 'Kiev National Taras Shevchenko University';
UPDATE young_company.institutions SET institution_name = 'Taras Shevchenko National University of Kyiv' WHERE institution_name = 'National Taras Shevchenko University, Kiev, Ukraine';
UPDATE young_company.institutions SET institution_name = 'Northwestern University' WHERE institution_name = 'Northwestern University / VIT University';
UPDATE young_company.institutions SET institution_name = 'New York University' WHERE institution_name = 'State University of New York';
UPDATE young_company.institutions SET institution_name = 'University of New Hampshire' WHERE institution_name = 'Hampshire College';
UPDATE young_company.institutions SET institution_name = 'University of Madras' WHERE institution_name = 'Madras University';
UPDATE young_company.institutions SET institution_name = 'KIT, Karlsruhe Institute of Technology' WHERE institution_name = 'Karlsruher Institut für Technologie (KIT)';
UPDATE young_company.institutions SET institution_name = 'Université Paris 1 Panthéon-Sorbonne ' WHERE institution_name = 'Université Panthéon Sorbonne';
UPDATE young_company.institutions SET institution_name = 'Universidade de São Paulo' WHERE institution_name = 'Universidade Metodista de São Paulo';
UPDATE young_company.institutions SET institution_name = 'University of Delhi' WHERE institution_name = 'Delhi Public School - India';
UPDATE young_company.institutions SET institution_name = 'SRM INSTITUTE OF SCIENCE AND TECHNOLOGY' WHERE institution_name = 'SRM Institute of Science and Technology';
UPDATE young_company.institutions SET institution_name = 'Khulna University of Engineering and Technology' WHERE institution_name = 'K.J.Somaiya College of Engineering';
UPDATE young_company.institutions SET institution_name = 'Indian Institute of Technology Hyderabad' WHERE institution_name = 'Indian Institute of Technology, Patna';
UPDATE young_company.institutions SET institution_name = 'University of Salford' WHERE institution_name = 'Salisbury University';
UPDATE young_company.institutions SET institution_name = 'University of Colombo' WHERE institution_name = 'Royal College Colombo';
UPDATE young_company.institutions SET institution_name = 'Pontificia Universidade Católica do Minas Gerais - PUC Minas' WHERE institution_name = 'Pontificia Universidade Católica de Minas Gerais';
UPDATE young_company.institutions SET institution_name = 'The University of Hong Kong' WHERE institution_name = 'Hong Kong University';
UPDATE young_company.institutions SET institution_name = 'University of Massachusetts Boston' WHERE institution_name = 'Massachusetts College of Liberal Arts';
UPDATE young_company.institutions SET institution_name = 'Harvard University' WHERE institution_name = 'Harvard Innovation Labs';
UPDATE young_company.institutions SET institution_name = 'Tecnológico de Monterrey' WHERE institution_name = 'Prepa Tec Monterrey';
UPDATE young_company.institutions SET institution_name = 'University of Chicago' WHERE institution_name = 'University of Chicago Law School';
UPDATE young_company.institutions SET institution_name = 'University at Buffalo SUNY' WHERE institution_name = 'Northwestern University/SUNY Buffalo';
UPDATE young_company.institutions SET institution_name = 'Northwest University' WHERE institution_name = 'Northwest A&F University';
UPDATE young_company.institutions SET institution_name = 'Bina Nusantara University' WHERE institution_name = 'Universitas Bina Nusantara (Binus)';
UPDATE young_company.institutions SET institution_name = 'Peter the Great St. Petersburg Polytechnic University' WHERE institution_name = 'Peter the Great St.Petersburg Polytechnic University';
UPDATE young_company.institutions SET institution_name = 'Saint Petersburg State University' WHERE institution_name = 'Saint Petersburg University of Telecommunications';
UPDATE young_company.institutions SET institution_name = 'The University of Tennessee, Knoxville' WHERE institution_name = 'University of Tennessee, Knoxville';
UPDATE young_company.institutions SET institution_name = 'Mendel University in Brno' WHERE institution_name = 'Mendel University';
UPDATE young_company.institutions SET institution_name = 'University of Toronto' WHERE institution_name = 'University of Toronto - Woodsworth College';
UPDATE young_company.institutions SET institution_name = 'Loughborough University' WHERE institution_name = 'Loughborough Grammar School';
UPDATE young_company.institutions SET institution_name = 'University at Albany SUNY' WHERE institution_name = 'University at Albany, SUNY';
UPDATE young_company.institutions SET institution_name = 'Carnegie Mellon University' WHERE institution_name = 'Carnegie Mellon University - Tepper School of Business';
UPDATE young_company.institutions SET institution_name = 'Université de Lille' WHERE institution_name = 'Université des Sciences et Technologies de Lille (Lille I)';
UPDATE young_company.institutions SET institution_name = 'Heriot-Watt University' WHERE institution_name = 'Heriot Watt University Edinburgh';
UPDATE young_company.institutions SET institution_name = 'The University of Edinburgh' WHERE institution_name = 'University of Edinburgh';
UPDATE young_company.institutions SET institution_name = 'Savitribai Phule Pune University' WHERE institution_name = 'College of Engineering Pune';
UPDATE young_company.institutions SET institution_name = 'University of Virginia' WHERE institution_name = 'The University of Virginia';
UPDATE young_company.institutions SET institution_name = 'Università degli studi Roma Tre' WHERE institution_name = 'Università degli Studi di Roma Tre';
UPDATE young_company.institutions SET institution_name = 'Cornell University' WHERE institution_name = 'Joan & Sanford I. Weill Medical College of Cornell University';
UPDATE young_company.institutions SET institution_name = 'The Ohio State University' WHERE institution_name = 'Ohio State University';
UPDATE young_company.institutions SET institution_name = 'Washington University in St. Louis' WHERE institution_name = 'Olin Business School at Washington University in St. Louis';
UPDATE young_company.institutions SET institution_name = 'Case Western Reserve University' WHERE institution_name = 'Western Reserve Academy';
UPDATE young_company.institutions SET institution_name = 'OSMANIA UNIVERSITY' WHERE institution_name = 'Osmania University';
UPDATE young_company.institutions SET institution_name = 'Indian Institute of Science' WHERE institution_name = 'Indian Institute of Sciences, Bangalore';
UPDATE young_company.institutions SET institution_name = 'Indian Institute of Technology Hyderabad' WHERE institution_name = 'International Institute of Information Technology, Hyderabad (IIIT-Hyderabad)';
UPDATE young_company.institutions SET institution_name = 'De La Salle University' WHERE institution_name = 'La Salle University';
UPDATE young_company.institutions SET institution_name = 'Università degli studi Roma Tre' WHERE institution_name = 'Università degli Studi di Napoli Federico II';
UPDATE young_company.institutions SET institution_name = 'Boston University' WHERE institution_name = 'Boston University Academy';
UPDATE young_company.institutions SET institution_name = 'University of Washington' WHERE institution_name = 'University of Washington School of Law';
UPDATE young_company.institutions SET institution_name = 'University of Illinois at Urbana-Champaign' WHERE institution_name = 'University of Illinois Urbana-Champaign';
UPDATE young_company.institutions SET institution_name = 'Albert-Ludwigs-Universitaet Freiburg' WHERE institution_name = 'Albert-Ludwig University Freiburg';
UPDATE young_company.institutions SET institution_name = 'University of Klagenfurt' WHERE institution_name = 'Alpen-Adria-University Klagenfurt';
UPDATE young_company.institutions SET institution_name = 'Universidade de Santiago de Compostela' WHERE institution_name = 'University of Santiago de Compostela';
UPDATE young_company.institutions SET institution_name = 'Université Jean Moulin Lyon 3' WHERE institution_name = 'Université Jean Moulin';
UPDATE young_company.institutions SET institution_name = 'University of St.Gallen' WHERE institution_name = 'Universität St.Gallen (HSG)';
UPDATE young_company.institutions SET institution_name = 'Queensland University of Technology' WHERE institution_name = 'QUT (Queensland University of Technology)';
UPDATE young_company.institutions SET institution_name = 'National Technical University of Ukraine "Igor Sikorsky Kyiv Polytechnic Institute"' WHERE institution_name = 'National Technical University of Ukraine ''Kyiv Polytechnic Institute''';
UPDATE young_company.institutions SET institution_name = 'Kwame Nkrumah University of Science and Technology' WHERE institution_name = 'Kwame Nkrumah'' University of Science and Technology, Kumasi';
UPDATE young_company.institutions SET institution_name = 'University of Copenhagen' WHERE institution_name = 'IT University of Copenhagen';
UPDATE young_company.institutions SET institution_name = 'University of the Punjab' WHERE institution_name = 'Punjab Engineering College';
UPDATE young_company.institutions SET institution_name = 'City University of New York' WHERE institution_name = 'City University of New York-Hunter College';
UPDATE young_company.institutions SET institution_name = 'International University of Business, Agriculture and Technology' WHERE institution_name = 'Jomo Kenyatta University of Agriculture and Technology';
UPDATE young_company.institutions SET institution_name = 'University of Calgary' WHERE institution_name = 'The University of Calgary';
UPDATE young_company.institutions SET institution_name = 'Bielefeld University' WHERE institution_name = 'Universität Bielefeld';
UPDATE young_company.institutions SET institution_name = 'Concordia University' WHERE institution_name = 'Concordia University, Montreal';
UPDATE young_company.institutions SET institution_name = 'University of Illinois at Chicago' WHERE institution_name = 'University of Illinois College of Medicine';
UPDATE young_company.institutions SET institution_name = 'Universidad de Costa Rica' WHERE institution_name = 'Universidad Latina de Costa Rica';
UPDATE young_company.institutions SET institution_name = 'University of Delhi' WHERE institution_name = 'Delhi Public School Bangalore North';
UPDATE young_company.institutions SET institution_name = 'Université Paris-Nanterre' WHERE institution_name = 'Université Paris Nanterre';
UPDATE young_company.institutions SET institution_name = 'University of Miami' WHERE institution_name = 'University of Miami Medical School';
UPDATE young_company.institutions SET institution_name = 'University Politehnica of Timisoara, UPT' WHERE institution_name = 'Politehnica University of Timisoara';
UPDATE young_company.institutions SET institution_name = 'University of New Mexico' WHERE institution_name = 'The University of New Mexico';
UPDATE young_company.institutions SET institution_name = 'National University of Singapore' WHERE institution_name = 'Institute of Systems Sciences, National University of Singapore';
UPDATE young_company.institutions SET institution_name = 'Moscow City University' WHERE institution_name = 'Moscow State University';
UPDATE young_company.institutions SET institution_name = 'Ankara Üniversitesi' WHERE institution_name = 'Ankara University';
UPDATE young_company.institutions SET institution_name = 'Friedrich-Alexander-Universität Erlangen-Nürnberg' WHERE institution_name = 'Friedrich-Alexander-University Erlangen-Nuremberg (Germany)';
UPDATE young_company.institutions SET institution_name = 'Eberhard Karls Universität Tübingen' WHERE institution_name = 'Eberhard-Karls-University Tübingen (Germany)';
UPDATE young_company.institutions SET institution_name = 'Universidad de Guadalajara' WHERE institution_name = 'University of Guadalajara (Mexico)';
UPDATE young_company.institutions SET institution_name = 'Philipps-Universität Marburg ' WHERE institution_name = 'Philipps';
UPDATE young_company.institutions SET institution_name = 'Macquarie University' WHERE institution_name = 'Macquarie Graduate School of Management';
UPDATE young_company.institutions SET institution_name = 'King Fahd University of Petroleum & Minerals' WHERE institution_name = 'King Fahd University of Petroleum & Minerals - KFUPM';
UPDATE young_company.institutions SET institution_name = 'New York University' WHERE institution_name = 'New York University School of Medicine';
UPDATE young_company.institutions SET institution_name = 'Johns Hopkins University' WHERE institution_name = 'Johns Hopkins University School of Advanced International Studies (SAIS)';
UPDATE young_company.institutions SET institution_name = 'University of Geneva' WHERE institution_name = 'Geneva University (Mérieux Foundation)';
UPDATE young_company.institutions SET institution_name = 'University of Twente' WHERE institution_name = 'Universiteit Twente';
UPDATE young_company.institutions SET institution_name = 'Russian Presidential Academy of National Economy and Public Administration' WHERE institution_name = 'Academy of National Economy under the Government of the Russian Federation';
UPDATE young_company.institutions SET institution_name = 'University of Missouri, Kansas City' WHERE institution_name = 'Missouri State University';
UPDATE young_company.institutions SET institution_name = 'Indian Institute of Technology Indore' WHERE institution_name = 'IIM Indore';
UPDATE young_company.institutions SET institution_name = 'Tokai University' WHERE institution_name = 'Tokai University\
\
2007';
UPDATE young_company.institutions SET institution_name = 'Newcastle University' WHERE institution_name = 'Newcastle University (UK)';
UPDATE young_company.institutions SET institution_name = 'The University of Manchester' WHERE institution_name = 'The University of Manchester 1st';
UPDATE young_company.institutions SET institution_name = 'University Paris 2 Panthéon-Assas' WHERE institution_name = 'Panthéon-Assas université';
UPDATE young_company.institutions SET institution_name = 'The University of Queensland' WHERE institution_name = 'University of Queensland';
UPDATE young_company.institutions SET institution_name = 'University of Klagenfurt' WHERE institution_name = 'Universität Klagenfurt';
UPDATE young_company.institutions SET institution_name = 'Charles Sturt University ' WHERE institution_name = 'Charles Sturt University';
UPDATE young_company.institutions SET institution_name = 'Tel Aviv University' WHERE institution_name = 'Tel-Aviv University';
UPDATE young_company.institutions SET institution_name = 'Texas A&M University' WHERE institution_name = 'Texas A&M University - Commerce';
UPDATE young_company.institutions SET institution_name = 'National University of Singapore' WHERE institution_name = 'Singapore National Academy';
UPDATE young_company.institutions SET institution_name = 'Boston College' WHERE institution_name = 'Boston College Carroll School of Management';
UPDATE young_company.institutions SET institution_name = 'California State University Long Beach' WHERE institution_name = 'Cal State Long Beach';
UPDATE young_company.institutions SET institution_name = 'University of Michigan-Ann Arbor' WHERE institution_name = 'University of Michigan Medical School';
UPDATE young_company.institutions SET institution_name = 'University of Hyderabad' WHERE institution_name = 'JNTUH College of Engineering Hyderabad';
UPDATE young_company.institutions SET institution_name = 'Fundación Universidad De Bogotá-Jorge Tadeo Lozano' WHERE institution_name = 'Universidad Jorge Tadeo Lozano';
UPDATE young_company.institutions SET institution_name = 'University of Virginia' WHERE institution_name = 'University of Virginia School of Law';
UPDATE young_company.institutions SET institution_name = 'California State University Long Beach' WHERE institution_name = 'California State University-Long Beach';
UPDATE young_company.institutions SET institution_name = 'University Paris 2 Panthéon-Assas' WHERE institution_name = 'Panthéon Assas Paris University';
UPDATE young_company.institutions SET institution_name = 'Keio University' WHERE institution_name = 'Keio Univercity';
UPDATE young_company.institutions SET institution_name = 'American University in Dubai' WHERE institution_name = 'American School of Dubai';
UPDATE young_company.institutions SET institution_name = 'Texas A&M University' WHERE institution_name = 'Texas A&M University-Corpus Christi';
UPDATE young_company.institutions SET institution_name = 'Birla Institute of Technology and Science, Pilani' WHERE institution_name = 'Birla Institute of Technology & Science';
UPDATE young_company.institutions SET institution_name = 'Universitat Politècnica de València' WHERE institution_name = 'Universitat Politècnica de València (UPV)';
UPDATE young_company.institutions SET institution_name = 'Université de Rennes 1' WHERE institution_name = 'Université de Rennes I';
UPDATE young_company.institutions SET institution_name = 'Amity University' WHERE institution_name = 'Amity International School';
UPDATE young_company.institutions SET institution_name = 'Universidade Federal do Ceará' WHERE institution_name = 'IFCE (Instituto Federal do Ceará)';
UPDATE young_company.institutions SET institution_name = 'Moscow City University' WHERE institution_name = 'Moscow State School 57';
UPDATE young_company.institutions SET institution_name = 'Universität Duisburg-Essen' WHERE institution_name = 'University of Duisburg-Essen (Germany)';
UPDATE young_company.institutions SET institution_name = 'Erasmus University Rotterdam ' WHERE institution_name = 'Erasmus Universiteit Rotterdam';
UPDATE young_company.institutions SET institution_name = 'Azerbaijan State Oil and Industry University' WHERE institution_name = 'Azerbaijan State Oil Academy';
UPDATE young_company.institutions SET institution_name = 'Washington State University' WHERE institution_name = 'Washington College';
UPDATE young_company.institutions SET institution_name = 'University of Canberra ' WHERE institution_name = 'University of Canberra';
UPDATE young_company.institutions SET institution_name = 'University of Alabama, Birmingham' WHERE institution_name = 'University of Alabama at Birmingham';
UPDATE young_company.institutions SET institution_name = 'Université du Québec' WHERE institution_name = 'Université du Québec à Montréal';
UPDATE young_company.institutions SET institution_name = 'CUNY The City College of New York' WHERE institution_name = 'The City College of New York';
UPDATE young_company.institutions SET institution_name = 'Zhejiang University' WHERE institution_name = 'zhejiang university city collega';
UPDATE young_company.institutions SET institution_name = 'University of New Mexico' WHERE institution_name = 'New Mexico State University';
UPDATE young_company.institutions SET institution_name = 'National Technical University of Ukraine "Igor Sikorsky Kyiv Polytechnic Institute"' WHERE institution_name = 'National Technical University of Ukraine';
UPDATE young_company.institutions SET institution_name = 'Aristotle University of Thessaloniki' WHERE institution_name = 'Aristotle University Thessaloniki';
UPDATE young_company.institutions SET institution_name = 'Moscow Institute of Physics and Technology' WHERE institution_name = 'Bogomoletz Institute of Physiology (Kiev, Ukraine), Moscow Institute of Physics and Technology';
UPDATE young_company.institutions SET institution_name = 'Indian Institute of Technology Hyderabad' WHERE institution_name = 'International Institute of Information Technology - Hyderabad';
UPDATE young_company.institutions SET institution_name = 'The University of Tokyo' WHERE institution_name = 'University of Tokyo';
UPDATE young_company.institutions SET institution_name = 'Technische Universität Braunschweig' WHERE institution_name = 'Technische Universität Carolo-Wilhelmina zu Braunschweig';
UPDATE young_company.institutions SET institution_name = 'Fordham University ' WHERE institution_name = 'Fordham University';
UPDATE young_company.institutions SET institution_name = 'University of Waikato' WHERE institution_name = 'The University of Waikato';
UPDATE young_company.institutions SET institution_name = 'Universidade Federal do Rio de Janeiro' WHERE institution_name = 'Federal University of the State of Rio de Janeiro';
UPDATE young_company.institutions SET institution_name = 'University of Westminster' WHERE institution_name = 'Westminster School';
UPDATE young_company.institutions SET institution_name = 'High School' WHERE institution_name = 'Higher School of Economics';
UPDATE young_company.institutions SET institution_name = 'High School' WHERE institution_name = 'St. George Higher Secondary School (HS)';
UPDATE young_company.institutions SET institution_name = 'High School' WHERE institution_name = 'Homestead Highschool';
UPDATE young_company.institutions SET institution_name = 'High School' WHERE institution_name = 'M. P. Birla Foundation Higher Secondary School';
UPDATE young_company.institutions SET institution_name = 'High School' WHERE institution_name = 'Glory Highschool';
UPDATE young_company.institutions SET institution_name = 'V. N. Karazin Kharkiv National University' WHERE institution_name = 'Kharkiv National University';
UPDATE young_company.institutions SET institution_name = 'Manchester Metropolitan University' WHERE institution_name = 'Manchester University';
UPDATE young_company.institutions SET institution_name = "Queen's University Belfast" WHERE institution_name = "Queen's University";
UPDATE young_company.institutions SET institution_name = 'V. N. Karazin Kharkiv National University' WHERE institution_name = 'Kharkiv National University';

UPDATE young_company.institutions SET institution_name = 'UCL' WHERE institution_name = 'University College London';
UPDATE young_company.institutions SET institution_name = 'Universidad Iberoamericana IBERO' WHERE institution_name = 'Universidad Iberoamericana, Ciudad de México';
UPDATE young_company.institutions SET institution_name = 'Indian Institute of Technology Roorkee' WHERE institution_name = 'IIT Roorkee';
UPDATE young_company.institutions SET institution_name = 'IE University' WHERE institution_name = 'IE Business School';


SET SQL_SAFE_UPDATES = 1;

-- SELECT * FROM program
-- INTO OUTFILE '/private/tmp/programs_founder.csv'
-- FIELDS TERMINATED BY ',' 
-- ENCLOSED BY '"'
-- LINES TERMINATED BY '\n';

-- ===========================
-- SECTION 4: ETL 
-- This section generate analytical layer tables
-- ===========================
SET SQL_SAFE_UPDATES = 0;

SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'young_company'
ORDER BY TABLE_NAME, ORDINAL_POSITION;
DELETE FROM program 
WHERE program_category IS NULL;
SET SQL_SAFE_UPDATES = 1;

-- --------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- --------------------------------------------------------------------------------------------------------------------------------------------------------------------

SET SESSION group_concat_max_len = 10000;

SET SQL_SAFE_UPDATES = 0;

UPDATE industry
SET IndustryName = REPLACE(REPLACE(TRIM(REPLACE(REPLACE(IndustryName, ' ', '_'), '-', '_')), ' ', ''), ',', '');
UPDATE industry
SET IndustryName = UPPER(IndustryName);

UPDATE program
SET program_category = REPLACE(REPLACE(TRIM(REPLACE(REPLACE(program_category, ' ', '_'), '-', '_')), ' ', ''), ',', '');
UPDATE program
SET program_category = UPPER(program_category);
UPDATE program
SET program_category = REPLACE(
                          REPLACE(
                              REPLACE(program_category, CHAR(13), ''),  -- Remove carriage returns
                              CHAR(10), ''),                           -- Remove line feeds
                          '\r\n', '');                               -- Remove Windows-style line breaks

-- ---------------------------------
DROP TABLE IF EXISTS company_analytics;

SET @dynamic_sql = NULL;

SELECT 
    GROUP_CONCAT(CONCAT(REPLACE(column_name, ' ', '_'), ' TINYINT(1) DEFAULT 0') SEPARATOR ', ') 
INTO @dynamic_sql
FROM (
    SELECT REPLACE(i.IndustryName, ' ', '_') AS column_name
    FROM (SELECT DISTINCT IndustryName FROM industry) AS i
    UNION ALL
    SELECT REPLACE(p.program_category, ' ', '_') AS column_name
    FROM (SELECT DISTINCT program_category FROM program) AS p
) AS combined_columns;

SELECT @dynamic_sql;


SET @dynamic_sql = CONCAT('
    CREATE TABLE IF NOT EXISTS company_analytics (
        CompanyId INT PRIMARY KEY,
        CompanyName VARCHAR(50),
        SuccessIndex INT,
        Region VARCHAR(100),
        Country VARCHAR(100),
        FounderPriorCompanies INT,
        FirstEnrollmentYear INT,
        TotalEnrollmentYears INT,
        CompanyTags VARCHAR(500),
        CompanyHighlight VARCHAR(1000),
        Founders VARCHAR(1000),
        UniPrograms VARCHAR(1000),
        ProgramCategory VARCHAR (1000),
        Institutions VARCHAR(1000),
        UniHighestRank INT,
        CompanyStatus VARCHAR(50),
        CompanySlug VARCHAR(50),
        Industries VARCHAR (500),
        ', @dynamic_sql, '
    )
');

PREPARE stmt FROM @dynamic_sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;


DROP PROCEDURE IF EXISTS ETL_Company_Analytics;
DELIMITER $$

CREATE PROCEDURE ETL_Company_Analytics()
BEGIN
    TRUNCATE TABLE company_analytics;

    INSERT INTO company_analytics (
        CompanyId, CompanyName, SuccessIndex, Region, Country, FounderPriorCompanies, 
        FirstEnrollmentYear, TotalEnrollmentYears, Industries, CompanyTags, CompanyHighlight, Founders, 
        UniPrograms, ProgramCategory, Institutions, UniHighestRank, CompanyStatus, CompanySlug
    )
    SELECT 
        c.CompanyId,
        LEFT(c.CompanyName, 50),  
        CASE
            WHEN c.status = 'public' THEN 2
            WHEN c.status = 'active' THEN 1
            ELSE 1
        END 
        + 
        CASE
            WHEN MAX(f.top_company) = 1 THEN 2
            ELSE 0
        END
        +
        CASE
            WHEN FIND_IN_SET('topCompany', GROUP_CONCAT(DISTINCT b.Badge)) > 0 THEN 3
            WHEN FIND_IN_SET('isHiring', GROUP_CONCAT(DISTINCT b.Badge)) > 0 THEN 2
            WHEN GROUP_CONCAT(DISTINCT b.Badge) IS NOT NULL THEN 1
            ELSE 0
        END AS SuccessIndex,
        
        MAX(r.region) AS Region,  
        MAX(r.country) AS Country, 
        MAX(priorcompanycount.FounderPriorCompanies) AS FounderPriorCompanies,
        MIN(e.year) AS FirstEnrollmentYear,   
        COUNT(DISTINCT e.year) AS TotalEnrollmentYears, 
        
        LEFT(GROUP_CONCAT(DISTINCT i.IndustryName), 500) AS Industries,  
        LEFT(GROUP_CONCAT(DISTINCT t.tagName), 500) AS Tags,            
        LEFT(GROUP_CONCAT(DISTINCT b.Badge), 1000) AS Badges,          
        LEFT(GROUP_CONCAT(DISTINCT CONCAT(f.first_name, ' ', f.last_name)), 1000) AS Founders,  
        LEFT(GROUP_CONCAT(DISTINCT p.program_name), 1000) AS UniPrograms,  
        LEFT(GROUP_CONCAT(DISTINCT p.program_category), 1000) AS ProgramsCategory, 
        LEFT(GROUP_CONCAT(DISTINCT inst.institution_name), 1000) AS Institutions, 
		
        MAX(qs.rank_2024) AS UniHighestRank,   -- Fetch the highest rank of institution from QS
        
        LEFT(c.status, 50) AS Status,
        LEFT(c.slug, 50) AS slug 
    FROM company c
    LEFT JOIN regions r ON c.CompanyId = r.CompanyId
    LEFT JOIN company_industry ci ON c.CompanyId = ci.CompanyId
    LEFT JOIN industry i ON ci.IndustryId = i.IndustryId
    LEFT JOIN company_tag ct ON c.CompanyId = ct.CompanyId
    LEFT JOIN tag t ON ct.tagId = t.tagId
    LEFT JOIN companybadge cb ON c.CompanyId = cb.CompanyId
    LEFT JOIN badge b ON cb.BadgeId = b.BadgeId
    LEFT JOIN founders f ON c.slug = f.company_slug
    LEFT JOIN prior_companies pc ON f.hnid = pc.hnid
    LEFT JOIN enrollment e ON f.hnid = e.hnid
    LEFT JOIN institutions inst ON e.institution_id = inst.institution_id
    LEFT JOIN program p ON e.program_id = p.program_id
    LEFT JOIN (
        SELECT f.hnid, COUNT(pc.hnid) AS FounderPriorCompanies
        FROM founders f
        LEFT JOIN prior_companies pc ON f.hnid = pc.hnid
        GROUP BY f.hnid
    ) AS priorcompanycount ON f.hnid = priorcompanycount.hnid
    
    LEFT JOIN qs_rank qs ON inst.institution_name = qs.institution 
    
    GROUP BY c.CompanyId;
END $$

DELIMITER ;

DROP PROCEDURE IF EXISTS Update_Industry_Columns;
DELIMITER $$

CREATE PROCEDURE Update_Industry_Columns()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE IndustryName VARCHAR(255);
    DECLARE industry_cursor CURSOR FOR 
        SELECT DISTINCT i.IndustryName 
        FROM industry i;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN industry_cursor;

    read_loop: LOOP
        FETCH industry_cursor INTO IndustryName;

        IF done THEN
            LEAVE read_loop; 
        END IF;

        SET @query = CONCAT('
            UPDATE company_analytics 
            SET ', IndustryName, ' = CASE 
                WHEN CompanyId IN (
                    SELECT ci.CompanyId 
                    FROM company_industry ci 
                    JOIN industry i ON ci.IndustryId = i.IndustryId 
                    WHERE i.IndustryName = ''', IndustryName, '''
                ) 
                THEN 1 ELSE 0 END
        ');

        PREPARE stmt FROM @query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    
		SET @rename_query = CONCAT(
            'ALTER TABLE company_analytics CHANGE ', 
            IndustryName, ' IDTY_', IndustryName, ' INT'
        );

        PREPARE rename_stmt FROM @rename_query;
        EXECUTE rename_stmt;
        DEALLOCATE PREPARE rename_stmt;
    
    END LOOP;

    CLOSE industry_cursor; 
END $$

DELIMITER ;



DROP PROCEDURE IF EXISTS Update_Program_Columns;
DELIMITER $$

CREATE PROCEDURE Update_Program_Columns()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE program_category VARCHAR(255);
    
    DECLARE program_cursor CURSOR FOR 
        SELECT DISTINCT p.program_category 
        FROM program p;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    OPEN program_cursor;

    read_loop: LOOP
        FETCH program_cursor INTO program_category;

        IF done THEN
            LEAVE read_loop; 
        END IF;

       SET @query = CONCAT('UPDATE company_analytics ca 
							SET ca.', program_category, ' = CASE 
							WHEN EXISTS (
								SELECT 1 
								FROM enrollment e 
								JOIN program p ON e.program_id = p.program_id 
								JOIN founders f ON e.hnid = f.hnid 
								WHERE p.program_category = ''', program_category, ''' 
								AND f.company_slug = ca.CompanySlug
							) 
							THEN 1 ELSE 0 END'
        );
        

        PREPARE stmt FROM @query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        SET @rename_query = CONCAT(
            'ALTER TABLE company_analytics CHANGE ', 
            program_category, ' UNI_', 
            program_category, ' INT'
        );


        PREPARE rename_stmt FROM @rename_query;
        EXECUTE rename_stmt;
        DEALLOCATE PREPARE rename_stmt;

    END LOOP;

    CLOSE program_cursor; 
END $$

DELIMITER ;


CALL ETL_Company_Analytics();
CALL Update_Industry_Columns();
CALL Update_Program_Columns();

SET SQL_SAFE_UPDATES = 1;

SELECT * FROM young_company.company_analytics;



DROP PROCEDURE IF EXISTS update_company_analytics_after_insert;

DELIMITER //

DROP TRIGGER IF EXISTS update_company_analytics_after_insert;

DELIMITER //

CREATE TRIGGER update_company_analytics_after_insert
AFTER INSERT ON company
FOR EACH ROW
BEGIN
    IF NOT EXISTS (SELECT 1 FROM company_analytics WHERE CompanyId = NEW.CompanyId) THEN
        INSERT INTO company_analytics (
            CompanyId, 
            CompanyName, 
            CompanyStatus, 
            CompanySlug, 
            CompanyTags, 
            SuccessIndex, 
            Region
        ) VALUES (
            NEW.CompanyId, 
            NEW.CompanyName, 
            NEW.status, 
            NEW.slug, 
            (SELECT GROUP_CONCAT(tagName) 
             FROM company_tag 
             INNER JOIN tag ON company_tag.tagId = tag.tagId 
             WHERE company_tag.CompanyId = NEW.CompanyId),
            0,  
            (SELECT region FROM regions WHERE CompanyId = NEW.CompanyId LIMIT 1)
        );
        
    ELSE
        UPDATE company_analytics
        SET CompanyName = NEW.CompanyName,
            CompanyStatus = NEW.status,
            CompanySlug = NEW.slug,
            CompanyTags = (SELECT GROUP_CONCAT(tagName) 
                           FROM company_tag 
                           INNER JOIN tag ON company_tag.tagId = tag.tagId 
                           WHERE company_tag.CompanyId = NEW.CompanyId),
            Region = (SELECT region FROM regions WHERE CompanyId = NEW.CompanyId LIMIT 1)
        WHERE CompanyId = NEW.CompanyId;
    END IF;
    
END //

DELIMITER ;

-- INSERT INTO company (CompanyId, CompanyName, status, slug) VALUES (29992, 'Company C', 'Active', 'company-c');


-- ===========================
-- SECTION 5: Procedures
-- ===========================

DROP PROCEDURE IF EXISTS GetSuccessfulStartups;
DELIMITER $$

CREATE PROCEDURE GetSuccessfulStartups(
    IN p_FilterType ENUM('University', 'Industry'),
    IN p_FilterValue VARCHAR(255)
)
BEGIN
    IF p_FilterType = 'University' THEN
        SELECT 
            inst.institution_name AS University,
            COUNT(DISTINCT f.hnid) AS SuccessfulFoundersCount, 
            SUM(ca.SuccessIndex) AS TotalSuccessIndex
        FROM 
            young_company.company_analytics ca
        JOIN 
            young_company.founders f ON ca.CompanySlug = f.company_slug
        JOIN 
            young_company.enrollment e ON f.hnid = e.hnid
        JOIN 
            young_company.institutions inst ON e.institution_id = inst.institution_id
        WHERE 
            ca.SuccessIndex > 0 
            AND inst.institution_name = p_FilterValue
        GROUP BY 
            inst.institution_name;

    ELSEIF p_FilterType = 'Industry' THEN
        SELECT 
            i.IndustryName AS Industry,
            COUNT(DISTINCT ca.CompanyId) AS SuccessfulStartupsCount,
            SUM(ca.SuccessIndex) AS TotalSuccessIndex
        FROM 
            young_company.company_analytics ca
        JOIN 
            young_company.company_industry ci ON ca.CompanyId = ci.CompanyId
        JOIN 
            young_company.industry i ON ci.IndustryId = i.IndustryId
        WHERE 
            ca.SuccessIndex > 0  
            AND i.IndustryName = p_FilterValue
        GROUP BY 
            i.IndustryName;

    ELSE
        SELECT 'Invalid filter type. Use ''University'' or ''Industry''.' AS ErrorMessage;
    END IF;
END$$

DELIMITER ;



DROP PROCEDURE IF EXISTS GetUniqueUniversities;
DELIMITER $$

CREATE PROCEDURE GetUniqueUniversities()
BEGIN
    SELECT DISTINCT institution_name AS University
    FROM young_company.institutions;
END $$

DELIMITER ;

DROP PROCEDURE IF EXISTS GetUniqueIndustries;
DELIMITER $$

CREATE PROCEDURE GetUniqueIndustries()
BEGIN
    SELECT DISTINCT IndustryName AS Industry
    FROM young_company.industry;
END $$

DELIMITER ;


DROP PROCEDURE IF EXISTS RankUniversitiesBySuccessIndex;
DELIMITER $$

CREATE PROCEDURE RankUniversitiesBySuccessIndex()
BEGIN
    SELECT 
        inst.institution_name AS University,
        COUNT(DISTINCT f.hnid) AS SuccessfulFoundersCount, 
        SUM(ca.SuccessIndex) AS TotalSuccessIndex
    FROM 
        young_company.company_analytics ca
    JOIN 
        young_company.founders f ON ca.CompanySlug = f.company_slug
    JOIN 
        young_company.enrollment e ON f.hnid = e.hnid
    JOIN 
        young_company.institutions inst ON e.institution_id = inst.institution_id
    WHERE 
        ca.SuccessIndex IS NOT NULL  
    GROUP BY 
        inst.institution_name
    ORDER BY 
        TotalSuccessIndex DESC;  
END $$

DELIMITER ;


DROP PROCEDURE IF EXISTS RankIndustriesBySuccessIndex;
DELIMITER $$

CREATE PROCEDURE RankIndustriesBySuccessIndex()
BEGIN
    SELECT 
        i.IndustryName AS Industry,
        COUNT(DISTINCT f.hnid) AS SuccessfulFoundersCount,  
        SUM(ca.SuccessIndex) AS TotalSuccessIndex
    FROM 
        young_company.company_analytics ca
    JOIN 
        young_company.founders f ON ca.CompanySlug = f.company_slug
    JOIN 
        young_company.company_industry ci ON ca.CompanyId = ci.CompanyId
    JOIN 
        young_company.industry i ON ci.IndustryId = i.IndustryId
    WHERE 
        ca.SuccessIndex IS NOT NULL  
    GROUP BY 
        i.IndustryName
    ORDER BY 
        TotalSuccessIndex DESC;  
END $$

DELIMITER ;




DROP PROCEDURE IF EXISTS GetUniqueCountries;
DELIMITER $$

CREATE PROCEDURE GetUniqueCountries()
BEGIN
    SELECT DISTINCT 
        r.country AS Country
    FROM 
        young_company.regions r
    ORDER BY 
        r.country; 
END $$

DELIMITER ;


DROP PROCEDURE IF EXISTS GetUniqueRegions;
DELIMITER $$

CREATE PROCEDURE GetUniqueRegions()
BEGIN
    SELECT DISTINCT 
        r.region AS Region
    FROM 
        young_company.regions r
    ORDER BY 
        r.region;  
END $$

DELIMITER ;


DROP PROCEDURE IF EXISTS RankCountriesAndRegions;
DELIMITER $$

CREATE PROCEDURE RankCountriesAndRegions(
    IN p_FilterType ENUM('Country', 'Region'),
    IN p_FilterValue VARCHAR(255)
)
BEGIN
    DROP TEMPORARY TABLE IF EXISTS CountryRankings;
    DROP TEMPORARY TABLE IF EXISTS RegionRankings;

    CREATE TEMPORARY TABLE CountryRankings AS
    SELECT 
        r.country,
        COUNT(DISTINCT ca.CompanyId) AS SuccessfulCompaniesCount,  
        SUM(ca.SuccessIndex) AS TotalSuccessIndex,  
        RANK() OVER (ORDER BY SUM(ca.SuccessIndex) DESC) AS Ranking
    FROM 
        young_company.company_analytics ca
    JOIN 
        young_company.regions r ON ca.CompanyId = r.CompanyId
    WHERE 
        ca.SuccessIndex > 0  
        AND (p_FilterType = 'Country' AND (r.country = p_FilterValue OR p_FilterValue IS NULL)) 
    GROUP BY 
        r.country;

    CREATE TEMPORARY TABLE RegionRankings AS
    SELECT 
        r.region,
        COUNT(DISTINCT ca.CompanyId) AS SuccessfulCompaniesCount,  
        SUM(ca.SuccessIndex) AS TotalSuccessIndex, 
        RANK() OVER (ORDER BY SUM(ca.SuccessIndex) DESC) AS Ranking 
    FROM 
        young_company.company_analytics ca
    JOIN 
        young_company.regions r ON ca.CompanyId = r.CompanyId
    WHERE 
        ca.SuccessIndex > 0  
        AND (p_FilterType = 'Region' AND (r.region = p_FilterValue OR p_FilterValue IS NULL)) 
    GROUP BY 
        r.region;

    IF p_FilterType = 'Country' THEN
        SELECT 
            country, 
            SuccessfulCompaniesCount, 
            TotalSuccessIndex, 
            Ranking
        FROM 
            CountryRankings;
    END IF;

    IF p_FilterType = 'Region' THEN
        SELECT 
            region, 
            SuccessfulCompaniesCount, 
            TotalSuccessIndex, 
            Ranking
        FROM 
            RegionRankings;
    END IF;

END $$

DELIMITER ;

DROP PROCEDURE IF EXISTS RankUniversitiesRankingBySuccessIndex;
DELIMITER $$

CREATE PROCEDURE RankUniversitiesRankingBySuccessIndex()
BEGIN
    SELECT 
        ca.UniHighestRank AS Ranking2024,
        COUNT(DISTINCT f.hnid) AS SuccessfulFoundersCount, 
        SUM(ca.SuccessIndex) AS TotalSuccessIndex
    FROM 
        young_company.company_analytics ca
    JOIN 
        young_company.founders f ON ca.CompanySlug = f.company_slug
    JOIN 
        young_company.enrollment e ON f.hnid = e.hnid
    JOIN 
        young_company.institutions inst ON e.institution_id = inst.institution_id
    WHERE 
        ca.SuccessIndex IS NOT NULL  
    GROUP BY 
        ca.UniHighestRank
    ORDER BY 
        TotalSuccessIndex DESC;  
END $$

DELIMITER ;
DROP PROCEDURE IF EXISTS RankProgramCategoryBySuccessIndex;
DELIMITER $$

CREATE PROCEDURE RankProgramCategoryBySuccessIndex(IN input_program_category VARCHAR(255))
BEGIN
    DECLARE dynamic_query TEXT DEFAULT '';
    SET @input_program_category = input_program_category;
    SET @escaped_column_name = input_program_category;
    SELECT GROUP_CONCAT(CONCAT('IFNULL(', COLUMN_NAME, ', 0)') SEPARATOR ' + ') 
    INTO @dynamic_query
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'company_analytics'
      AND COLUMN_NAME LIKE CONCAT(@escaped_column_name, '%');

    IF @dynamic_query IS NULL OR @dynamic_query = '' THEN
        SIGNAL SQLSTATE '45000' 
        SET MESSAGE_TEXT = 'No matching columns found for the provided program category.';
    END IF;

    SET @final_query = CONCAT('
        SELECT "', @input_program_category, '" AS UniProgram, SUM(', @dynamic_query, ') AS TotalSuccessIndex
        FROM young_company.company_analytics ca
        WHERE ', @escaped_column_name, ' = 1;');

    -- Display the query for debugging (optional)
    SELECT @input_program_category;

    -- Prepare and execute the dynamic query
    PREPARE stmt FROM @final_query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END $$

DELIMITER ;

DROP PROCEDURE IF EXISTS RankProgramCategoryBySuccessIndexALL;
DELIMITER $$

CREATE PROCEDURE RankProgramCategoryBySuccessIndexALL()
BEGIN
    DECLARE dynamic_query TEXT DEFAULT '';
    
    -- Construct the dynamic SQL query to get success index for each UNI_ column
    SELECT GROUP_CONCAT(
               CONCAT('SELECT "', COLUMN_NAME, '" AS UniProgram, SUM(IFNULL(', COLUMN_NAME, ', 0)) AS TotalSuccessIndex FROM young_company.company_analytics')
           SEPARATOR ' UNION ALL ')
    INTO @dynamic_query
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'company_analytics'
		AND COLUMN_NAME LIKE BINARY 'UNI\_%';
    -- Check if any columns were found
    IF @dynamic_query IS NULL OR @dynamic_query = '' THEN
        SIGNAL SQLSTATE '45000' 
        SET MESSAGE_TEXT = 'No matching columns found with prefix UNI_.';
    END IF;

    -- Add outer query to order the results in descending order
    SET @final_query = CONCAT('SELECT * FROM (', @dynamic_query, ') AS UniSuccessIndex ORDER BY TotalSuccessIndex DESC;');

    -- Prepare and execute the dynamic query
    PREPARE stmt FROM @final_query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END $$

DELIMITER ;

CALL GetUniqueRegions(); 
CALL GetUniqueCountries();
CALL GetUniqueUniversities(); 
CALL GetUniqueIndustries();    
CALL RankUniversitiesRankingBySuccessIndex();
CALL RankProgramCategoryBySuccessIndex('UNI_SOFTWARE_AND_COMPUTER_ENGINEERING');
CALL RankProgramCategoryBySuccessIndexALL();
CALL RankUniversitiesBySuccessIndex();  
CALL RankIndustriesBySuccessIndex();   
CALL GetSuccessfulStartups('University', 'Newcastle University'); -- example calls
CALL GetSuccessfulStartups('Industry', 'Aviation and Space'); -- example calls
CALL RankCountriesAndRegions('Country', 'United States of America');  
CALL RankCountriesAndRegions('Region', 'Uganda');  
CALL RankCountriesAndRegions('Country', NULL); 
CALL RankCountriesAndRegions('Region', NULL); 



-- ===========================
-- SECTION 6: Creating Data Marts
-- ===========================


DELIMITER $$
DROP PROCEDURE IF EXISTS CreateRankView$$
CREATE PROCEDURE CreateRankView()
BEGIN
    DECLARE dynamic_query TEXT DEFAULT '';
    SELECT GROUP_CONCAT(
               CONCAT('SELECT "', COLUMN_NAME, '" AS UniProgram, SUM(IFNULL(', COLUMN_NAME, ', 0)) AS TotalSuccessIndex FROM young_company.company_analytics')
           SEPARATOR ' UNION ALL ')
    INTO dynamic_query
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'company_analytics'
        AND COLUMN_NAME LIKE BINARY 'UNI\_%';
    
    IF dynamic_query IS NULL OR dynamic_query = '' THEN
        SIGNAL SQLSTATE '45000' 
        SET MESSAGE_TEXT = 'No matching columns found with prefix UNI_.';
    END IF;

    SET @final_query = CONCAT('CREATE OR REPLACE VIEW view_RankProgramCategoryBySuccessIndex AS (', dynamic_query, ' ORDER BY TotalSuccessIndex DESC);');
    PREPARE stmt FROM @final_query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

END$$
DELIMITER ;
CALL CreateRankView();
DROP PROCEDURE IF EXISTS CreateRankView;



DROP VIEW IF EXISTS view_RankUniversitiesBySuccessIndex;

CREATE VIEW view_RankUniversitiesBySuccessIndex AS
SELECT 
    inst.institution_name AS University,
    COUNT(DISTINCT f.hnid) AS SuccessfulFoundersCount, 
    SUM(ca.SuccessIndex) AS TotalSuccessIndex
FROM 
    young_company.company_analytics ca
JOIN 
    young_company.founders f ON ca.CompanySlug = f.company_slug
JOIN 
    young_company.enrollment e ON f.hnid = e.hnid
JOIN 
    young_company.institutions inst ON e.institution_id = inst.institution_id
WHERE 
    ca.SuccessIndex IS NOT NULL  
GROUP BY 
    inst.institution_name
ORDER BY 
    TotalSuccessIndex DESC;
    
    
    
    
DROP VIEW IF EXISTS view_RankIndustriesBySuccessIndex;

CREATE VIEW view_RankIndustriesBySuccessIndex AS
SELECT 
    i.IndustryName AS Industry,
    COUNT(DISTINCT f.hnid) AS SuccessfulFoundersCount,  
    SUM(ca.SuccessIndex) AS TotalSuccessIndex
FROM 
    young_company.company_analytics ca
JOIN 
    young_company.founders f ON ca.CompanySlug = f.company_slug
JOIN 
    young_company.company_industry ci ON ca.CompanyId = ci.CompanyId
JOIN 
    young_company.industry i ON ci.IndustryId = i.IndustryId
WHERE 
    ca.SuccessIndex IS NOT NULL  
GROUP BY 
    i.IndustryName
ORDER BY 
    TotalSuccessIndex DESC;
    
    


DROP VIEW IF EXISTS view_CountryRankings;
DROP VIEW IF EXISTS view_RegionRankings;

CREATE VIEW view_CountryRankings AS
SELECT 
    r.country,
    COUNT(DISTINCT ca.CompanyId) AS SuccessfulCompaniesCount,  
    SUM(ca.SuccessIndex) AS TotalSuccessIndex,  
    RANK() OVER (ORDER BY SUM(ca.SuccessIndex) DESC) AS Ranking
FROM 
    young_company.company_analytics ca
JOIN 
    young_company.regions r ON ca.CompanyId = r.CompanyId
WHERE 
    ca.SuccessIndex > 0  
GROUP BY 
    r.country;

CREATE VIEW view_RegionRankings AS
SELECT 
    r.region,
    COUNT(DISTINCT ca.CompanyId) AS SuccessfulCompaniesCount,  
    SUM(ca.SuccessIndex) AS TotalSuccessIndex, 
    RANK() OVER (ORDER BY SUM(ca.SuccessIndex) DESC) AS Ranking 
FROM 
    young_company.company_analytics ca
JOIN 
    young_company.regions r ON ca.CompanyId = r.CompanyId
WHERE 
    ca.SuccessIndex > 0  
GROUP BY 
    r.region;

SELECT * FROM young_company.company_analytics;