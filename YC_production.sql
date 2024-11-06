-- ============================================================================================================
-- SECTION 1: Data Ingestion
-- This section imports the necessary tables from /private/tmp/

-- SECTION 2: Data Normalization

-- SECTION 3: QS-Ranking Mapping
-- This section maps the different university names to the QS standard name with cosine similarity method done in Python

-- SECTION 4: ETL 
-- This section generate analytical layer tables

-- SECTION 5: Procedures

-- SECTION 6: Creating Data Marts
-- ============================================================================================================


-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################

-- ============================================================================================================
-- SECTION 1: Data Ingestion
-- This section imports the necessary tables from /private/tmp/
-- ============================================================================================================

DROP SCHEMA IF EXISTS young_company;

SET foreign_key_checks = 0; -- not constrained when dropping when having foreign key constrains.

CREATE SCHEMA IF NOT EXISTS young_company; -- create schema first 
USE young_company; -- use this schema for this script

DROP TABLE IF EXISTS badges;
CREATE TABLE badges ( -- create table
    RowNumber INT, -- VARCHAR(10)
    CompanyId INT,
    CompanyBadge VARCHAR(50)
);
TRUNCATE badges; -- clear the table before inserting
LOAD DATA INFILE '/private/tmp/badges.csv' 
INTO TABLE badges
FIELDS TERMINATED BY ','
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES -- to remove the header of the csv
(@RowNumber, @CompanyId, @CompanyBadge)

SET -- for making sure there is on empty string --> change it to null value
RowNumber = NULLIF(@RowNumber, ''),
CompanyId = NULLIF(@CompanyId, ''),
CompanyBadge = NULLIF(@CompanyBadge, '');

-- ######################################### the next file import
-- the comments of the previous file importing applys to this one
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

-- ######################################### the next file import
-- the comments of the previous file importing applys to this one

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
                    WHEN UPPER(TRIM(@top_company)) LIKE 'TRUE%' THEN 1 -- convert the true or false to 1 or 0 because that is adaptable to mySQL.
                    WHEN UPPER(TRIM(@top_company)) LIKE 'FALSE%' THEN 0 -- TRIM values to make sure no spaces. 
                    ELSE NULL 
                END
-- top_company = NULLIF(@top_company, '')
;
-- ######################################### the next file import
-- the comments of the previous file importing applys to this one

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
    industry = LEFT(NULLIF(@industry, ''), 200);  -- this is to make sure the string is not too long if too long then cut off the right part.

  
-- ######################################### the next file import
-- the comments of the previous file importing applys to this one


CREATE SCHEMA IF NOT EXISTS young_company;
USE young_company;

DROP TABLE IF EXISTS prior_companies;
CREATE TABLE prior_companies (
    hnid VARCHAR(15),
    company VARCHAR (300),
    FOREIGN KEY (hnid) REFERENCES founders(hnid) ON DELETE CASCADE -- hnid is the foreign key of this table which can be connected to the founder table
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
  company = LEFT(NULLIF(@company, ''), 300); -- this is to make sure the string is not too long if too long then cut off the right part.
  
-- ######################################### the next file import
-- the comments of the previous file importing applys to this one

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
  
-- ######################################### the next file import
-- the comments of the previous file importing applys to this one
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
  
-- ######################################### the next file import
-- the comments of the previous file importing applys to this one

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
-- ============================================================================================================
-- SECTION 3: QS-Ranking Mapping
-- This section maps the different university names to the QS standard name with cosine similarity method done in Python
-- ============================================================================================================

-- this part mapes the non-standard university names to the standard ones that works for QS ranking data.
SET SQL_SAFE_UPDATES = 0; -- turn off so that I can map the data and change the value. it is turned back to 1 after updating
-- these data for mapping are generated with python BERT model. Please refer to the readme

UPDATE schools SET school = 'High School' WHERE LOWER(school) LIKE '%high school%'; -- change all the high school related to high school for simplicity becuase the majority of the education institutions are universities.
UPDATE young_company.schools SET school = 'The London School of Economics and Political Science' WHERE school = 'London School of Economics and Political Science';
UPDATE young_company.schools SET school = 'Indian Institute of Technology Guwahati' WHERE school = 'Indian Institute of Technology, Guwahati';
UPDATE young_company.schools SET school = 'University of Minnesota Twin Cities' WHERE school = 'University of Minnesota';
UPDATE young_company.schools SET school = 'Harvard University' WHERE school = 'Harvard Business School Online';
UPDATE young_company.schools SET school = 'Universidad Iberoamericana IBERO' WHERE school = 'Universidad Iberoamericana';
UPDATE young_company.schools SET school = 'Stanford University' WHERE school = 'Stanford University Graduate School of Business';
UPDATE young_company.schools SET school = 'University of California, Berkeley' WHERE school = 'UC Berkeley College of Engineering';
UPDATE young_company.schools SET school = 'Indian Institute of Technology Bombay' WHERE school = 'IIT Bombay';
UPDATE young_company.schools SET school = 'Birla Institute of Technology and Science, Pilani' WHERE school = 'Birla Institute of Technology';
UPDATE young_company.schools SET school = 'Harvard University' WHERE school = 'Harvard Business School';
UPDATE young_company.schools SET school = 'Indian Institute of Technology Delhi' WHERE school = 'IIT Delhi';
UPDATE young_company.schools SET school = 'National Institute of Technology Tiruchirappalli' WHERE school = 'National Institute of Technology, Kurukshetra';
UPDATE young_company.schools SET school = 'Carnegie Mellon University' WHERE school = 'Carnegie Mellon University College of Engineering';
UPDATE young_company.schools SET school = 'Western University' WHERE school = 'Ivey Business School at Western University';
UPDATE young_company.schools SET school = 'University of British Columbia' WHERE school = 'The University of British Columbia';
UPDATE young_company.schools SET school = 'École Normale Supérieure de Lyon' WHERE school = 'Ecole normale supérieure';
UPDATE young_company.schools SET school = 'Indian Institute of Technology Kanpur' WHERE school = 'IIT Kanpur';
UPDATE young_company.schools SET school = 'Pondicherry University' WHERE school = 'Pondicherry Engineering College';
UPDATE young_company.schools SET school = 'Stanford University' WHERE school = 'Stanford Continuing Studies';
UPDATE young_company.schools SET school = 'University of Nottingham' WHERE school = 'Nottingham';
UPDATE young_company.schools SET school = 'Massachusetts Institute of Technology ' WHERE school = 'Massachusetts Institute of Technology';
UPDATE young_company.schools SET school = 'Pennsylvania State University' WHERE school = 'Penn State University';
UPDATE young_company.schools SET school = 'George Washington University' WHERE school = 'The George Washington University';
UPDATE young_company.schools SET school = 'Harvard University' WHERE school = 'Harvard Law School';
UPDATE young_company.schools SET school = 'Cornell University' WHERE school = 'Cornell University College of Engineering';
UPDATE young_company.schools SET school = 'Harvard University' WHERE school = 'Harvard Graduate School of Arts and Sciences';
UPDATE young_company.schools SET school = 'University of Delhi' WHERE school = 'Delhi Public School, R.K.Puram, New Delhi';
UPDATE young_company.schools SET school = 'National Institute of Technology Tiruchirappalli' WHERE school = 'National Institute of Technology, Tiruchirappalli';
UPDATE young_company.schools SET school = 'Universidad del Norte ' WHERE school = 'Universidad del Norte';
UPDATE young_company.schools SET school = 'Université de Montpellier' WHERE school = 'Montpellier Business School';
UPDATE young_company.schools SET school = 'Harvard University' WHERE school = 'Harvard University Kennedy School of Government';
UPDATE young_company.schools SET school = 'Banaras Hindu University' WHERE school = 'Banha University';
UPDATE young_company.schools SET school = 'Massachusetts Institute of Technology ' WHERE school = 'Massachusetts Institute of Techonology';
UPDATE young_company.schools SET school = 'Indian Institute of Technology Madras' WHERE school = 'IIT Madras';
UPDATE young_company.schools SET school = 'Northumbria University at Newcastle' WHERE school = 'Northumbria University';
UPDATE young_company.schools SET school = 'University of Delhi' WHERE school = 'Delhi Public School - R. K. Puram';
UPDATE young_company.schools SET school = 'Northumbria University at Newcastle' WHERE school = 'Northumbria University (UK)';
UPDATE young_company.schools SET school = 'University of Mumbai' WHERE school = 'St. Xavier''s College, Mumbai';
UPDATE young_company.schools SET school = 'Rutgers University–Newark' WHERE school = 'Rutgers University';
UPDATE young_company.schools SET school = 'Birla Institute of Technology and Science, Pilani' WHERE school = 'Birla Institute of Technology and Science, Pilani (BITS Pilani)';
UPDATE young_company.schools SET school = 'Indian Institute of Technology Kharagpur' WHERE school = 'IIT Kharagpur';
UPDATE young_company.schools SET school = 'University of Massachusetts Amherst' WHERE school = 'Amherst College';
UPDATE young_company.schools SET school = 'Damascus University' WHERE school = 'Aleppo University';
UPDATE young_company.schools SET school = 'Jawaharlal Nehru University' WHERE school = 'Mahatma Gandhi University';
UPDATE young_company.schools SET school = 'Universidade Federal de Minas Gerais      ' WHERE school = 'Universidade Federal de Minas Gerais';
UPDATE young_company.schools SET school = 'Universidade Federal de Minas Gerais      ' WHERE school = 'Instituto Federal de Educação, Ciência e Tecnologia de Minas Gerais - IFMG';
UPDATE young_company.schools SET school = 'Università Cattolica del Sacro Cuore' WHERE school = 'Università Cattolica del Sacro Cuore';
UPDATE young_company.schools SET school = 'University of California, Berkeley' WHERE school = 'University of California, Berkeley, Haas School of Business';
UPDATE young_company.schools SET school = 'Universidad de Talca' WHERE school = 'Universidad Francisco de Vitoria';
UPDATE young_company.schools SET school = 'Universidade de São Paulo' WHERE school = 'Graded School of São Paulo';
UPDATE young_company.schools SET school = 'University of Maribor' WHERE school = 'Univerza v Mariboru';
UPDATE young_company.schools SET school = 'Ludwig-Maximilians-Universität München' WHERE school = 'Ludwig Maximilian University of Munich';
UPDATE young_company.schools SET school = 'KTH Royal Institute of Technology ' WHERE school = 'KTH Royal Institute of Technology';
UPDATE young_company.schools SET school = 'Sciences Po ' WHERE school = 'Sciences Po';
UPDATE young_company.schools SET school = 'Linköping University' WHERE school = 'Linköping University (Sweden)';
UPDATE young_company.schools SET school = 'University of Brighton' WHERE school = 'Brighton Secondary College';
UPDATE young_company.schools SET school = 'Duke University' WHERE school = 'Duke Medical School';
UPDATE young_company.schools SET school = 'Universidad Nacional Autónoma de México' WHERE school = 'Universidad Nacional Autonoma de Mexico (UNAM)';
UPDATE young_company.schools SET school = 'Cornell University' WHERE school = 'Cornell University - Johnson Graduate School of Management';
UPDATE young_company.schools SET school = 'Bilkent University' WHERE school = 'Bilkent Üniversitesi / Bilkent University';
UPDATE young_company.schools SET school = 'Indiana State University' WHERE school = 'Indiana University';
UPDATE young_company.schools SET school = 'Yeshiva University' WHERE school = 'Benjamin N. Cardozo School of Law, Yeshiva University';
UPDATE young_company.schools SET school = 'University of San Diego' WHERE school = 'College of San Mateo';
UPDATE young_company.schools SET school = 'Northwest Agriculture and Forestry University' WHERE school = 'Northeast Agricultural University';
UPDATE young_company.schools SET school = 'University of Navarra' WHERE school = 'Universida de Navarra';
UPDATE young_company.schools SET school = 'Harvard University' WHERE school = 'Harvard University Graduate School of Education';
UPDATE young_company.schools SET school = 'Universidad Tecnológica de la Habana José Antonio Echeverría, Cujae' WHERE school = 'Universidade Estadual Paulista Júlio de Mesquita Filho';
UPDATE young_company.schools SET school = 'Jamia Hamdard' WHERE school = 'Jamia hamdard';
UPDATE young_company.schools SET school = 'The Hebrew University of Jerusalem' WHERE school = 'The Hebrew University';
UPDATE young_company.schools SET school = 'The University of Sydney' WHERE school = 'University of Sydney';
UPDATE young_company.schools SET school = 'Yale University' WHERE school = 'Yale School of Management';
UPDATE young_company.schools SET school = 'University of California, Irvine' WHERE school = 'UC Irvine Heath';
UPDATE young_company.schools SET school = 'Chalmers University of Technology' WHERE school = 'Chalmers University';
UPDATE young_company.schools SET school = 'Universitas Indonesia' WHERE school = 'University of Indonesia';
UPDATE young_company.schools SET school = 'Samara National Research University' WHERE school = 'Samara State Aerospace University';
UPDATE young_company.schools SET school = 'Universidad del Valle' WHERE school = 'Universidad del Valle (CO)';
UPDATE young_company.schools SET school = 'Universidad del Pacífico' WHERE school = 'Universidad del Pacífico (PE)';
UPDATE young_company.schools SET school = 'The University of Tennessee, Knoxville' WHERE school = 'Tennessee State University';
UPDATE young_company.schools SET school = 'University of International Business and Economics' WHERE school = 'Lazaridis School of Business & Economics at Wilfrid Laurier University';
UPDATE young_company.schools SET school = 'University of Delhi' WHERE school = 'Delhi Private School, Sharjah';
UPDATE young_company.schools SET school = 'University of Denver' WHERE school = 'Community College of Denver';
UPDATE young_company.schools SET school = 'University of Delhi' WHERE school = 'Delhi University';
UPDATE young_company.schools SET school = 'Harvard University' WHERE school = 'Harvard College';
UPDATE young_company.schools SET school = 'École Normale Supérieure de Lyon' WHERE school = 'Ecole Centrale de Lyon';
UPDATE young_company.schools SET school = 'University of Delhi' WHERE school = 'Delhi College of Engineering';
UPDATE young_company.schools SET school = 'University of Basel' WHERE school = 'International School Basel, Switzerland';
UPDATE young_company.schools SET school = 'University of Delhi' WHERE school = 'Delhi Public School, Vasant Kunj, New Delhi';
UPDATE young_company.schools SET school = 'University of Colorado, Denver ' WHERE school = 'University of Colorado Denver';
UPDATE young_company.schools SET school = 'Stanford University' WHERE school = 'Stanford Law School';
UPDATE young_company.schools SET school = 'Alexandria University ' WHERE school = 'Alexandria University';
UPDATE young_company.schools SET school = 'University of Nebraska - Lincoln' WHERE school = 'University of Nebraska at Omaha';
UPDATE young_company.schools SET school = 'Imperial College London' WHERE school = 'Imperial College Business School';
UPDATE young_company.schools SET school = 'Stellenbosch University' WHERE school = 'University of Stellenbosch';
UPDATE young_company.schools SET school = 'University of Hyderabad' WHERE school = 'The Hyderabad Public School';
UPDATE young_company.schools SET school = 'Sharif University of Technology' WHERE school = 'Jaypee University of Information Technology';
UPDATE young_company.schools SET school = 'Indian Institute of Technology Guwahati' WHERE school = 'Dhirubhai Ambani Institute of Information and Communication Technology';
UPDATE young_company.schools SET school = 'Carleton University' WHERE school = 'Carleton College';
UPDATE young_company.schools SET school = 'The University of New South Wales' WHERE school = 'University of New South Wales';
UPDATE young_company.schools SET school = 'The University of Sydney' WHERE school = 'University of NSW, Australia';
UPDATE young_company.schools SET school = 'Case Western Reserve University' WHERE school = 'Case Western Reserve Univeristy';
UPDATE young_company.schools SET school = 'Harvard University' WHERE school = 'Harvard School of Engineering and Applied Sciences';
UPDATE young_company.schools SET school = 'Kazan Federal University' WHERE school = 'Kazan State University';
UPDATE young_company.schools SET school = 'Durham University' WHERE school = 'Durham University Business School';
UPDATE young_company.schools SET school = 'Beijing Foreign Studies University' WHERE school = 'Beijing Language and Culture University';
UPDATE young_company.schools SET school = 'Vellore Institute of Technology' WHERE school = 'VIT University, Vellore';
UPDATE young_company.schools SET school = 'Stanford University' WHERE school = 'Stanford University School of Medicine';
UPDATE young_company.schools SET school = 'Universidad Autonoma de Yucatan' WHERE school = 'Autonomous University of Yucatan';
UPDATE young_company.schools SET school = 'Università degli studi Roma Tre' WHERE school = 'Università degli Studi di Roma ''La Sapienza''';
UPDATE young_company.schools SET school = 'Vanderbilt University' WHERE school = 'Vanderbilt University Law School';
UPDATE young_company.schools SET school = 'Instituto Tecnológico Autónomo de México' WHERE school = 'Instituto Tecnologico Autonomo de Mexico (ITAM)';
UPDATE young_company.schools SET school = 'Wesleyan University' WHERE school = 'Wesleyan School';
UPDATE young_company.schools SET school = 'Georgetown University' WHERE school = 'Georgetown Prep';
UPDATE young_company.schools SET school = 'Kingston University, London' WHERE school = 'Kingston University';
UPDATE young_company.schools SET school = 'Lund University' WHERE school = 'Lunds tekniska högskola / The Faculty of Engineering at Lund University';
UPDATE young_company.schools SET school = 'V. N. Karazin Kharkiv National University' WHERE school = 'Kharkiv V.N. Karazin National University';
UPDATE young_company.schools SET school = 'Ben-Gurion University of The Negev' WHERE school = 'Universitat Ben Gurion Ba';
UPDATE young_company.schools SET school = 'Indian Institute of Technology Guwahati' WHERE school = 'IIT Guwahati';
UPDATE young_company.schools SET school = 'Université de Liège' WHERE school = 'University of Liège';
UPDATE young_company.schools SET school = 'Birla Institute of Technology and Science, Pilani' WHERE school = 'Birla Institute of Technology & Science, Pilani';
UPDATE young_company.schools SET school = 'Aix-Marseille University' WHERE school = 'Faculté des sciences Aix-Marseille';
UPDATE young_company.schools SET school = 'The "Gheorghe Asachi" Technical University of Iasi' WHERE school = 'The \Gheorghe Asachi\" Technical University of Iasi"';
UPDATE young_company.schools SET school = 'University of San Francisco' WHERE school = 'City College of San Francisco';
UPDATE young_company.schools SET school = 'McGill University' WHERE school = 'McGill University - Desautels Faculty of Management';
UPDATE young_company.schools SET school = 'Columbia University' WHERE school = 'Teachers College of Columbia University';
UPDATE young_company.schools SET school = 'Delft University of Technology' WHERE school = 'Technische Universiteit Delft';
UPDATE young_company.schools SET school = 'University of Texas Dallas' WHERE school = 'The University of Texas at Dallas';
UPDATE young_company.schools SET school = 'Vilnius University ' WHERE school = 'Vilniaus universitetas / Vilnius University';
UPDATE young_company.schools SET school = 'University of International Business and Economics' WHERE school = 'International Business School';
UPDATE young_company.schools SET school = 'Stanford University' WHERE school = 'Stanford University, Graduate School of Business';
UPDATE young_company.schools SET school = 'University of Delhi' WHERE school = 'Delhi Public School';
UPDATE young_company.schools SET school = 'Pennsylvania State University' WHERE school = 'Penn State';
UPDATE young_company.schools SET school = 'Louisiana State University' WHERE school = 'University of Louisiana at Monroe';
UPDATE young_company.schools SET school = 'Ohio University' WHERE school = 'ohio university';
UPDATE young_company.schools SET school = 'Durham University' WHERE school = 'University of Durham, St John''s College';
UPDATE young_company.schools SET school = 'V. N. Karazin Kharkiv National University' WHERE school = 'Harkivs''kij Nacional''nij Universitet im. V.N. Karazina';
UPDATE young_company.schools SET school = 'Bina Nusantara University' WHERE school = 'Universitas Bina Nusantara (Binus) International';
UPDATE young_company.schools SET school = 'University of Arkansas Fayetteville ' WHERE school = 'Arkansas State University';
UPDATE young_company.schools SET school = 'University of the Pacific' WHERE school = 'Azusa Pacific University';
UPDATE young_company.schools SET school = 'Columbia University' WHERE school = 'Columbia University - School of International and Public Affairs';
UPDATE young_company.schools SET school = 'Columbia University' WHERE school = 'Columbia College, Columbia University';
UPDATE young_company.schools SET school = 'Stanford University' WHERE school = 'Stanford University School of Engineering';
UPDATE young_company.schools SET school = 'California Polytechnic State University' WHERE school = 'California Polytechnic State University-San Luis Obispo';
UPDATE young_company.schools SET school = 'New York University' WHERE school = 'New York University\
\
2007';
UPDATE young_company.schools SET school = 'Texas A&M University' WHERE school = 'Texas A&amp;M University';
UPDATE young_company.schools SET school = 'Technological University Dublin' WHERE school = 'Dublin Institute of Technology';
UPDATE young_company.schools SET school = 'Lund University' WHERE school = 'The Faculty of Engineering at Lund University';
UPDATE young_company.schools SET school = 'Tampere University' WHERE school = 'Tampere University of Technology';
UPDATE young_company.schools SET school = 'Technical University of Denmark' WHERE school = 'DTU - Technical University of Denmark';
UPDATE young_company.schools SET school = 'Universidade Federal de Pernambuco ' WHERE school = 'Universidade de Pernambuco';
UPDATE young_company.schools SET school = 'Institut Polytechnique de Paris' WHERE school = 'Ecole Polytechnique, Paris';
UPDATE young_company.schools SET school = 'University of the Sunshine Coast ' WHERE school = 'University of the Sunshine Coast';
UPDATE young_company.schools SET school = 'KU Leuven' WHERE school = 'Imec / KU Leuven';
UPDATE young_company.schools SET school = 'Georgetown University' WHERE school = 'Georgetown University Law Center';
UPDATE young_company.schools SET school = 'Imperial College London' WHERE school = 'Royal College of Art | Imperial College London';
UPDATE young_company.schools SET school = 'Universidad de los Andes' WHERE school = 'Universidad de Los Andes';
UPDATE young_company.schools SET school = 'University of Nebraska - Lincoln' WHERE school = 'University of Nebraska-Lincoln';
UPDATE young_company.schools SET school = 'Manchester Metropolitan University' WHERE school = 'The Manchester Metropolitan University';
UPDATE young_company.schools SET school = 'Tecnológico de Monterrey' WHERE school = 'Instituto Tecnológico y de Estudios Superiores de Monterrey / ITESM\
Preparatoria';
UPDATE young_company.schools SET school = 'Tecnológico de Monterrey' WHERE school = 'Instituto Tecnológico y de Estudios Superiores de Monterrey / ITESM';
UPDATE young_company.schools SET school = 'Universidad de los Andes' WHERE school = 'University of the Andes';
UPDATE young_company.schools SET school = 'Universidad de Santiago de Chile' WHERE school = 'Santiago College';
UPDATE young_company.schools SET school = 'Universidade da Coruña ' WHERE school = 'Universidad de A Coruna, Spain';
UPDATE young_company.schools SET school = 'University of Florida' WHERE school = 'University of S. Florida';
UPDATE young_company.schools SET school = 'University of Notre Dame' WHERE school = 'Notre Dame';
UPDATE young_company.schools SET school = 'University of Pennsylvania' WHERE school = 'University of Pennsylvania Law School';
UPDATE young_company.schools SET school = 'University of Texas at San Antonio' WHERE school = 'The University of Texas at San Antonio';
UPDATE young_company.schools SET school = 'North Carolina State University' WHERE school = 'North Carolina Agricultural and Technical State University';
UPDATE young_company.schools SET school = 'Goldsmiths, University of London' WHERE school = 'Goldsmiths College, U. of London';
UPDATE young_company.schools SET school = 'The University of Hong Kong' WHERE school = 'The University of Hong Kong, Faculty of Law';
UPDATE young_company.schools SET school = 'University of Toronto' WHERE school = 'University of Toronto, Faculty of Law';
UPDATE young_company.schools SET school = 'Utah State University ' WHERE school = 'Utah State University';
UPDATE young_company.schools SET school = 'KIT, Karlsruhe Institute of Technology' WHERE school = 'Karlsruhe Institute of Technology (KIT)';
UPDATE young_company.schools SET school = 'Oklahoma State University ' WHERE school = 'Oklahoma State University';
UPDATE young_company.schools SET school = 'Maastricht University ' WHERE school = 'Maastricht University';
UPDATE young_company.schools SET school = 'Maastricht University ' WHERE school = 'Universiteit Maastricht';
UPDATE young_company.schools SET school = 'Universidade Federal de Pernambuco ' WHERE school = 'Universidade Federal de Pernambuco';
UPDATE young_company.schools SET school = 'University of St.Gallen' WHERE school = 'University of St. Gallen (HSG)';
UPDATE young_company.schools SET school = 'Ecole des Ponts ParisTech' WHERE school = 'École des Ponts ParisTech';
UPDATE young_company.schools SET school = 'University of Wisconsin Milwaukee ' WHERE school = 'University of Wisconsin-Milwaukee';
UPDATE young_company.schools SET school = 'Washington University in St. Louis' WHERE school = 'Washington University School of Medicine in St. Louis';
UPDATE young_company.schools SET school = 'University of St Andrews' WHERE school = 'St. Andrews Episcopal School';
UPDATE young_company.schools SET school = 'The University of Adelaide' WHERE school = 'University of Adelaide';
UPDATE young_company.schools SET school = 'Columbia University' WHERE school = 'Barnard College, Columbia University';
UPDATE young_company.schools SET school = 'University of Missouri, Kansas City' WHERE school = 'University of Missouri-Kansas City';
UPDATE young_company.schools SET school = 'University of Westminster' WHERE school = 'Westminster College';
UPDATE young_company.schools SET school = 'College of William and Mary' WHERE school = 'St. Mary''s College of Maryland';
UPDATE young_company.schools SET school = 'Trinity College Dublin, The University of Dublin' WHERE school = 'Trinity College, Dublin';
UPDATE young_company.schools SET school = 'University of San Francisco' WHERE school = 'University of San Francisco School of Law';
UPDATE young_company.schools SET school = 'Tecnológico de Monterrey' WHERE school = 'Tecnologico de estudios superiores de monterrey';
UPDATE young_company.schools SET school = 'Tecnológico de Monterrey' WHERE school = 'Tecnológico de monterrey campus GDA';
UPDATE young_company.schools SET school = 'King''s College London' WHERE school = 'The King''s College';
UPDATE young_company.schools SET school = 'Binghamton University SUNY' WHERE school = 'Binghamton University';
UPDATE young_company.schools SET school = 'Stony Brook University, State University of New York' WHERE school = 'Stony Brook University';
UPDATE young_company.schools SET school = 'University of Kansas' WHERE school = 'The University of Kansas';
UPDATE young_company.schools SET school = 'Yonsei University' WHERE school = 'Yonsei University 연세대학교';
UPDATE young_company.schools SET school = 'University of Haifa' WHERE school = 'HAIFA UNIVERSITY';
UPDATE young_company.schools SET school = 'University at Buffalo SUNY' WHERE school = 'State University of New York at Buffalo';
UPDATE young_company.schools SET school = 'Xi’an Jiaotong University' WHERE school = 'Xi''an Jiaotong University';
UPDATE young_company.schools SET school = 'San Diego State University' WHERE school = 'San Diego State University-California State University';
UPDATE young_company.schools SET school = 'Auburn University' WHERE school = 'Auburn University, Samuel Ginn College of Engineering';
UPDATE young_company.schools SET school = 'The University of Sheffield' WHERE school = 'Sheffield University';
UPDATE young_company.schools SET school = 'University at Albany SUNY' WHERE school = 'University at Albany';
UPDATE young_company.schools SET school = 'University of Cape Town' WHERE school = 'University of Cape Town (Graduate School of Business)';
UPDATE young_company.schools SET school = 'Tel Aviv University' WHERE school = 'Tel-Aviv University Secondary School';
UPDATE young_company.schools SET school = 'Erasmus University Rotterdam ' WHERE school = 'Erasmus University Rotterdam';
UPDATE young_company.schools SET school = 'Saint Petersburg State University' WHERE school = 'Saint Petersburg State Art-Industrial Academy';
UPDATE young_company.schools SET school = 'Ateneo de Manila University' WHERE school = 'De La Salle University - Manila';
UPDATE young_company.schools SET school = 'Pennsylvania State University' WHERE school = 'Pennsylvania State University, Schreyer Honors College';
UPDATE young_company.schools SET school = 'Southern Federal University' WHERE school = 'Southern Federal University (former Rostov State University)';
UPDATE young_company.schools SET school = 'SRM INSTITUTE OF SCIENCE AND TECHNOLOGY' WHERE school = 'SRM University';
UPDATE young_company.schools SET school = 'University of Zurich' WHERE school = 'Universität  Zürich | University of Zurich';
UPDATE young_company.schools SET school = 'Queen''s University at Kingston' WHERE school = 'Queen''s College';
UPDATE young_company.schools SET school = 'Brigham Young University' WHERE school = 'Brigham Young University - Hawaii';
UPDATE young_company.schools SET school = 'College of William and Mary' WHERE school = 'William & Mary';
UPDATE young_company.schools SET school = 'Universidad La Salle' WHERE school = 'La Salle BCN';
UPDATE young_company.schools SET school = 'Indian Institute of Science' WHERE school = 'Indian Institute of Science (IISc)';
UPDATE young_company.schools SET school = 'Universidad Pontificia Bolivariana ' WHERE school = 'Universidad Pontificia Bolivariana';
UPDATE young_company.schools SET school = 'University of Göttingen' WHERE school = 'Georg-August-Universität Göttingen';
UPDATE young_company.schools SET school = 'University of British Columbia' WHERE school = 'British Columbia Institute of Technology';
UPDATE young_company.schools SET school = 'Temple University' WHERE school = 'Temple University College of Engineering';
UPDATE young_company.schools SET school = 'Ghent University' WHERE school = 'Ghent University, Belgium';
UPDATE young_company.schools SET school = 'Universita'' degli Studi di Ferrara' WHERE school = 'Università degli Studi di Trieste';
UPDATE young_company.schools SET school = 'Universidade de São Paulo' WHERE school = 'Escola Politécnica da Universidade de São Paulo';
UPDATE young_company.schools SET school = 'Duke University' WHERE school = 'Duke University School of Law';
UPDATE young_company.schools SET school = 'Saint Petersburg State University' WHERE school = 'St. Petersburg State University';
UPDATE young_company.schools SET school = 'Queen Mary University of London' WHERE school = 'Queen Mary, U. of London';
UPDATE young_company.schools SET school = 'Lomonosov Moscow State University' WHERE school = 'Lomonosov Moscow State University (MSU)';
UPDATE young_company.schools SET school = 'KAIST - Korea Advanced Institute of Science & Technology' WHERE school = 'KAIST(Korea Advanced Institute of Science and Technology)';
UPDATE young_company.schools SET school = 'University of California, Berkeley' WHERE school = 'UC Berkeley and UCSF';
UPDATE young_company.schools SET school = 'University of Electronic Science and Technology of China' WHERE school = 'Institute of Computing Technology, Chinese Academy of Science';
UPDATE young_company.schools SET school = 'Kansas State University ' WHERE school = 'Kansas State University';
UPDATE young_company.schools SET school = 'Kenyatta University' WHERE school = 'Kenyatta university';
UPDATE young_company.schools SET school = 'Bilkent University' WHERE school = 'Bilkent Üniversitesi';
UPDATE young_company.schools SET school = 'College of William and Mary' WHERE school = 'William & Mary Law School';
UPDATE young_company.schools SET school = 'The University of Melbourne' WHERE school = 'University of Melbourne';
UPDATE young_company.schools SET school = 'De La Salle University' WHERE school = 'La Salle (Ramon Llull University)';
UPDATE young_company.schools SET school = 'Singapore Management University' WHERE school = 'Singapore Institute of Management';
UPDATE young_company.schools SET school = 'University of Toronto' WHERE school = 'University of Toronto at Scarborough';
UPDATE young_company.schools SET school = 'Lomonosov Moscow State University' WHERE school = 'Lomonosov Moscow University';
UPDATE young_company.schools SET school = 'Stony Brook University, State University of New York' WHERE school = 'State University of New York at Stony Brook';
UPDATE young_company.schools SET school = 'Georgia Institute of Technology' WHERE school = 'Georgia Institute of Technology (Atlanta)';
UPDATE young_company.schools SET school = 'Management and Science University' WHERE school = 'College of Management';
UPDATE young_company.schools SET school = 'Indian Institute of Technology Indore' WHERE school = 'Indian Institute of Technology';
UPDATE young_company.schools SET school = 'Universidad Panamericana' WHERE school = 'Universidad Panamericana Preparatoria';
UPDATE young_company.schools SET school = 'Robert Gordon University' WHERE school = 'Robert Gordon''s College';
UPDATE young_company.schools SET school = 'Wesleyan University' WHERE school = 'Oklahoma Wesleyan University';
UPDATE young_company.schools SET school = 'University of Ottawa' WHERE school = 'University of Ottawa Law';
UPDATE young_company.schools SET school = 'Victoria University ' WHERE school = 'Victoria Park Collegiate Institute';
UPDATE young_company.schools SET school = 'Umea University' WHERE school = 'Umeå University';
UPDATE young_company.schools SET school = 'Universidad de Puerto Rico' WHERE school = 'University of Puerto Rico';
UPDATE young_company.schools SET school = 'Wesleyan University' WHERE school = 'Ohio Wesleyan University';
UPDATE young_company.schools SET school = 'The American University in Cairo' WHERE school = 'American University of Cairo';
UPDATE young_company.schools SET school = 'Friedrich-Alexander-Universität Erlangen-Nürnberg' WHERE school = 'University of Erlangen-Nuremberg';
UPDATE young_company.schools SET school = 'Université Paris 1 Panthéon-Sorbonne ' WHERE school = 'Université Paris 1 Panthéon-Sorbonne';
UPDATE young_company.schools SET school = 'Brigham Young University' WHERE school = 'Brigham Young University Marriott School of Business';
UPDATE young_company.schools SET school = 'University of Washington' WHERE school = 'Washington and Lee University';
UPDATE young_company.schools SET school = 'University of California, Santa Cruz' WHERE school = 'UC Santa Cruz';
UPDATE young_company.schools SET school = 'University of Texas at Austin' WHERE school = 'The University of Texas at Austin - Red McCombs School of Business';
UPDATE young_company.schools SET school = 'St. Louis University' WHERE school = 'Saint Louis University';
UPDATE young_company.schools SET school = 'Aix-Marseille University' WHERE school = 'Aix-Marseille Graduate School of Management - IAE';
UPDATE young_company.schools SET school = 'Nanyang Technological University, Singapore' WHERE school = 'Nanyang Polytechnic';
UPDATE young_company.schools SET school = 'National Research Nuclear University MEPhI' WHERE school = 'National Research Nuclear University MEPhI (Moscow Engineering Physics Institute)';
UPDATE young_company.schools SET school = 'Stockholm University' WHERE school = 'SAE Stockholm';
UPDATE young_company.schools SET school = 'London Metropolitan University' WHERE school = 'City University London';
UPDATE young_company.schools SET school = 'University of Notre Dame' WHERE school = 'Notre Dame College';
UPDATE young_company.schools SET school = 'Durham University' WHERE school = 'University of Durham';
UPDATE young_company.schools SET school = 'The University of Warwick' WHERE school = 'Warwick Business School';
UPDATE young_company.schools SET school = 'International Islamic University Islamabad' WHERE school = 'International Islamic University';
UPDATE young_company.schools SET school = 'Universidade Federal de Santa Maria' WHERE school = 'Universidad ''Santa María''';
UPDATE young_company.schools SET school = 'Eberhard Karls Universität Tübingen' WHERE school = 'University of Tübingen';
UPDATE young_company.schools SET school = 'Universidad de Buenos Aires' WHERE school = 'Universidad de Belgrano, Buenos Aires, Argentina';
UPDATE young_company.schools SET school = 'Sakarya University' WHERE school = 'Sakarya Üniversitesi';
UPDATE young_company.schools SET school = 'Poznań University of Technology' WHERE school = 'Politechnika Wrocławska / Wroclaw University of Technology';
UPDATE young_company.schools SET school = 'Bielefeld University' WHERE school = 'Fachhochschule Bielefeld, University of Applied Sciences, Germany';
UPDATE young_company.schools SET school = 'Universidade de São Paulo' WHERE school = 'Universidade de São Paulo (USP)';
UPDATE young_company.schools SET school = 'Saint Petersburg State University' WHERE school = 'St. Petersburg College';
UPDATE young_company.schools SET school = 'Rutgers, The State University of New Jersey, Camden' WHERE school = 'Rutgers, The State University of New Jersey';
UPDATE young_company.schools SET school = 'Instituto Tecnológico de Buenos Aires' WHERE school = 'Instituto Tecnológico de Buenos Aires (ITBA)';
UPDATE young_company.schools SET school = 'Universita'' degli Studi di Ferrara' WHERE school = 'Università degli Studi di Ferrara';
UPDATE young_company.schools SET school = 'Tecnológico de Monterrey' WHERE school = 'Tec de Monterrey';
UPDATE young_company.schools SET school = 'Sapienza University of Rome' WHERE school = 'University of Rome \La Sapienza\""';
UPDATE young_company.schools SET school = 'Harvard University' WHERE school = 'Harvard Extension School';
UPDATE young_company.schools SET school = 'Universität Stuttgart' WHERE school = 'University of Stuttgart';
UPDATE young_company.schools SET school = 'University of Arkansas Fayetteville ' WHERE school = 'University of Arkansas';
UPDATE young_company.schools SET school = 'Shanghai International Studies University' WHERE school = 'ESAI Shanghai';
UPDATE young_company.schools SET school = 'Yale University' WHERE school = 'Yale Law School';
UPDATE young_company.schools SET school = 'Yale University' WHERE school = 'Yale College';
UPDATE young_company.schools SET school = 'Universidade Federal de São Carlos' WHERE school = 'Universidade São Francisco';
UPDATE young_company.schools SET school = 'Universidade Estadual de Campinas' WHERE school = 'University of Campinas';
UPDATE young_company.schools SET school = 'Fudan University' WHERE school = 'Fudan University, Shanghai (PRC) & University Assas (Paris II)';
UPDATE young_company.schools SET school = 'Universidad Nacional Autónoma de México' WHERE school = 'Universidad Nacional Autonoma de Mexico';
UPDATE young_company.schools SET school = 'George Washington University' WHERE school = 'The George Washington University Law School';
UPDATE young_company.schools SET school = 'University of Westminster' WHERE school = 'Westminster Schools';
UPDATE young_company.schools SET school = 'University of Delhi' WHERE school = 'Delhi Public School, Vasant Kunj';
UPDATE young_company.schools SET school = 'University of North Carolina at Charlotte' WHERE school = 'University of North Carolina at Wilmington';
UPDATE young_company.schools SET school = 'University of Hawaiʻi at Mānoa' WHERE school = 'University of Hawaii at Manoa';
UPDATE young_company.schools SET school = 'Università di Padova' WHERE school = 'Università degli Studi di Padova';
UPDATE young_company.schools SET school = 'Alma Mater Studiorum - University of Bologna' WHERE school = 'University of Bologna';
UPDATE young_company.schools SET school = 'Ecole des Ponts ParisTech' WHERE school = 'ENSTA ParisTech - École Nationale Supérieure de Techniques Avancées';
UPDATE young_company.schools SET school = 'Universitat de Barcelona' WHERE school = 'ESADE Business School, Barcelona';
UPDATE young_company.schools SET school = 'Universidad La Salle' WHERE school = 'Instituto La Salle Florida';
UPDATE young_company.schools SET school = 'University of Salamanca' WHERE school = 'Universidad Pontificia de Salamanca';
UPDATE young_company.schools SET school = 'University of Deusto' WHERE school = 'Universidad de Deusto';
UPDATE young_company.schools SET school = 'Universidad de Buenos Aires' WHERE school = 'University of Buenos Aires';
UPDATE young_company.schools SET school = 'Colorado State University' WHERE school = 'Colorado College';
UPDATE young_company.schools SET school = 'University of Washington' WHERE school = 'University of California, Washington Center';
UPDATE young_company.schools SET school = 'Lehigh University' WHERE school = 'Lehigh University College of Business';
UPDATE young_company.schools SET school = 'Tampere University' WHERE school = 'Tampere University of Applied Sciences';
UPDATE young_company.schools SET school = 'Amrita Vishwa Vidyapeetham' WHERE school = 'AMRITA VISHWA VIDYAPEETHAM';
UPDATE young_company.schools SET school = 'ETH Zurich - Swiss Federal Institute of Technology' WHERE school = 'ETH Zürich - Swiss Federal Institute of Technology Zurich';
UPDATE young_company.schools SET school = 'University of Brighton' WHERE school = 'Brighton College';
UPDATE young_company.schools SET school = 'KAIST - Korea Advanced Institute of Science & Technology' WHERE school = 'Korea Advanced Institute of Science and Technology';
UPDATE young_company.schools SET school = 'Trinity College Dublin, The University of Dublin' WHERE school = 'university of dublin, trinity college';
UPDATE young_company.schools SET school = 'Paul Valéry University Montpellier' WHERE school = 'Université Paul Valéry';
UPDATE young_company.schools SET school = 'Technical University of Crete' WHERE school = 'Technical University of Crete / Πολυτεχνείο Κρήτης';
UPDATE young_company.schools SET school = 'City, University of London' WHERE school = 'City of London School';
UPDATE young_company.schools SET school = 'Universiti Teknologi Malaysia ' WHERE school = 'Universiti Teknologi Malaysia';
UPDATE young_company.schools SET school = 'Universiti Teknologi MARA - UiTM' WHERE school = 'Universiti Teknologi MARA';
UPDATE young_company.schools SET school = 'The University of Alabama' WHERE school = 'University of Alabama';
UPDATE young_company.schools SET school = 'Università degli Studi di Perugia' WHERE school = 'Università per Stranieri di Perugia';
UPDATE young_company.schools SET school = 'Xiamen University' WHERE school = 'Xiamen University, Xiamen, Fujian, P.R. China';
UPDATE young_company.schools SET school = 'Middlesex University' WHERE school = 'Middlesex School';
UPDATE young_company.schools SET school = 'City University of New York' WHERE school = 'Baruch College, City University of New York';
UPDATE young_company.schools SET school = 'Pontificia Universidad Católica del Perú' WHERE school = 'Pontificia Universidad Catolica de Perú';
UPDATE young_company.schools SET school = 'Universidad Nacional de Ingeniería Peru' WHERE school = 'Universidad Nacional de Ingenieria (INICTEL)';
UPDATE young_company.schools SET school = 'Harvard University' WHERE school = 'Harvard Medical School';
UPDATE young_company.schools SET school = 'University of Bergen' WHERE school = 'Universitetet i Bergen';
UPDATE young_company.schools SET school = 'Yale University' WHERE school = 'Yale-NUS College';
UPDATE young_company.schools SET school = 'Universidade Federal do Paraná - UFPR' WHERE school = 'Universidade Tecnológica Federal do Paraná';
UPDATE young_company.schools SET school = 'Sungkyunkwan University' WHERE school = 'SKK GSB Sungkyunkwan University';
UPDATE young_company.schools SET school = 'Université Laval' WHERE school = 'Université Laval / Laval University';
UPDATE young_company.schools SET school = 'University of Southern California' WHERE school = 'University of Southern California - Marshall School of Business';
UPDATE young_company.schools SET school = 'University of Salford' WHERE school = 'The University of Salford';
UPDATE young_company.schools SET school = 'University of Washington' WHERE school = 'Washington International School';
UPDATE young_company.schools SET school = 'University of Washington' WHERE school = 'University of Washington Information School';
UPDATE young_company.schools SET school = 'University of California, Berkeley' WHERE school = 'UC Berkeley MS ME';
UPDATE young_company.schools SET school = 'University of Michigan-Ann Arbor' WHERE school = 'University of Michigan-Dearborn';
UPDATE young_company.schools SET school = 'Universidad Metropolitana' WHERE school = 'Universidad Metropolitana (VE)';
UPDATE young_company.schools SET school = 'Institut National des Sciences Appliquées de Lyon' WHERE school = 'INSA Lyon - Institut National des Sciences Appliquées de Lyon';
UPDATE young_company.schools SET school = 'Novosibirsk State Technical University' WHERE school = 'Novosibirsk State Technical University (NSTU)';
UPDATE young_company.schools SET school = 'Ajou University ' WHERE school = 'Ajou University';
UPDATE young_company.schools SET school = 'Albert-Ludwigs-Universitaet Freiburg' WHERE school = 'Université de Fribourg - Universität Freiburg';
UPDATE young_company.schools SET school = 'Texas Tech University' WHERE school = 'Texas State University';
UPDATE young_company.schools SET school = 'Washington University in St. Louis' WHERE school = 'Washington University, St. Louis';
UPDATE young_company.schools SET school = 'Universidad de Puerto Rico' WHERE school = 'University of Puerto Rico-Mayaguez';
UPDATE young_company.schools SET school = 'University of St.Gallen' WHERE school = 'Universität St.Gallen';
UPDATE young_company.schools SET school = 'George Mason University' WHERE school = 'George Mason University School of Law';
UPDATE young_company.schools SET school = 'University of Brighton' WHERE school = 'Brighton University';
UPDATE young_company.schools SET school = 'Tecnológico de Costa Rica -TEC' WHERE school = 'Instituto Tecnológico de Costa Rica';
UPDATE young_company.schools SET school = 'Universidad de Costa Rica' WHERE school = 'Centro Educativo Adventista de Costa Rica';
UPDATE young_company.schools SET school = 'Portland State University' WHERE school = 'University of Portland';
UPDATE young_company.schools SET school = 'Auburn University' WHERE school = 'Auburn University Harbert College of Business';
UPDATE young_company.schools SET school = 'Utrecht University' WHERE school = 'Utrecht School of Arts';
UPDATE young_company.schools SET school = 'Universidad de Zaragoza' WHERE school = 'University of Zaragoza, Spain';
UPDATE young_company.schools SET school = 'University of Glasgow' WHERE school = 'The University of Glasgow';
UPDATE young_company.schools SET school = 'Instituto Tecnológico de Buenos Aires' WHERE school = 'Cultural Inglesa de Buenos Aires';
UPDATE young_company.schools SET school = 'Instituto Tecnológico de Buenos Aires' WHERE school = 'ITBA (Instituto Tecnologico de Buenos Aires)';
UPDATE young_company.schools SET school = 'Griffith University' WHERE school = 'Griffith University (Australia)';
UPDATE young_company.schools SET school = 'Queen''s University at Kingston' WHERE school = 'Queens University';
UPDATE young_company.schools SET school = 'Universidad Católica Andres Bello' WHERE school = 'UCAB - Universidad Católica Andrés Bello';
UPDATE young_company.schools SET school = 'The University of Melbourne' WHERE school = 'The Univeristy of Melbourne';
UPDATE young_company.schools SET school = 'Tufts University' WHERE school = 'Tufts University School of Medicine';
UPDATE young_company.schools SET school = 'University of Bath' WHERE school = 'The University of Bath';
UPDATE young_company.schools SET school = 'University of Minnesota Twin Cities' WHERE school = 'University of Minnesota, Twin Cities';
UPDATE young_company.schools SET school = 'National Yang Ming Chiao Tung University' WHERE school = 'National Yang Ming University';
UPDATE young_company.schools SET school = 'The University of Tennessee, Knoxville' WHERE school = 'The University of Tennessee Knoxville';
UPDATE young_company.schools SET school = 'Jagiellonian University' WHERE school = 'Jagiellonian University\
Master of Science';
UPDATE young_company.schools SET school = 'Florida State University' WHERE school = 'Florida State University College of Law';
UPDATE young_company.schools SET school = 'University of Bristol' WHERE school = 'Bristol University';
UPDATE young_company.schools SET school = 'Princeton University' WHERE school = 'The Hun School of Princeton';
UPDATE young_company.schools SET school = 'Universidad Externado de Colombia ' WHERE school = 'Universidad Externado de Colombia';
UPDATE young_company.schools SET school = 'University of Texas Dallas' WHERE school = 'The University of Dallas';
UPDATE young_company.schools SET school = 'Universidade Federal de Santa Catarina ' WHERE school = 'Universidade Federal de Santa Catarina';
UPDATE young_company.schools SET school = 'Loyola University Chicago' WHERE school = 'Loyola College';
UPDATE young_company.schools SET school = 'Pontificia Universidad Católica Argentina' WHERE school = 'Pontificia Universidad Católica de Córdoba';
UPDATE young_company.schools SET school = 'Ateneo de Manila University' WHERE school = 'Ateneo de Manila University (dropped out)';
UPDATE young_company.schools SET school = 'The Ohio State University' WHERE school = 'The Ohio State University\
\
2011';
UPDATE young_company.schools SET school = 'Gwangju Institute of Science and Technology' WHERE school = 'Gwangju institute of science and technology';
UPDATE young_company.schools SET school = 'Georgetown University' WHERE school = 'Georgetown';
UPDATE young_company.schools SET school = 'Université du Québec' WHERE school = 'Université du Québec à Rimouski';
UPDATE young_company.schools SET school = 'ITESO, Universidad Jesuita de Guadalajara' WHERE school = 'ITESO Universidad Jesuita de Guadalajara';
UPDATE young_company.schools SET school = 'Auburn University' WHERE school = 'Auburn University, College of Business';
UPDATE young_company.schools SET school = 'Universidad de Carabobo' WHERE school = 'Universidad de Carabobo (VE)';
UPDATE young_company.schools SET school = 'Universität Mannheim' WHERE school = 'University of Mannheim';
UPDATE young_company.schools SET school = 'New York University' WHERE school = 'New York University School of Law';
UPDATE young_company.schools SET school = 'Universidad Nacional de Ingeniería Peru' WHERE school = 'Universidad Nacional de Ingenieria';
UPDATE young_company.schools SET school = 'University at Buffalo SUNY' WHERE school = 'University at Buffalo';
UPDATE young_company.schools SET school = 'Tallinn University ' WHERE school = 'Tallinna Reaalkool';
UPDATE young_company.schools SET school = 'Clark University' WHERE school = 'Clark Atlanta University';
UPDATE young_company.schools SET school = 'Brown University' WHERE school = 'Brown University School of Engineering';
UPDATE young_company.schools SET school = 'National University of Modern Languages, Islamabad' WHERE school = 'National University of Modern Languages (NUML)';
UPDATE young_company.schools SET school = 'Beijing Normal University ' WHERE school = 'Beijing Normal University';
UPDATE young_company.schools SET school = 'Indian Institute of Technology Hyderabad' WHERE school = 'IIIT Hyderabad';
UPDATE young_company.schools SET school = 'Uppsala University' WHERE school = 'University of Uppsala, Sweden';
UPDATE young_company.schools SET school = 'Indian Institute of Technology Hyderabad' WHERE school = 'Indian Institute of Technology, Hyderabad';
UPDATE young_company.schools SET school = 'Universiti Malaya' WHERE school = 'University of Malaya';
UPDATE young_company.schools SET school = 'Tulane University' WHERE school = 'Tulane University School of Medicine';
UPDATE young_company.schools SET school = 'Duke University' WHERE school = 'Duke University School of Medicine';
UPDATE young_company.schools SET school = 'The University of New South Wales' WHERE school = 'University of Wales, Aberystwyth';
UPDATE young_company.schools SET school = 'TU Dortmund University' WHERE school = 'Technical University Dortmund';
UPDATE young_company.schools SET school = 'Ruhr-Universität Bochum' WHERE school = 'Ruhr University Bochum';
UPDATE young_company.schools SET school = 'Novosibirsk State Technical University' WHERE school = 'Novosibirsk State University of Economics and Management';
UPDATE young_company.schools SET school = 'University of Tehran' WHERE school = 'Tehran University of Medical Sciences';
UPDATE young_company.schools SET school = 'Dalian University of Technology' WHERE school = 'Dalian University of Technology, China';
UPDATE young_company.schools SET school = 'Ateneo de Manila University' WHERE school = 'Ateneo De Manila University School of Law';
UPDATE young_company.schools SET school = 'Perm State National Research University' WHERE school = 'Perm State University (PSU)';
UPDATE young_company.schools SET school = 'Duke University' WHERE school = 'Duke Graduate School';
UPDATE young_company.schools SET school = 'Lancaster University' WHERE school = 'University of Lancaster';
UPDATE young_company.schools SET school = 'Aristotle University of Thessaloniki' WHERE school = 'Aristotle University of Thessaloniki (AUTH)';
UPDATE young_company.schools SET school = 'Universität Duisburg-Essen' WHERE school = 'Universität Duisburg-Essen, Standort Duisburg';
UPDATE young_company.schools SET school = 'University Paris 2 Panthéon-Assas' WHERE school = 'Université Panthéon Assas (Paris II)';
UPDATE young_company.schools SET school = 'University of Milano-Bicocca ' WHERE school = 'Università degli Studi di Milano-Bicocca';
UPDATE young_company.schools SET school = 'Università degli studi di Bergamo' WHERE school = 'Università degli Studi di Bergamo';
UPDATE young_company.schools SET school = 'University Paris 2 Panthéon-Assas' WHERE school = 'Université Panthéon Assas';
UPDATE young_company.schools SET school = 'Freie Universitaet Berlin' WHERE school = 'Freie Universität Berlin';
UPDATE young_company.schools SET school = 'Financial University under the Government of the Russian Federation' WHERE school = 'Finance University under the Government of the Russian Federation';
UPDATE young_company.schools SET school = 'Pontificia Universidad Católica de Chile' WHERE school = 'Pontificia Universidad Católica of Chile';
UPDATE young_company.schools SET school = 'Ben-Gurion University of The Negev' WHERE school = 'Ben-Gurion University of the Negev';
UPDATE young_company.schools SET school = 'University of Illinois at Urbana-Champaign' WHERE school = 'Univeristy of Illinois Cahmpaign-Urbana';
UPDATE young_company.schools SET school = 'Indian Institute of Technology Kanpur' WHERE school = 'National Institute of Technology Raipur';
UPDATE young_company.schools SET school = 'Universität Hamburg' WHERE school = 'University of Hamburg';
UPDATE young_company.schools SET school = 'University of Toronto' WHERE school = 'University of Toronto - University of Trinity College';
UPDATE young_company.schools SET school = 'York University' WHERE school = 'York College';
UPDATE young_company.schools SET school = 'Universidad Pontificia Comillas' WHERE school = 'Universidad Pontificia de Comillas (ICADE)';
UPDATE young_company.schools SET school = 'Athens University of Economics and Business' WHERE school = 'Stockholm School of Economics';
UPDATE young_company.schools SET school = 'Stockholm University' WHERE school = 'Stockholms universitet';
UPDATE young_company.schools SET school = 'Comenius University Bratislava' WHERE school = 'Comenius University in Bratislava';
UPDATE young_company.schools SET school = 'University of Texas at Austin' WHERE school = 'The University of Texas at Austin - The Red McCombs School of Business';
UPDATE young_company.schools SET school = 'Pontificia Universidad Catolica Madre y Maestra' WHERE school = 'Pontificia Universidad Católica Madre y Maestra';
UPDATE young_company.schools SET school = 'Universidad Tecnica Particular De Loja' WHERE school = 'Universidad Tecnologica de Jalisco';
UPDATE young_company.schools SET school = 'Pontificia Universidad Catolica Madre y Maestra' WHERE school = 'Pontificia Universidad Católica Madre y Maestra (PUCMM)';
UPDATE young_company.schools SET school = 'Instituto Tecnológico de Buenos Aires' WHERE school = 'Instituto tecnologico de las Americas (ITLA)';
UPDATE young_company.schools SET school = 'University of the Philippines' WHERE school = 'University of the Philippines Diliman';
UPDATE young_company.schools SET school = 'University of Ljubljana' WHERE school = 'Biotechnical faculty, University of Ljubljana';
UPDATE young_company.schools SET school = 'Texas A&M University' WHERE school = 'Texas A&amp;M University - Dwight Look College of Engineering';
UPDATE young_company.schools SET school = 'Complutense University of Madrid' WHERE school = 'Universidad Complutense de Madrid';
UPDATE young_company.schools SET school = 'Stanford University' WHERE school = 'Stanford University Graduate School of Education';
UPDATE young_company.schools SET school = 'Washington University in St. Louis' WHERE school = 'Washington University in St Louis';
UPDATE young_company.schools SET school = 'Umea University' WHERE school = 'Umeå universitet';
UPDATE young_company.schools SET school = 'Université de Tunis' WHERE school = 'National Polytechnical School, Algiers, Algeria';
UPDATE young_company.schools SET school = 'Université de Sherbrooke' WHERE school = 'University of sherbrooke, QC, Canada';
UPDATE young_company.schools SET school = 'University of Ljubljana' WHERE school = 'University of Ljubljana, Biotechnical faculty';
UPDATE young_company.schools SET school = 'Christian-Albrechts-University zu Kiel' WHERE school = 'Christian-Albrechts-Universität zu Kiel';
UPDATE young_company.schools SET school = 'Jawaharlal Nehru University' WHERE school = 'Jawaharlal Nehru Technological University';
UPDATE young_company.schools SET school = 'San Francisco State University' WHERE school = 'San Francisco State University. College of Extended Learning.';
UPDATE young_company.schools SET school = 'Universidad Politécnica de Madrid' WHERE school = 'Universidad Politécnica de Madrid\
.';
UPDATE young_company.schools SET school = 'Universidad de La Sabana' WHERE school = 'Universidad de la Sabana';
UPDATE young_company.schools SET school = 'Curtin University' WHERE school = 'Curtin University of Technology';
UPDATE young_company.schools SET school = 'University of Basel' WHERE school = 'Universität Basel';
UPDATE young_company.schools SET school = 'Lund University' WHERE school = 'Lunds universitet';
UPDATE young_company.schools SET school = 'Goethe-University Frankfurt am Main' WHERE school = 'Johann Wolfgang Goethe-Universität Frankfurt am Main';
UPDATE young_company.schools SET school = 'Universidade de Brasília' WHERE school = 'Universidade Católica de Brasília';
UPDATE young_company.schools SET school = 'Universidad Autonoma de Yucatan' WHERE school = 'Universidad Autónoma de Yucatán';
UPDATE young_company.schools SET school = "King Mongkut\''s Institute of Technology Ladkrabang" WHERE school = "King Mongkut''s Institute of Technology Ladkrabang";
UPDATE young_company.schools SET school = 'Albert-Ludwigs-Universitaet Freiburg' WHERE school = 'The University of Freiburg';
UPDATE young_company.schools SET school = 'Graz University of Technology' WHERE school = 'Technical University Graz';
UPDATE young_company.schools SET school = 'Perm State National Research University' WHERE school = 'Perm State Technical University';
UPDATE young_company.schools SET school = 'University of Siena' WHERE school = 'Università di Siena';
UPDATE young_company.schools SET school = 'The New School' WHERE school = 'New economic school';
UPDATE young_company.schools SET school = 'Purdue University' WHERE school = 'Purdue University Krannert School of Management';
UPDATE young_company.schools SET school = 'Université de Montréal ' WHERE school = 'University of Montreal';
UPDATE young_company.schools SET school = 'California Polytechnic State University' WHERE school = 'California State Polytechnic University-Pomona';
UPDATE young_company.schools SET school = 'University of Witwatersrand' WHERE school = 'University of the Witwatersrand';
UPDATE young_company.schools SET school = 'Delft University of Technology' WHERE school = 'Technical university Delft';
UPDATE young_company.schools SET school = 'Belarusian State University of Informatics and Radioelectronics' WHERE school = 'Belarussian State University of Informatics and Radioelectronics';
UPDATE young_company.schools SET school = 'Michigan State University' WHERE school = 'University of Michigan Law School';
UPDATE young_company.schools SET school = 'University of Hawaiʻi at Mānoa' WHERE school = 'University of Hawai‘i System';
UPDATE young_company.schools SET school = 'University of Delhi' WHERE school = 'Delhi Public School Pune';
UPDATE young_company.schools SET school = 'University of British Columbia' WHERE school = 'The University of British Columbia / UBC (Okanagan)';
UPDATE young_company.schools SET school = 'University of Pisa' WHERE school = 'Università di Pisa / University of Pisa';
UPDATE young_company.schools SET school = 'Syracuse University' WHERE school = 'SUNY ESF/Syracuse University';
UPDATE young_company.schools SET school = 'University of Colorado, Denver ' WHERE school = 'University of Colorado at Denver';
UPDATE young_company.schools SET school = 'University of California, Berkeley' WHERE school = 'UC Berkeley School of Information';
UPDATE young_company.schools SET school = 'Qatar University' WHERE school = 'Georgetown University & Qatar University';
UPDATE young_company.schools SET school = 'Universitas Pendidikan Indonesia' WHERE school = 'UNIVERSITAS PENDIDIKAN INDONESIA';
UPDATE young_company.schools SET school = 'University of Colorado Boulder' WHERE school = 'University of Colorado at Boulder - Leeds School of Business';
UPDATE young_company.schools SET school = 'University of Notre Dame' WHERE school = 'Notre Dame of Maryland University';
UPDATE young_company.schools SET school = 'Dartmouth College' WHERE school = 'Thayer School of Engineering at Dartmouth';
UPDATE young_company.schools SET school = 'Ben-Gurion University of The Negev' WHERE school = 'Ben Gurion university';
UPDATE young_company.schools SET school = 'California Polytechnic State University' WHERE school = 'California State University, Stanislaus';
UPDATE young_company.schools SET school = 'Tecnológico de Monterrey' WHERE school = 'Instituto Tecnológico y de Estudios Superiores de Monterrey';
UPDATE young_company.schools SET school = 'University of the Pacific' WHERE school = 'University of Asia and the Pacific';
UPDATE young_company.schools SET school = 'Cornell University' WHERE school = 'Cornell Law School';
UPDATE young_company.schools SET school = 'University of Maryland, College Park' WHERE school = 'University of Maryland College Park\
BS \
2006';
UPDATE young_company.schools SET school = 'University of Ljubljana' WHERE school = 'University of Ljubljana, Faculty of Arts';
UPDATE young_company.schools SET school = 'Wageningen University & Research' WHERE school = 'Wageningen University';
UPDATE young_company.schools SET school = 'Tel Aviv University' WHERE school = 'The Academic College of Tel-Aviv, Yaffo';
UPDATE young_company.schools SET school = 'Birla Institute of Technology and Science, Pilani' WHERE school = 'Birla Institute of Technology and Science';
UPDATE young_company.schools SET school = 'University of Warsaw ' WHERE school = 'University of Warsaw';
UPDATE young_company.schools SET school = 'The New School' WHERE school = 'Modern School';
UPDATE young_company.schools SET school = 'Universidade Estadual de Campinas' WHERE school = 'Universidade Estadual de Campinas (UNICAMP)';
UPDATE young_company.schools SET school = 'Universidade Federal do Rio de Janeiro' WHERE school = 'Universidade Federal do Rio De Janeiro';
UPDATE young_company.schools SET school = 'Sorbonne University' WHERE school = 'Sorbonne Université';
UPDATE young_company.schools SET school = 'The University of Exeter' WHERE school = 'Exeter College, Oxford';
UPDATE young_company.schools SET school = 'Universidad de Antioquia' WHERE school = 'Universidad de Antioquía';
UPDATE young_company.schools SET school = 'Universidad de San Andrés - UdeSA' WHERE school = 'Universidad de ''San Andrés''';
UPDATE young_company.schools SET school = 'Universidad Austral' WHERE school = 'Universidad Austral, Argentina';
UPDATE young_company.schools SET school = 'University of Madras' WHERE school = 'Vidya Mandir Senior Secondary School';
UPDATE young_company.schools SET school = 'University of Hyderabad' WHERE school = 'The Hyderabad Public School Ramanthapur - HPS (R)';
UPDATE young_company.schools SET school = 'Moscow Institute of Physics and Technology' WHERE school = 'National Research University of Information Technologies, Mechanics and Optics. St. Petersburg';
UPDATE young_company.schools SET school = 'Birla Institute of Technology and Science, Pilani' WHERE school = 'Birla Institute of Technology and Science - Pilani, Goa';
UPDATE young_company.schools SET school = 'University of Delhi' WHERE school = 'Faculty of Management Studies - University of Delhi';
UPDATE young_company.schools SET school = 'Singapore Management University' WHERE school = 'Management Development Institute of Singapore';
UPDATE young_company.schools SET school = 'Peter the Great St. Petersburg Polytechnic University' WHERE school = 'St Petersburg State Polytechnic University';
UPDATE young_company.schools SET school = 'Tufts University' WHERE school = 'Tufts';
UPDATE young_company.schools SET school = 'Universität Bremen' WHERE school = 'Jacobs University Bremen / International University Bremen';
UPDATE young_company.schools SET school = 'Kaunas University of Technology' WHERE school = 'Kaunas University of Technology Gymnasium';
UPDATE young_company.schools SET school = 'Istanbul University' WHERE school = 'Istanbul Sehir University';
UPDATE young_company.schools SET school = 'Ain Shams University ' WHERE school = 'Ain Shams University';
UPDATE young_company.schools SET school = 'Cornell University' WHERE school = 'Cornell University Graduate School';
UPDATE young_company.schools SET school = 'Purdue University' WHERE school = 'Purdue University Fort Wayne';
UPDATE young_company.schools SET school = 'Ben-Gurion University of The Negev' WHERE school = 'Ben Gurion University (BGU)';
UPDATE young_company.schools SET school = 'Royal Holloway University of London' WHERE school = 'Royal Holloway, University of London';
UPDATE young_company.schools SET school = 'Bauman Moscow State Technical University' WHERE school = 'Bauman Moscow State Technical University (BMSTU)';
UPDATE young_company.schools SET school = 'Siberian Federal University, SibFU' WHERE school = 'Siberian State University';
UPDATE young_company.schools SET school = 'University of Greenwich' WHERE school = 'University of Greenwich - United Kingdom';
UPDATE young_company.schools SET school = 'Southwest University' WHERE school = 'Southwestern University';
UPDATE young_company.schools SET school = 'Technion - Israel Institute of Technology' WHERE school = 'Technion Israel Institute of Technology';
UPDATE young_company.schools SET school = 'Harbin Institute of Technology' WHERE school = 'Harbin Institute of Technology at Weihai';
UPDATE young_company.schools SET school = 'Hacettepe University ' WHERE school = 'Hacettepe University';
UPDATE young_company.schools SET school = 'KU Leuven' WHERE school = 'KULeuven';
UPDATE young_company.schools SET school = 'University of Rome "Tor Vergata"' WHERE school = 'University of Rome Tor Vergata';
UPDATE young_company.schools SET school = 'Universidad Pontificia Comillas' WHERE school = 'Universidad Pontificia de Comillas';
UPDATE young_company.schools SET school = 'Universidade Federal de Pelotas ' WHERE school = 'Federal University of Pelotas';
UPDATE young_company.schools SET school = 'University of Ottawa' WHERE school = 'Ottawa University';
UPDATE young_company.schools SET school = 'Wesleyan University' WHERE school = 'Indiana Wesleyan University';
UPDATE young_company.schools SET school = 'SOAS University of London ' WHERE school = 'SOAS University of London';
UPDATE young_company.schools SET school = 'University of Murcia' WHERE school = 'Universidad de Murcia';
UPDATE young_company.schools SET school = 'Complutense University of Madrid' WHERE school = 'University of Madrid (UPM)';
UPDATE young_company.schools SET school = 'University of New Brunswick' WHERE school = 'Brunswick School';
UPDATE young_company.schools SET school = 'Universidade de São Paulo' WHERE school = 'University of Sao Paulo';
UPDATE young_company.schools SET school = 'University of Minho' WHERE school = 'Universidade do Minho';
UPDATE young_company.schools SET school = 'Johns Hopkins University' WHERE school = 'Hopkins School';
UPDATE young_company.schools SET school = 'University of Arkansas Fayetteville ' WHERE school = 'University of Arkansas at Fayetteville';
UPDATE young_company.schools SET school = 'University of Cambridge' WHERE school = 'Trinity College, University of Cambridge';
UPDATE young_company.schools SET school = 'Saint Petersburg Electrotechnical University ETU-LETI' WHERE school = 'Saint Petersburg Electrotechnical University \LETI\""';
UPDATE young_company.schools SET school = 'Colorado State University' WHERE school = 'Colorado State University-Global Campus';
UPDATE young_company.schools SET school = 'Taras Shevchenko National University of Kyiv' WHERE school = 'Kiev National Taras Shevchenko University';
UPDATE young_company.schools SET school = 'Taras Shevchenko National University of Kyiv' WHERE school = 'National Taras Shevchenko University, Kiev, Ukraine';
UPDATE young_company.schools SET school = 'Northwestern University' WHERE school = 'Northwestern University / VIT University';
UPDATE young_company.schools SET school = 'New York University' WHERE school = 'State University of New York';
UPDATE young_company.schools SET school = 'University of New Hampshire' WHERE school = 'Hampshire College';
UPDATE young_company.schools SET school = 'University of Madras' WHERE school = 'Madras University';
UPDATE young_company.schools SET school = 'KIT, Karlsruhe Institute of Technology' WHERE school = 'Karlsruher Institut für Technologie (KIT)';
UPDATE young_company.schools SET school = 'Université Paris 1 Panthéon-Sorbonne ' WHERE school = 'Université Panthéon Sorbonne';
UPDATE young_company.schools SET school = 'Universidade de São Paulo' WHERE school = 'Universidade Metodista de São Paulo';
UPDATE young_company.schools SET school = 'University of Delhi' WHERE school = 'Delhi Public School - India';
UPDATE young_company.schools SET school = 'SRM INSTITUTE OF SCIENCE AND TECHNOLOGY' WHERE school = 'SRM Institute of Science and Technology';
UPDATE young_company.schools SET school = 'Khulna University of Engineering and Technology' WHERE school = 'K.J.Somaiya College of Engineering';
UPDATE young_company.schools SET school = 'Indian Institute of Technology Hyderabad' WHERE school = 'Indian Institute of Technology, Patna';
UPDATE young_company.schools SET school = 'University of Salford' WHERE school = 'Salisbury University';
UPDATE young_company.schools SET school = 'University of Colombo' WHERE school = 'Royal College Colombo';
UPDATE young_company.schools SET school = 'Pontificia Universidade Católica do Minas Gerais - PUC Minas' WHERE school = 'Pontificia Universidade Católica de Minas Gerais';
UPDATE young_company.schools SET school = 'The University of Hong Kong' WHERE school = 'Hong Kong University';
UPDATE young_company.schools SET school = 'University of Massachusetts Boston' WHERE school = 'Massachusetts College of Liberal Arts';
UPDATE young_company.schools SET school = 'Harvard University' WHERE school = 'Harvard Innovation Labs';
UPDATE young_company.schools SET school = 'Tecnológico de Monterrey' WHERE school = 'Prepa Tec Monterrey';
UPDATE young_company.schools SET school = 'University of Chicago' WHERE school = 'University of Chicago Law School';
UPDATE young_company.schools SET school = 'University at Buffalo SUNY' WHERE school = 'Northwestern University/SUNY Buffalo';
UPDATE young_company.schools SET school = 'Northwest University' WHERE school = 'Northwest A&F University';
UPDATE young_company.schools SET school = 'Bina Nusantara University' WHERE school = 'Universitas Bina Nusantara (Binus)';
UPDATE young_company.schools SET school = 'Peter the Great St. Petersburg Polytechnic University' WHERE school = 'Peter the Great St.Petersburg Polytechnic University';
UPDATE young_company.schools SET school = 'Saint Petersburg State University' WHERE school = 'Saint Petersburg University of Telecommunications';
UPDATE young_company.schools SET school = 'The University of Tennessee, Knoxville' WHERE school = 'University of Tennessee, Knoxville';
UPDATE young_company.schools SET school = 'Mendel University in Brno' WHERE school = 'Mendel University';
UPDATE young_company.schools SET school = 'University of Toronto' WHERE school = 'University of Toronto - Woodsworth College';
UPDATE young_company.schools SET school = 'Loughborough University' WHERE school = 'Loughborough Grammar School';
UPDATE young_company.schools SET school = 'University at Albany SUNY' WHERE school = 'University at Albany, SUNY';
UPDATE young_company.schools SET school = 'Carnegie Mellon University' WHERE school = 'Carnegie Mellon University - Tepper School of Business';
UPDATE young_company.schools SET school = 'Université de Lille' WHERE school = 'Université des Sciences et Technologies de Lille (Lille I)';
UPDATE young_company.schools SET school = 'Heriot-Watt University' WHERE school = 'Heriot Watt University Edinburgh';
UPDATE young_company.schools SET school = 'The University of Edinburgh' WHERE school = 'University of Edinburgh';
UPDATE young_company.schools SET school = 'Savitribai Phule Pune University' WHERE school = 'College of Engineering Pune';
UPDATE young_company.schools SET school = 'University of Virginia' WHERE school = 'The University of Virginia';
UPDATE young_company.schools SET school = 'Università degli studi Roma Tre' WHERE school = 'Università degli Studi di Roma Tre';
UPDATE young_company.schools SET school = 'Cornell University' WHERE school = 'Joan & Sanford I. Weill Medical College of Cornell University';
UPDATE young_company.schools SET school = 'The Ohio State University' WHERE school = 'Ohio State University';
UPDATE young_company.schools SET school = 'Washington University in St. Louis' WHERE school = 'Olin Business School at Washington University in St. Louis';
UPDATE young_company.schools SET school = 'Case Western Reserve University' WHERE school = 'Western Reserve Academy';
UPDATE young_company.schools SET school = 'OSMANIA UNIVERSITY' WHERE school = 'Osmania University';
UPDATE young_company.schools SET school = 'Indian Institute of Science' WHERE school = 'Indian Institute of Sciences, Bangalore';
UPDATE young_company.schools SET school = 'Indian Institute of Technology Hyderabad' WHERE school = 'International Institute of Information Technology, Hyderabad (IIIT-Hyderabad)';
UPDATE young_company.schools SET school = 'De La Salle University' WHERE school = 'La Salle University';
UPDATE young_company.schools SET school = 'Università degli studi Roma Tre' WHERE school = 'Università degli Studi di Napoli Federico II';
UPDATE young_company.schools SET school = 'Boston University' WHERE school = 'Boston University Academy';
UPDATE young_company.schools SET school = 'University of Washington' WHERE school = 'University of Washington School of Law';
UPDATE young_company.schools SET school = 'University of Illinois at Urbana-Champaign' WHERE school = 'University of Illinois Urbana-Champaign';
UPDATE young_company.schools SET school = 'Albert-Ludwigs-Universitaet Freiburg' WHERE school = 'Albert-Ludwig University Freiburg';
UPDATE young_company.schools SET school = 'University of Klagenfurt' WHERE school = 'Alpen-Adria-University Klagenfurt';
UPDATE young_company.schools SET school = 'Universidade de Santiago de Compostela' WHERE school = 'University of Santiago de Compostela';
UPDATE young_company.schools SET school = 'Université Jean Moulin Lyon 3' WHERE school = 'Université Jean Moulin';
UPDATE young_company.schools SET school = 'University of St.Gallen' WHERE school = 'Universität St.Gallen (HSG)';
UPDATE young_company.schools SET school = 'Queensland University of Technology' WHERE school = 'QUT (Queensland University of Technology)';
UPDATE young_company.schools SET school = 'National Technical University of Ukraine "Igor Sikorsky Kyiv Polytechnic Institute"' WHERE school = 'National Technical University of Ukraine ''Kyiv Polytechnic Institute''';
UPDATE young_company.schools SET school = 'Kwame Nkrumah University of Science and Technology' WHERE school = 'Kwame Nkrumah'' University of Science and Technology, Kumasi';
UPDATE young_company.schools SET school = 'University of Copenhagen' WHERE school = 'IT University of Copenhagen';
UPDATE young_company.schools SET school = 'University of the Punjab' WHERE school = 'Punjab Engineering College';
UPDATE young_company.schools SET school = 'City University of New York' WHERE school = 'City University of New York-Hunter College';
UPDATE young_company.schools SET school = 'International University of Business, Agriculture and Technology' WHERE school = 'Jomo Kenyatta University of Agriculture and Technology';
UPDATE young_company.schools SET school = 'University of Calgary' WHERE school = 'The University of Calgary';
UPDATE young_company.schools SET school = 'Bielefeld University' WHERE school = 'Universität Bielefeld';
UPDATE young_company.schools SET school = 'Concordia University' WHERE school = 'Concordia University, Montreal';
UPDATE young_company.schools SET school = 'University of Illinois at Chicago' WHERE school = 'University of Illinois College of Medicine';
UPDATE young_company.schools SET school = 'Universidad de Costa Rica' WHERE school = 'Universidad Latina de Costa Rica';
UPDATE young_company.schools SET school = 'University of Delhi' WHERE school = 'Delhi Public School Bangalore North';
UPDATE young_company.schools SET school = 'Université Paris-Nanterre' WHERE school = 'Université Paris Nanterre';
UPDATE young_company.schools SET school = 'University of Miami' WHERE school = 'University of Miami Medical School';
UPDATE young_company.schools SET school = 'University Politehnica of Timisoara, UPT' WHERE school = 'Politehnica University of Timisoara';
UPDATE young_company.schools SET school = 'University of New Mexico' WHERE school = 'The University of New Mexico';
UPDATE young_company.schools SET school = 'National University of Singapore' WHERE school = 'Institute of Systems Sciences, National University of Singapore';
UPDATE young_company.schools SET school = 'Moscow City University' WHERE school = 'Moscow State University';
UPDATE young_company.schools SET school = 'Ankara Üniversitesi' WHERE school = 'Ankara University';
UPDATE young_company.schools SET school = 'Friedrich-Alexander-Universität Erlangen-Nürnberg' WHERE school = 'Friedrich-Alexander-University Erlangen-Nuremberg (Germany)';
UPDATE young_company.schools SET school = 'Eberhard Karls Universität Tübingen' WHERE school = 'Eberhard-Karls-University Tübingen (Germany)';
UPDATE young_company.schools SET school = 'Universidad de Guadalajara' WHERE school = 'University of Guadalajara (Mexico)';
UPDATE young_company.schools SET school = 'Philipps-Universität Marburg ' WHERE school = 'Philipps';
UPDATE young_company.schools SET school = 'Macquarie University' WHERE school = 'Macquarie Graduate School of Management';
UPDATE young_company.schools SET school = 'King Fahd University of Petroleum & Minerals' WHERE school = 'King Fahd University of Petroleum & Minerals - KFUPM';
UPDATE young_company.schools SET school = 'New York University' WHERE school = 'New York University School of Medicine';
UPDATE young_company.schools SET school = 'Johns Hopkins University' WHERE school = 'Johns Hopkins University School of Advanced International Studies (SAIS)';
UPDATE young_company.schools SET school = 'University of Geneva' WHERE school = 'Geneva University (Mérieux Foundation)';
UPDATE young_company.schools SET school = 'University of Twente' WHERE school = 'Universiteit Twente';
UPDATE young_company.schools SET school = 'Russian Presidential Academy of National Economy and Public Administration' WHERE school = 'Academy of National Economy under the Government of the Russian Federation';
UPDATE young_company.schools SET school = 'University of Missouri, Kansas City' WHERE school = 'Missouri State University';
UPDATE young_company.schools SET school = 'Indian Institute of Technology Indore' WHERE school = 'IIM Indore';
UPDATE young_company.schools SET school = 'Tokai University' WHERE school = 'Tokai University\
\
2007';
UPDATE young_company.schools SET school = 'Newcastle University' WHERE school = 'Newcastle University (UK)';
UPDATE young_company.schools SET school = 'The University of Manchester' WHERE school = 'The University of Manchester 1st';
UPDATE young_company.schools SET school = 'University Paris 2 Panthéon-Assas' WHERE school = 'Panthéon-Assas université';
UPDATE young_company.schools SET school = 'The University of Queensland' WHERE school = 'University of Queensland';
UPDATE young_company.schools SET school = 'University of Klagenfurt' WHERE school = 'Universität Klagenfurt';
UPDATE young_company.schools SET school = 'Charles Sturt University ' WHERE school = 'Charles Sturt University';
UPDATE young_company.schools SET school = 'Tel Aviv University' WHERE school = 'Tel-Aviv University';
UPDATE young_company.schools SET school = 'Texas A&M University' WHERE school = 'Texas A&M University - Commerce';
UPDATE young_company.schools SET school = 'National University of Singapore' WHERE school = 'Singapore National Academy';
UPDATE young_company.schools SET school = 'Boston College' WHERE school = 'Boston College Carroll School of Management';
UPDATE young_company.schools SET school = 'California State University Long Beach' WHERE school = 'Cal State Long Beach';
UPDATE young_company.schools SET school = 'University of Michigan-Ann Arbor' WHERE school = 'University of Michigan Medical School';
UPDATE young_company.schools SET school = 'University of Hyderabad' WHERE school = 'JNTUH College of Engineering Hyderabad';
UPDATE young_company.schools SET school = 'Fundación Universidad De Bogotá-Jorge Tadeo Lozano' WHERE school = 'Universidad Jorge Tadeo Lozano';
UPDATE young_company.schools SET school = 'University of Virginia' WHERE school = 'University of Virginia School of Law';
UPDATE young_company.schools SET school = 'California State University Long Beach' WHERE school = 'California State University-Long Beach';
UPDATE young_company.schools SET school = 'University Paris 2 Panthéon-Assas' WHERE school = 'Panthéon Assas Paris University';
UPDATE young_company.schools SET school = 'Keio University' WHERE school = 'Keio Univercity';
UPDATE young_company.schools SET school = 'American University in Dubai' WHERE school = 'American School of Dubai';
UPDATE young_company.schools SET school = 'Texas A&M University' WHERE school = 'Texas A&M University-Corpus Christi';
UPDATE young_company.schools SET school = 'Birla Institute of Technology and Science, Pilani' WHERE school = 'Birla Institute of Technology & Science';
UPDATE young_company.schools SET school = 'Universitat Politècnica de València' WHERE school = 'Universitat Politècnica de València (UPV)';
UPDATE young_company.schools SET school = 'Université de Rennes 1' WHERE school = 'Université de Rennes I';
UPDATE young_company.schools SET school = 'Amity University' WHERE school = 'Amity International School';
UPDATE young_company.schools SET school = 'Universidade Federal do Ceará' WHERE school = 'IFCE (Instituto Federal do Ceará)';
UPDATE young_company.schools SET school = 'Moscow City University' WHERE school = 'Moscow State School 57';
UPDATE young_company.schools SET school = 'Universität Duisburg-Essen' WHERE school = 'University of Duisburg-Essen (Germany)';
UPDATE young_company.schools SET school = 'Erasmus University Rotterdam ' WHERE school = 'Erasmus Universiteit Rotterdam';
UPDATE young_company.schools SET school = 'Azerbaijan State Oil and Industry University' WHERE school = 'Azerbaijan State Oil Academy';
UPDATE young_company.schools SET school = 'Washington State University' WHERE school = 'Washington College';
UPDATE young_company.schools SET school = 'University of Canberra ' WHERE school = 'University of Canberra';
UPDATE young_company.schools SET school = 'University of Alabama, Birmingham' WHERE school = 'University of Alabama at Birmingham';
UPDATE young_company.schools SET school = 'Université du Québec' WHERE school = 'Université du Québec à Montréal';
UPDATE young_company.schools SET school = 'CUNY The City College of New York' WHERE school = 'The City College of New York';
UPDATE young_company.schools SET school = 'Zhejiang University' WHERE school = 'zhejiang university city collega';
UPDATE young_company.schools SET school = 'University of New Mexico' WHERE school = 'New Mexico State University';
UPDATE young_company.schools SET school = 'National Technical University of Ukraine "Igor Sikorsky Kyiv Polytechnic Institute"' WHERE school = 'National Technical University of Ukraine';
UPDATE young_company.schools SET school = 'Aristotle University of Thessaloniki' WHERE school = 'Aristotle University Thessaloniki';
UPDATE young_company.schools SET school = 'Moscow Institute of Physics and Technology' WHERE school = 'Bogomoletz Institute of Physiology (Kiev, Ukraine), Moscow Institute of Physics and Technology';
UPDATE young_company.schools SET school = 'Indian Institute of Technology Hyderabad' WHERE school = 'International Institute of Information Technology - Hyderabad';
UPDATE young_company.schools SET school = 'The University of Tokyo' WHERE school = 'University of Tokyo';
UPDATE young_company.schools SET school = 'Technische Universität Braunschweig' WHERE school = 'Technische Universität Carolo-Wilhelmina zu Braunschweig';
UPDATE young_company.schools SET school = 'Fordham University ' WHERE school = 'Fordham University';
UPDATE young_company.schools SET school = 'University of Waikato' WHERE school = 'The University of Waikato';
UPDATE young_company.schools SET school = 'Universidade Federal do Rio de Janeiro' WHERE school = 'Federal University of the State of Rio de Janeiro';
UPDATE young_company.schools SET school = 'University of Westminster' WHERE school = 'Westminster School';
UPDATE young_company.schools SET school = 'High School' WHERE school = 'Higher School of Economics';
UPDATE young_company.schools SET school = 'High School' WHERE school = 'St. George Higher Secondary School (HS)';
UPDATE young_company.schools SET school = 'High School' WHERE school = 'Homestead Highschool';
UPDATE young_company.schools SET school = 'High School' WHERE school = 'M. P. Birla Foundation Higher Secondary School';
UPDATE young_company.schools SET school = 'High School' WHERE school = 'Glory Highschool';
UPDATE young_company.schools SET school = 'V. N. Karazin Kharkiv National University' WHERE school = 'Kharkiv National University';
UPDATE young_company.schools SET school = 'Manchester Metropolitan University' WHERE school = 'Manchester University';
UPDATE young_company.schools SET school = "Queen's University Belfast" WHERE school = "Queen's University";
UPDATE young_company.schools SET school = 'V. N. Karazin Kharkiv National University' WHERE school = 'Kharkiv National University';

UPDATE young_company.schools SET school = 'UCL' WHERE school = 'University College London';
UPDATE young_company.schools SET school = 'Universidad Iberoamericana IBERO' WHERE school = 'Universidad Iberoamericana, Ciudad de México';
UPDATE young_company.schools SET school = 'Indian Institute of Technology Roorkee' WHERE school = 'IIT Roorkee';
UPDATE young_company.schools SET school = 'IE University' WHERE school = 'IE Business School';



-- ============================================================================================================
-- SECTION 2: Data Normalization
-- ============================================================================================================

USE young_company;

-- this procedure is created for the purpose of data normalization. More specifically, to extract table from the redundant tables.
DROP PROCEDURE IF EXISTS AlterColumnNames;
DELIMITER //
CREATE PROCEDURE AlterColumnNames()
BEGIN
    IF EXISTS (SELECT * 
               FROM information_schema.COLUMNS 
               WHERE TABLE_NAME = 'industries' AND COLUMN_NAME = 'id') THEN
        ALTER TABLE industries CHANGE id CompanyId INT; -- change id to CompanyId so that it is more intuitive for all the following
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

CALL AlterColumnNames(); -- normalization - this call will change the underlying data



SET foreign_key_checks = 0; -- because of the alter command below

DROP TABLE IF EXISTS institutions; -- originally, there is no institutions table. it was embedded in the main company table. now, we extract it to make it more clean.
CREATE TABLE institutions (
    institution_id INT AUTO_INCREMENT PRIMARY KEY, -- create a primary key for data consistency 
    institution_name VARCHAR(255)
);

ALTER TABLE institutions
ADD UNIQUE (institution_name);

CREATE SCHEMA IF NOT EXISTS young_company;
USE young_company;

-- ######################################### the next file import
-- the command of loading files can be found in the previous loadings at the beginning of this scripts.
DROP TABLE IF EXISTS qs_rank; -- same as the institution table creation. now we create a qs_rank but this data is external and is imported from QS ranking
CREATE TABLE qs_rank (
	RowNumber INT,
    rank_2024 INT,
    institution VARCHAR (100),
    location VARCHAR (100),
    FOREIGN KEY (institution) REFERENCES institutions(institution_name) ON DELETE CASCADE
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


DROP TABLE IF EXISTS program;
CREATE TABLE program (
    program_id INT AUTO_INCREMENT PRIMARY KEY,
    program_name VARCHAR(255)
);

DROP TABLE IF EXISTS enrollment; -- create a enrollment table to record the program and universities the founders went to.
CREATE TABLE enrollment (
    enrollment_id INT AUTO_INCREMENT PRIMARY KEY,
    institution_id INT,
    program_id INT,
    year INT,
    hnid VARCHAR(15),
    CONSTRAINT fk_institution FOREIGN KEY (institution_id) REFERENCES institutions(institution_id) ON DELETE CASCADE, -- link to another table's primary key as a foreign key
    CONSTRAINT fk_program FOREIGN KEY (program_id) REFERENCES program(program_id) ON DELETE CASCADE,-- link to another table's primary key as a foreign key
    CONSTRAINT fk_hnid FOREIGN KEY (hnid) REFERENCES founders(hnid) ON DELETE CASCADE -- link to another table's primary key as a foreign key
);

SET foreign_key_checks = 1;


INSERT INTO institutions (institution_name) -- data loading into the newly created normalized tables. 
SELECT DISTINCT school
FROM schools;

INSERT INTO program (program_name) -- data loading into the newly created normalized tables. 
SELECT DISTINCT field_of_study
FROM schools;

INSERT INTO enrollment (institution_id, program_id, year, hnid) -- data loading into the newly created normalized tables. 
SELECT i.institution_id, p.program_id, o.year, o.hnid
FROM schools o
JOIN institutions i ON o.school = i.institution_name
JOIN program p ON o.field_of_study = p.program_name;
DROP table if exists schools;


SET foreign_key_checks = 0;

CREATE SCHEMA IF NOT EXISTS young_company;
USE young_company;

DROP TABLE IF EXISTS program_category;
CREATE TABLE program_category ( -- same as the institution table creation. for data normalization
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




SET foreign_key_checks = 0;

DROP TABLE IF EXISTS badge; -- same as the institution table creation. for data normalization
CREATE TABLE badge (
    BadgeId INT AUTO_INCREMENT PRIMARY KEY,  
    Badge VARCHAR(50) UNIQUE  
);
INSERT INTO badge (Badge)
SELECT DISTINCT CompanyBadge
FROM badges;


DROP TABLE IF EXISTS companybadge; -- same as the institution table creation. for data normalization
CREATE TABLE companybadge (
    CompanyBadgeId INT AUTO_INCREMENT PRIMARY KEY,  
    CompanyId INT,
    BadgeId INT,
    CONSTRAINT fk_company FOREIGN KEY (CompanyId) REFERENCES company(CompanyId) ON DELETE CASCADE,
    CONSTRAINT fk_badge FOREIGN KEY (BadgeId) REFERENCES badge(BadgeId) ON DELETE CASCADE
);

INSERT INTO companybadge (CompanyId, BadgeId) -- load the data into the normalized one.
SELECT b.CompanyId, bd.BadgeId 
FROM badges AS b
JOIN badge AS bd ON b.CompanyBadge = bd.Badge; 
SET foreign_key_checks = 1;
DROP TABLE IF EXISTS badges; 
SET foreign_key_checks = 0;

DROP TABLE IF EXISTS industry;  -- same as the institution table creation. for data normalization
CREATE TABLE industry (
    IndustryId INT AUTO_INCREMENT PRIMARY KEY,  
    IndustryName VARCHAR(200) UNIQUE  
);
INSERT INTO industry (IndustryName)
SELECT DISTINCT industry
FROM industries;

DROP TABLE IF EXISTS company_industry;  

CREATE TABLE IF NOT EXISTS company_industry ( -- same as the institution table creation. for data normalization
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

DROP TABLE IF EXISTS tag; -- same as the institution table creation. for data normalization
CREATE TABLE tag (
    tagId INT AUTO_INCREMENT PRIMARY KEY,  
    tagName VARCHAR(200) UNIQUE  
);
INSERT INTO tag (tagName)
SELECT DISTINCT tag
FROM tags;

DROP TABLE IF EXISTS company_tag;

CREATE TABLE company_tag ( -- same as the institution table creation. for data normalization. 
    CompanyId INT,
    tagId INT,
    PRIMARY KEY (CompanyId, tagId),  
    CONSTRAINT fk_company3 FOREIGN KEY (CompanyId) REFERENCES company(CompanyId) ON DELETE CASCADE,
    CONSTRAINT fk_tag FOREIGN KEY (tagId) REFERENCES tag(tagId) ON DELETE CASCADE
);

INSERT INTO company_tag (CompanyId, tagId) -- same as the institution table creation. for data normalization
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
-- SECTION 4: ETL 
-- This section generate analytical layer tables
-- ============================================================================================================
SET SQL_SAFE_UPDATES = 0; -- needed when altering table with foreign key constrains

SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE -- this command was simply a tool to check on the columns and tables of this schema.
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'young_company'
ORDER BY TABLE_NAME, ORDINAL_POSITION;
DELETE FROM program -- delete that NULL row otherwise it will cause errors when mapping.
WHERE program_category IS NULL;
SET SQL_SAFE_UPDATES = 1;

-- --------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- --------------------------------------------------------------------------------------------------------------------------------------------------------------------

SET SESSION group_concat_max_len = 10000; -- this is to make sure the dynamic query coming up is not too long.

SET SQL_SAFE_UPDATES = 0;

-- to clean the potential column names, making sure there is no space or anything no acceptable.
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
DROP TABLE IF EXISTS company_analytics; -- building the structure of the analytical table 

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

SELECT @dynamic_sql; -- this is to create query which transposes the industry row values into columns


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

PREPARE stmt FROM @dynamic_sql; -- operate the dynamic query
EXECUTE stmt;
DEALLOCATE PREPARE stmt;


DROP PROCEDURE IF EXISTS ETL_Company_Analytics; -- start building the ETL procedure 
DELIMITER $$

CREATE PROCEDURE ETL_Company_Analytics()
BEGIN
    TRUNCATE TABLE company_analytics;

    INSERT INTO company_analytics (
        CompanyId, CompanyName, SuccessIndex, Region, Country, FounderPriorCompanies, 
        FirstEnrollmentYear, TotalEnrollmentYears, Industries, CompanyTags, CompanyHighlight, Founders, 
        UniPrograms, ProgramCategory, Institutions, UniHighestRank, CompanyStatus, CompanySlug -- insert the values except for industries and programs at the moment.
    )
    SELECT 
        c.CompanyId,
        LEFT(c.CompanyName, 50),  
        CASE
            WHEN c.status = 'public' THEN 2 -- this part is the successindex creation process based on status and badge
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
        
        -- joinings and connecting to other tables to get the necessary columns.
        
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

DROP PROCEDURE IF EXISTS Update_Industry_Columns; -- this is to change the industry rename columns names to "IDTY_..." and fill in the data that was left empty from the ETL procedure
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
-- to create queries automatically
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



DROP PROCEDURE IF EXISTS Update_Program_Columns; -- this is to change the university programs founders attended rename columns names to "UNI_..." and fill in the data that was left empty from the ETL procedure
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
-- to create queries automatically

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

-- operate the ETL process by executing these 3 procedures.
CALL ETL_Company_Analytics();
CALL Update_Industry_Columns();
CALL Update_Program_Columns();

SET SQL_SAFE_UPDATES = 1;

SELECT * FROM young_company.company_analytics;



DROP TRIGGER IF EXISTS update_company_analytics_after_insert; -- create a sample trigger to update analytical layer when data is inserted to the operational layer

DELIMITER //

DROP TRIGGER IF EXISTS update_company_analytics_after_insert;

DELIMITER //

CREATE TRIGGER update_company_analytics_after_insert
AFTER INSERT ON company
FOR EACH ROW
BEGIN
    IF NOT EXISTS (SELECT 1 FROM company_analytics WHERE CompanyId = NEW.CompanyId) THEN
        INSERT INTO company_analytics ( -- update the analytical table
            CompanyId, 
            CompanyName, 
            CompanyStatus, 
            CompanySlug, 
            CompanyTags, 
            SuccessIndex, 
            Region
        ) VALUES ( -- with the value...
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


-- ============================================================================================================
-- SECTION 5: Procedures
-- this section is to create stored procedures useful for analytical purposes
-- ============================================================================================================

DROP PROCEDURE IF EXISTS GetSuccessfulStartups; -- get successful start ups based on univeristy and industry
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
        SELECT 'Invalid filter type. Use ''University'' or ''Industry''.' AS ErrorMessage; -- testing. error handling
    END IF;
END$$

DELIMITER ;



DROP PROCEDURE IF EXISTS GetUniqueUniversities; -- this is equivalent to the select distinct command, but here we developt it so that it can be directly called. 
DELIMITER $$
-- could be useful when want to use the GetSuccessfulStartups(University, Industry) procedure
CREATE PROCEDURE GetUniqueUniversities()
BEGIN
    SELECT DISTINCT institution_name AS University
    FROM young_company.institutions;
END $$

DELIMITER ;

DROP PROCEDURE IF EXISTS GetUniqueIndustries; -- this is equivalent to the select distinct command, but here we developt it so that it can be directly called. 
DELIMITER $$

CREATE PROCEDURE GetUniqueIndustries()
BEGIN
    SELECT DISTINCT IndustryName AS Industry
    FROM young_company.industry;
END $$

DELIMITER ;


DROP PROCEDURE IF EXISTS RankUniversitiesBySuccessIndex;
DELIMITER $$
-- to see which university tend to produce the most success startups 
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
-- to see which industry tend to produce the most success startups 

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



-- this is equivalent to the select distinct command, but here we developt it so that it can be directly called. 
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

-- this is equivalent to the select distinct command, but here we developt it so that it can be directly called. 
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


DROP PROCEDURE IF EXISTS RankCountriesAndRegions; -- produce the successindex when given a country and/or region
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

-- this is to get the founders university quantitative rankings and startup successindex
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
        young_company.founders f ON ca.CompanySlug = f.company_slug -- company_slug is the foreign value in this case
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

    IF @dynamic_query IS NULL OR @dynamic_query = '' THEN -- testing and error handling because of the previous errors when loading variable into the dynamic query
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


-- thse are sample stored procedure calls 
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



-- ============================================================================================================
-- SECTION 6: Creating Data Marts
-- creating views
-- ============================================================================================================

-- this view need a procedure because of the pivoted columns
-- view_RankProgramCategoryBySuccessIndex
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
    
    IF dynamic_query IS NULL OR dynamic_query = '' THEN -- error handling
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


-- view 2
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
    
    
    
-- view 3
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
    
    

-- view 4 & 5
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


-- ---------------------End-Of-Operation---------------------
 
SELECT * FROM young_company.company_analytics;