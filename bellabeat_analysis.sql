-- ============================================
-- Bellabeat FitBit Analysis - SQL Queries
-- Author: Daniel Eniola
-- Tool: Google BigQuery
-- Dataset: bellabeat-analysis-489315.bellabeat_data
-- ============================================


-- ============================================
-- PROCESS PHASE: ROW COUNTS
-- ============================================

SELECT COUNT(*) AS number_of_rows
FROM `bellabeat-analysis-489315.bellabeat_data.weightloginfo_merged`;

SELECT COUNT(*) AS number_of_rows
FROM `bellabeat-analysis-489315.bellabeat_data.sleepday_merged`;

SELECT COUNT(*) AS number_of_rows
FROM `bellabeat-analysis-489315.bellabeat_data.hourlysteps_merged`;

SELECT COUNT(*) AS number_of_rows
FROM `bellabeat-analysis-489315.bellabeat_data.dailyactivity_merged`;


-- ============================================
-- PROCESS PHASE: NULL CHECKS
-- ============================================

-- weightloginfo_merged: Fat column only
-- Result: 31 out of 33 rows have NULL Fat values
-- Decision: Fat column excluded from analysis due to insufficient data
SELECT COUNT(*) AS null_values
FROM `bellabeat-analysis-489315.bellabeat_data.weightloginfo_merged`
WHERE Fat IS NULL;

-- sleepday_merged
SELECT COUNT(*) AS null_values
FROM `bellabeat-analysis-489315.bellabeat_data.sleepday_merged`
WHERE Id IS NULL
OR SleepDay IS NULL
OR TotalSleepRecords IS NULL
OR TotalMinutesAsleep IS NULL
OR TotalTimeInBed IS NULL;

-- hourlysteps_merged
SELECT COUNT(*) AS null_values
FROM `bellabeat-analysis-489315.bellabeat_data.hourlysteps_merged`
WHERE Id IS NULL
OR ActivityHour IS NULL
OR StepTotal IS NULL;

-- dailyactivity_merged (all columns)
SELECT COUNT(*) AS null_values
FROM `bellabeat-analysis-489315.bellabeat_data.dailyactivity_merged`
WHERE Id IS NULL
OR ActivityDate IS NULL
OR TotalSteps IS NULL
OR TotalDistance IS NULL
OR TrackerDistance IS NULL
OR LoggedActivitiesDistance IS NULL
OR VeryActiveDistance IS NULL
OR ModeratelyActiveDistance IS NULL
OR LightActiveDistance IS NULL
OR SedentaryActiveDistance IS NULL
OR VeryActiveMinutes IS NULL
OR FairlyActiveMinutes IS NULL
OR LightlyActiveMinutes IS NULL
OR SedentaryMinutes IS NULL
OR Calories IS NULL;


-- ============================================
-- PROCESS PHASE: DUPLICATE CHECKS
-- ============================================

-- weightloginfo_merged
SELECT Id, Date, COUNT(*) AS num_duplicates
FROM `bellabeat-analysis-489315.bellabeat_data.weightloginfo_merged`
GROUP BY Id, Date
HAVING COUNT(*) > 1;

-- sleepday_merged (before cleaning)
SELECT Id, SleepDay, COUNT(*) AS num_duplicates
FROM `bellabeat-analysis-489315.bellabeat_data.sleepday_merged`
GROUP BY Id, SleepDay
HAVING COUNT(*) > 1;

-- sleepday_clean (after cleaning - verify duplicates removed)
SELECT Id, SleepDay, COUNT(*) AS num_duplicates
FROM `bellabeat-analysis-489315.bellabeat_data.sleepday_clean`
GROUP BY Id, SleepDay
HAVING COUNT(*) > 1;

-- hourlysteps_merged
SELECT Id, ActivityHour, COUNT(*) AS num_duplicates
FROM `bellabeat-analysis-489315.bellabeat_data.hourlysteps_merged`
GROUP BY Id, ActivityHour
HAVING COUNT(*) > 1;

-- dailyactivity_merged
SELECT Id, ActivityDate, COUNT(*) AS num_duplicates
FROM `bellabeat-analysis-489315.bellabeat_data.dailyactivity_merged`
GROUP BY Id, ActivityDate
HAVING COUNT(*) > 1;


-- ============================================
-- PROCESS PHASE: REMOVE DUPLICATES
-- ============================================

-- Create clean sleep table with duplicates removed
-- Result: 413 rows reduced to 410 rows (3 duplicates removed)
CREATE TABLE `bellabeat-analysis-489315.bellabeat_data.sleepday_clean` AS
SELECT DISTINCT *
FROM `bellabeat-analysis-489315.bellabeat_data.sleepday_merged`;


-- ============================================
-- PROCESS PHASE: DISTINCT USER COUNTS
-- ============================================

SELECT COUNT(DISTINCT Id) AS unique_id
FROM `bellabeat-analysis-489315.bellabeat_data.weightloginfo_merged`;

SELECT COUNT(DISTINCT Id) AS unique_id
FROM `bellabeat-analysis-489315.bellabeat_data.sleepday_merged`;

SELECT COUNT(DISTINCT Id) AS unique_id
FROM `bellabeat-analysis-489315.bellabeat_data.hourlysteps_merged`;

SELECT COUNT(DISTINCT Id) AS unique_id
FROM `bellabeat-analysis-489315.bellabeat_data.dailyactivity_merged`;


-- ============================================
-- ANALYZE PHASE: EXPLORATORY ANALYSIS
-- ============================================

-- Daily activity averages
-- Note: 'avg_colories' is a typo in original query, should be 'avg_calories'
SELECT
  AVG(TotalSteps) AS avg_total_steps,
  AVG(Calories) AS avg_colories,
  AVG(VeryActiveMinutes) AS avg_active_minutes,
  AVG(SedentaryMinutes) AS avg_sedentary_minutes
FROM `bellabeat-analysis-489315.bellabeat_data.dailyactivity_merged`;

-- Sleep averages
SELECT
  AVG(TotalMinutesAsleep) AS avg_minutes_asleep,
  AVG(TotalTimeInBed) AS avg_time_in_bed
FROM `bellabeat-analysis-489315.bellabeat_data.sleepday_clean`;

-- Hourly steps pattern
SELECT
  REGEXP_EXTRACT(ActivityHour, r'(\d+:\d+:\d+ \w+)') AS hour_of_day,
  AVG(StepTotal) AS avg_steps
FROM `bellabeat-analysis-489315.bellabeat_data.hourlysteps_merged`
GROUP BY hour_of_day
ORDER BY avg_steps DESC;

-- Activity vs Sleep per user (INNER JOIN)
-- Note: Only 12 users matched across both tables
SELECT
  da.Id,
  AVG(da.TotalSteps) AS avg_steps,
  AVG(da.Calories) AS avg_calories,
  AVG(da.SedentaryMinutes) AS avg_sedentary_minutes,
  AVG(sd.TotalMinutesAsleep) AS avg_minutes_asleep,
  AVG(sd.TotalTimeInBed) AS avg_time_in_bed
FROM `bellabeat-analysis-489315.bellabeat_data.dailyactivity_merged` AS da
JOIN `bellabeat-analysis-489315.bellabeat_data.sleepday_clean` AS sd
ON da.Id = sd.Id
AND da.ActivityDate = PARSE_DATE('%m/%d/%Y', SUBSTR(sd.SleepDay, 1, 9))
GROUP BY da.Id;
