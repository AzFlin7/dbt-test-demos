/* ========= Example Usage of above functions ===========*/

/*
-- trim_whitespaces

with test_data as (
select    
    '  test' as left_whitespace,
    'test   ' as right_whitespace,
    ' test ' as lr_whitespace
)
select 
    {{ trim_whitespaces('left_whitespace') }} as left_whitespace,
    {{ trim_whitespaces('right_whitespace') }} as right_whitespace,
    {{ trim_whitespaces('lr_whitespace') }} as lr_whitespace
from test_data

*/

/*
-- get_business_effective_date with different timezone
-- NOTE: bigquery timestamp function does not take timezone as input, so output does not reflect the timezone
select 
    {{ get_business_effective_date('date', 'EST') }} as date_est ,
    {{ get_business_effective_date('date', 'UTC') }} as date_utc,
    {{ get_business_effective_date('datetime', 'EST') }} as datetime_est,
    {{ get_business_effective_date('datetime', 'UTC') }} as datetime_utc,
    {{ get_business_effective_date('timestamp', 'EST') }} as timestamp_est,
     {{ get_business_effective_date('timestamp', 'UTC') }} as timestamp_utc
*/



/*
-- standardize_datetime
with test_data as (
    select 
        '2008-11-25 15:30:00' as datetime_col_1,
        'Thu Dec 25 07:30:00 2008' as datetime_col_2,
        '8/30/2018 2:23:38 pm' as datetime_col_3,
        'Wednesday, December 19, 2018' as datetime_col_4
)
select 
    {{ standardize_datetime('datetime_col_1') }} as datetime_col_1 ,
    {{ standardize_datetime('datetime_col_2', '%a %b %e %I:%M:%S %Y') }} as datetime_col_2,
    {{ standardize_datetime('datetime_col_3', '%m/%d/%Y %I:%M:%S %p') }} as datetime_col_3,
    {{ standardize_datetime('datetime_col_4', '%A, %B %e, %Y') }} as datetime_col_4
from test_data
*/



/*
-- standardize_timestamps
with test_data as (
    select 
        '2008-11-25 15:30:00 UTC' as timestamp_col_1,
        'Thu Dec 25 07:30:00 2008' as timestamp_col_2,
        '8/30/2018 2:23:38 pm' as timestamp_col_3,
        'Wednesday, December 19, 2018' as timestamp_col_4
)
select 
    {{ standardize_timestamps('timestamp_col_1') }} as timestamp_col_1 ,
    {{ standardize_timestamps('timestamp_col_2', '%a %b %e %I:%M:%S %Y') }} as timestamp_col_2,
    {{ standardize_timestamps('timestamp_col_3', '%m/%d/%Y %I:%M:%S %p') }} as timestamp_col_3,
    {{ standardize_timestamps('timestamp_col_4', '%A, %B %e, %Y') }} as timestamp_col_4
from test_data
*/


-- convert_timezone
with test_data as (
    select 
        TIMESTAMP '2008-11-25 15:30:00 UTC' as col_1
)
select 
    {{ convert_timezone('col_1', 'UTC') }} as col_UTC,
    {{ convert_timezone('col_1', 'EST') }} as col_EST,
    {{ convert_timezone('col_1', 'GMT') }} as col_GMT
from test_data







