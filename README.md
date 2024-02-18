# get_GA4_data

## put JSON for service account credential on 'configs' directory
see https://developers.google.com/analytics/devguides/reporting/data/v1/quickstart-client-libraries?hl=ja

## make .ini file
- GOOGLE_APPLICATION_CREDENTIALS: Path to JSON for service account credential
- PROPERTY_ID: Property ID
- FILTER_FIELD: Filter field, e.g., pagePathPlusQueryString
- FILTER_EXPRESSION = Filter expression for prefix match, e.g., /index

## install library
    pip install pandas
    pip install google-analytics-data

## Execute
    python .\get_GA4_data_main.py -configfile_GA_path "C:\Workspace\python\get_data_GA4\configs\config_GA4.ini" -startdate "2024-01-01" -enddate "2024-01-07" -csv_path "C:\Workspace\python\get_data_GA4\data\"

- configfile_GA_path: Path to .ini file
- startdate/enddate: Dates from and to.
- csv_path: Path to store data as CSV. 