import os
import argparse

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    Filter,
    FilterExpression,
    RunReportRequest,
)
import configparser
import pandas as pd

parser = argparse.ArgumentParser(description='This script gets GA4 data and store as CSV. ')
parser.add_argument('-configfile_GA_path', help = "Set path including config file name e.g. C:/Users/hogehoge/config.ini ", required = True)
parser.add_argument('-startdate', help = "Set startdate to insert into database as hyphen dilimiter yyyy-mm-dd e.g. 2021-01-01 ", required = True)
parser.add_argument('-enddate', help = "Set enddate to insert into database as hyphen dilimiter yyyy-mm-dd e.g. 2021-01-07 ", required = True)
parser.add_argument('-csv_path', help = "Set path to save csv path e.g. C:/Users/hogehoge", required = True)
parser.add_argument('-csv_prefix_name', help = "Set prefix csv name as [prefix name]yyyy-mm-dd~yyyy-mm-dd[suffix name]. ", default = "", required = False)
parser.add_argument('-csv_suffix_name', help = "Set suffix csv name as [prefix name]yyyy-mm-dd~yyyy-mm-dd[suffix name]. ", default = "", required = False)
args = parser.parse_args()

DIMENSIONS = [
    "date", 
    "dateHourMinute",
    "pagePathPlusQueryString",
    "sessionSource",
    "deviceCategory", 
    "operatingSystem", 
    "pageTitle"
    ]

METRICS = [
    "totalUsers",
    "sessions",
    "screenPageViews",
    "userEngagementDuration"
]

class get_GA4_data():
    def __init__(self, config_ini_path):
        config_ini = configparser.ConfigParser()
        if not os.path.exists(config_ini_path):
            raise FileNotFoundError(config_ini_path + " is not found. ")
        config_ini.read(config_ini_path, encoding="utf-8")
        

        self.GOOGLE_APPLICATION_CREDENTIALS = config_ini['DEFAULT']['GOOGLE_APPLICATION_CREDENTIALS']
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config_ini['DEFAULT']['GOOGLE_APPLICATION_CREDENTIALS']
        self.PROPERTY_ID = config_ini['DEFAULT']['PROPERTY_ID']
        self.FILTER_FIELD = config_ini['DEFAULT']['FILTER_FIELD']
        self.FILTER_EXPRESSION = config_ini['DEFAULT']['FILTER_EXPRESSION']
        print(f"self.FILTER_FIELD: {self.FILTER_FIELD}")
        print(f"self.FILTER_EXPRESSION: {self.FILTER_EXPRESSION}")


    def sample_run_report(self, startdate = "2023-03-01", enddate = "2023-03-07"):
        """Runs a simple report on a Google Analytics 4 property."""
        # TODO(developer): Uncomment this variable and replace with your
        #  Google Analytics 4 property ID before running the sample.
        # property_id = "YOUR-GA4-PROPERTY-ID"

        # Using a default constructor instructs the client to use the credentials
        # specified in GOOGLE_APPLICATION_CREDENTIALS environment variable.
        client = BetaAnalyticsDataClient()

        request = RunReportRequest(
            property=f"properties/{self.PROPERTY_ID}",
            dimensions=[ Dimension(name = dim) for dim in DIMENSIONS],
            metrics=[ Metric(name = met) for met in METRICS],
            date_ranges=[DateRange(start_date=startdate, end_date=enddate)],
            dimension_filter=FilterExpression(
                filter=Filter(
                    field_name = self.FILTER_FIELD,
                    string_filter = Filter.StringFilter(match_type = "BEGINS_WITH", value = self.FILTER_EXPRESSION),
                )
            ),
        )
        self.response = client.run_report(request)

    def make_dataframe(self):
        output_header = DIMENSIONS + METRICS
        print(output_header)
        
        ### 辞書にしてDataframe化
        output_dic = {}
        for header in output_header:
            output_dic[header] = []

        for row in self.response.rows:
            value_list = []
            value_list += [elem.value for elem in row.dimension_values]
            value_list += [elem.value for elem in row.metric_values]

            for k,v in zip(output_header, value_list): 
                output_dic[k].append(v)
        
        self.df = pd.DataFrame(output_dic)
        
    def write_csv(self, filename):
        self.df.to_csv(filename, index=False)

def main():
    config_GA_path = args.configfile_GA_path
    csv_path = args.csv_path
    csv_prefix_name = args.csv_prefix_name
    csv_suffix_name = args.csv_suffix_name
    startdate = args.startdate
    enddate = args.enddate
    
    con_GA = get_GA4_data(config_GA_path)
   
    con_GA.sample_run_report(startdate=str(startdate), enddate=str(enddate))
    con_GA.make_dataframe()
    filename = csv_path + "/" + csv_prefix_name + str(startdate) + "~" + str(enddate) + csv_suffix_name +".csv"
    con_GA.write_csv(filename)

if __name__ == "__main__":
    main()
