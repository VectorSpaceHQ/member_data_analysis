#! /usr/bin/python3

# Program to process a log list. We want to count the number of
# unique entries per day. 
# Unique entries oer week
# 

#import datetime
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import numpy as np
from scipy.stats import linregress

#making simple Lists first then list of lists 
#I also want to test the use of modules like math
#using the decimal type for number precision

def get_database():
        """
        Pull the latest database from the RPi
        /home/pi/repos/RFID-Access/server/rfid.db
        """
        pass


def plot_daily_uniques(df):
        """
        Generate a plot of the number of daily unique visitors versus time.
        """
        df = df.drop(['dow_averages','member','time'], axis=1)
        df = df.groupby('date', as_index=False).max() # aggregate down to one row per day
        yesterday = datetime.today() - timedelta(days=1)
        last_week = datetime.today() - timedelta(weeks=1)
        df_time_filtered = df[(df['date'] > (datetime.today() - timedelta(days=365))) & (df['date'] < last_week)]
        this_year = int(datetime.today().year)
        this_week = int(datetime.today().isocalendar()[1]) - 1
        year = pd.to_numeric(df['year'])
        week_num = pd.to_numeric(df['week_num'])
        df_time_filtered['weeks_ago'] = (this_year * 52 + this_week) - (year * 52 + week_num)
        
        df_last30 = df[(df['date'] > (datetime.today() - timedelta(days=30))) & (df['date'] < yesterday)]
        df_last90 = df[(df['date'] > (datetime.today() - timedelta(days=90))) & (df['date'] < yesterday)]
        df_last180 = df[(df['date'] > (datetime.today() - timedelta(days=180))) & (df['date'] < yesterday)]
        df_last365 = df[(df['date'] > (datetime.today() - timedelta(days=365))) & (df['date'] < yesterday)]
        
        df_time_filtered['weekly_visits'] = 0

        last_dow = 'Sunday'
        weekly_sum = 0
        weekly_sums = []
        for index,row in df_time_filtered.iterrows():
                if row.dow == 'Monday' and last_dow == 'Sunday':
                        weekly_sum = 0
                if last_dow != row.dow:
                        weekly_sum += row.unique_per_day
                        last_dow = row.dow
                weekly_sums.append(weekly_sum)
        df_time_filtered['weekly_visits'] = weekly_sums
        df_time_filtered = df_time_filtered.drop(['dow','unique_per_day'], axis=1)
        df_weekly = df_time_filtered.groupby('weeks_ago').max()

        df_weekly_last4 = df_weekly[(df_weekly['date'] > (datetime.today() - timedelta(weeks=4))) & (df_weekly['date'] < yesterday)]
        df_weekly_last12 = df_weekly[(df_weekly['date'] > (datetime.today() - timedelta(weeks=12))) & (df_weekly['date'] < yesterday)]
        df_weekly_last24 = df_weekly[(df_weekly['date'] > (datetime.today() - timedelta(weeks=24))) & (df_weekly['date'] < yesterday)]
        df_weekly_last52 = df_weekly[(df_weekly['date'] > (datetime.today() - timedelta(weeks=52))) & (df_weekly['date'] < yesterday)]
        
        fig = plt.figure()
        fig, ((ax1, ax2),(ax3,ax4)) = plt.subplots(2, 2)
        fig.set_figheight(8)
        fig.set_figwidth(10)
        
        mean_val = np.full(df_last30.shape[0], df_last30.unique_per_day.mean())
        ax1.plot(df_last30['date'], df_last30['unique_per_day'], '.', label='')
        ax1.plot(df_last30['date'], mean_val, '-', color='red', label='avg '+str(round(df_last30['unique_per_day'].mean(),1)))

        mean_val = np.full(df_last90.shape[0], df_last90.unique_per_day.mean())
        ax2.plot(df_last90['date'], df_last90['unique_per_day'], '.', label='')
        ax2.plot(df_last90['date'], mean_val, '-', color='red', label='avg '+str(round(df_last90['unique_per_day'].mean(),1)))

        mean_val = np.full(df_last180.shape[0], df_last180.unique_per_day.mean())
        ax3.plot(df_last180['date'], df_last180['unique_per_day'], '.', label='')
        ax3.plot(df_last180['date'], mean_val, '-', color='red', label='avg '+str(round(df_last180['unique_per_day'].mean(),1)))
        
        mean_val = np.full(df_last365.shape[0], df_last365.unique_per_day.mean())
        ax4.plot(df_last365['date'], df_last365['unique_per_day'], '.', label='')
        ax4.plot(df_last365['date'], mean_val, '-', color='red', label='avg '+str(round(df_last365['unique_per_day'].mean(),1)))
                
        fig.suptitle('Number of Unique Member Visits per Day', fontsize=16)
        ax1.set_title("Last 30 days")
        ax2.set_title("Last 90 days")
        ax3.set_title("Last 180 days")
        ax4.set_title("Last 365 days")
        ax1.legend(loc='best')
        ax2.legend(loc='best')
        ax3.legend(loc='best')
        ax4.legend(loc='best')
        
        ax1.xaxis.set_ticks([yesterday-timedelta(days=30),yesterday-timedelta(days=20),
                             yesterday-timedelta(days=10), yesterday])
        ax2.xaxis.set_ticks([yesterday-timedelta(days=90),yesterday-timedelta(days=60),
                             yesterday-timedelta(days=30), yesterday])
        ax3.xaxis.set_ticks([yesterday-timedelta(days=180),yesterday-timedelta(days=120),
                             yesterday-timedelta(days=60), yesterday])
        ax4.xaxis.set_ticks([yesterday-timedelta(days=365),yesterday-timedelta(days=240),
                             yesterday-timedelta(days=120), yesterday])
        
        ax1.set_xlabel("")
        ax2.set_xlabel("")
        ax3.set_xlabel("")
        ax4.set_xlabel("")
        fig.subplots_adjust(hspace=.75)
        
        import matplotlib.dates as mdates
        myFmt = mdates.DateFormatter('%m-%d')
        ax1.xaxis.set_major_formatter(myFmt)
        ax2.xaxis.set_major_formatter(myFmt)
        ax3.xaxis.set_major_formatter(myFmt)
        ax4.xaxis.set_major_formatter(myFmt)
        fig.savefig("daily_uniques.png")
        

        # Weekly Data
        fig, ((ax1, ax2),(ax3,ax4)) = plt.subplots(2, 2)
        fig.set_figheight(8)
        fig.set_figwidth(10)
        ax1.set_title("Past 4 weeks")
        ax2.set_title("Past 12 weeks")
        ax3.set_title("Past 24 weeks")
        ax4.set_title("Past 52 weeks")
        
        df_weekly_last4.plot(y='weekly_visits',use_index=True, ax=ax1, label='',legend=False, marker='.', ls='')
        df_weekly_last12.plot(y='weekly_visits',use_index=True, ax=ax2, label='',legend=False, marker='.', ls='')
        df_weekly_last24.plot(y='weekly_visits',use_index=True, ax=ax3, label='',legend=False, marker='.', ls='')
        df_weekly_last52.plot(y='weekly_visits',use_index=True, ax=ax4, label='',legend=False, marker='.', ls='')
        
        max_week = df_time_filtered['week_num'].max(axis=0)
        ax2.xaxis.set_ticks([0, 4, 8, 12])
        ax3.xaxis.set_ticks([0, 5, 10, 15, 20, 25])
        ax4.xaxis.set_ticks([0, 10, 20, 30, 40, 52])
        ax1.invert_xaxis()
        ax2.invert_xaxis()
        ax3.invert_xaxis()
        ax4.invert_xaxis()
        ax1.xaxis.set_major_locator(MaxNLocator(integer=True)) # force integer xaxis

        x = df_weekly_last4.index
        y = df_weekly_last4.weekly_visits
        stats = linregress(x,y)
        m = stats.slope
        b = stats.intercept
        y = m*x+b
        growth = ((m*x.values[0] + b)/ (m*x.values[-1] + b) - 1) * 100
        ax1.plot(x.values, y.values, color="red", label=str(int(growth)) + '%') 
        ax1.legend(loc='best')

        x = df_weekly_last12.index
        y = df_weekly_last12.weekly_visits
        stats = linregress(x,y)
        m = stats.slope
        b = stats.intercept
        y = m*x+b
        growth = ((m*x.values[0] + b)/ (m*x.values[-1] + b) - 1) * 100
        ax2.plot(x.values, y.values, color="red", label=str(int(growth)) + '%') 
        ax2.legend(loc='best')

        x = df_weekly_last24.index
        y = df_weekly_last24.weekly_visits
        stats = linregress(x,y)
        m = stats.slope
        b = stats.intercept
        y = m*x+b
        growth = ((m*x.values[0] + b)/ (m*x.values[-1] + b) - 1) * 100
        ax3.plot(x.values, y.values, color="red", label=str(int(growth)) + '%') 
        ax3.legend(loc='best')
        
        x = df_weekly.index
        y = df_weekly.weekly_visits
        stats = linregress(x,y)
        m = stats.slope
        b = stats.intercept
        y = m*x+b
        growth = ((m*x.values[0] + b)/ (m*x.values[-1] + b) - 1) * 100
        ax4.plot(x.values, y.values, color="red", label=str(int(growth)) + '%') 
        ax4.legend(loc='best')

        this_year = datetime.now().strftime('%Y-%m-%d, week ') + str(datetime.today().isocalendar()[1])
        fig.suptitle('Number of Unique Member Visits per Week\nPlot Generated ' + this_year, fontsize=14)
        fig.subplots_adjust(hspace=.4)
        ax1.set_xlabel("Weeks ago")
        ax2.set_xlabel("Weeks ago")
        ax3.set_xlabel("Weeks ago")
        ax4.set_xlabel("Weeks ago")
        fig.savefig("weekly_visits_"+datetime.now().strftime('%Y-%m-%d')+".png")



        # Monthly for seasonal trends
        this_month = datetime.today().strftime("%B")
        df = df.groupby(['year','month'], as_index=False)['unique_per_day'].sum()
        df = df.rename(columns={'unique_per_day':'unique_per_month'})
        df['date'] = pd.to_datetime(df['year'] + ',' + df['month'])
        df = df[(df['month'] != this_month) | (df['year'] != str(datetime.today().year))] # drop current month
        print(df)
        fig, ax1 = plt.subplots(1, 1)
        fig.set_figheight(8)
        fig.set_figwidth(10)
        ax1.bar(df['date'], df['unique_per_month'], 32)
        myFmt = mdates.DateFormatter('%b-%y')
        ax1.xaxis.set_major_formatter(myFmt)
        fig.suptitle("Number of Unique Member Visits per Month")
        fig.savefig("monthly_visits_"+datetime.now().strftime('%Y-%m-%d')+".png")
        
        
        

def main():
        cnx = sqlite3.connect('rfid.db')
        df = pd.read_sql_query("SELECT * FROM logs", cnx)
        df = df.drop(columns=['_etag','_updated','id','uuid','resource','granted','reason'])
        df['_created']=pd.to_datetime(df['_created'])
        df['date'] = df["_created"].dt.strftime("%Y-%m-%d")
        df['time'] = df["_created"].dt.strftime("%H:%M")
        df['year'] = df["_created"].dt.strftime("%Y")
        df = df.drop(columns=['_created'])
        df = df.replace({'member': {'': np.nan}}).dropna(subset=['member'])
        df = df.dropna(axis=0, how='any', thresh=None, subset=None, inplace=False)

        df['date']=pd.to_datetime(df['date'])
        df['week_num'] = df['date'].apply(lambda x: x.strftime("%U")) # 12/31 counts as week 52
        df['month'] = df["date"].dt.month_name()
        df['dow'] = df["date"].dt.weekday_name
        
        df['unique_per_day'] = df['member'].groupby(df["date"]).transform('nunique')
        df['dow_averages'] = df['unique_per_day'].groupby(df["dow"]).transform('mean')
        df = df.round({'dow_averages': 1})

        print(df)
        

        daily_data = df.groupby(df['date']).mean()

        dow_data = df['dow_averages'].groupby(df['dow']).mean()
        print("Average number of unique entries by day of week:")
        print(dow_data.sort_values(ascending=False))

        #monthly_data = df['monthly_averages'].groupby(df['month_num']).mean()
        #print("Monthly Averages:")
        #print(monthly_data.sort_values(ascending=False))

        plot_daily_uniques(df)

        
if __name__ == '__main__':
        main()

        
