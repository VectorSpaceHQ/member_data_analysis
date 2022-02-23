#! /usr/bin/python3

# Program to process a log list. We want to count the number of
# unique entries per day.
# Unique entries oer week
#

import matplotlib
matplotlib.use('pdf')
import matplotlib.pyplot as plt

import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
from scipy.stats import linregress
import paramiko
import sys
import os
from pathlib import Path
import glob
import datetime as dt


def get_database():
        """
        Pull the latest database from the RPi
        """
        if os.path.isfile('rfid.db'):
                modified_time = datetime.fromtimestamp(os.path.getmtime("rfid.db"))
                print(type(modified_time))
        else:
                modified_time = datetime.now() - timedelta(days=3)
        if (modified_time + timedelta(days=1)) < datetime.now():
                print("database is old, get new one")
                # sshpw = input("Enter SSH PW: ")
                with open("sshpw",'r') as f1:
                        sshpw = f1.readlines()
                ssh_client=paramiko.SSHClient()
                ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh_client.connect(hostname='10.0.0.145',username='pi',password=sshpw)
                ftp_client=ssh_client.open_sftp()
                ftp_client.get('/home/pi/RFID-Access/server/rfid.db', './rfid.db')
                ftp_client.close()
        else:
                print("database is recent enough.")


def remove_daily_duplicates(df):
        """
        Remove entries from same person more than once in the same day.
        """
        names = set()
        last_date = "-99"
        duplicate_rows = []
        for index, row in df.iterrows():
                if row['date'] != last_date:
                        names = set()

                if row['member'] in names:
                        # print(row['member'], row['date'])
                        duplicate_rows.append(index)
                else:
                        names.add(row['member'])
                last_date = row['date']

        df = df.drop(index=duplicate_rows)
        return df


def find_inactive_members(df):
        df_last90 = df[(df['date'] > (datetime.today() - timedelta(days=90))) & (df['date'] < datetime.today())]
        df_last30 = df[(df['date'] > (datetime.today() - timedelta(days=30))) & (df['date'] < datetime.today())]

        df_last30 = df_last30.groupby(df["member"]).mean()
        df_last90 = df_last90.groupby(df["member"]).mean()
        df_last30 = df_last30.drop(['unique_per_day','dow_averages','month','dow'], axis=1)
        df_last90 = df_last90.drop(['unique_per_day','dow_averages','month','dow'], axis=1)
        df_last30.reset_index(level=0, inplace=True)
        df_last90.reset_index(level=0, inplace=True)

        common = pd.merge(df_last30, df_last90, on=['member'], how='inner')
        uncommon = df_last90[(~df_last90.member.isin(common.member))]
        uncommon.reset_index(inplace=True, drop=True)
        print("\nThe following members have keyed into the space in the last 90 days, but not in the last 30 days")
        print(uncommon)

        print(df_last30)
        N_total_members = len(df_last90['member'])
        N_active_members = len(df_last30['member'])
        print("{} Total members (visited in last 90 days)".format(N_total_members))
        print("{} Active members (visited in last 30 days)".format(N_active_members))
        print("{}% of members are currently active".format(int(100 * N_active_members / N_total_members)))

        return uncommon


def plot_active_members(df):
        """
        Plot daily percent of active members
        """
        print("Generating active members plot. This function takes time...")
        skip_value = 1000
        for i in range(0,df.shape[0]-skip_value):
                j = i + skip_value
                df_last90 = df[(df['date'] > (df.iloc[j]['date'] - timedelta(days=90))) & (df['date'] < df.iloc[j]['date'])]
                df_last30 = df[(df['date'] > (df.iloc[j]['date'] - timedelta(days=30))) & (df['date'] < df.iloc[j]['date'])]

                df_last30 = df_last30.groupby(df["member"]).mean()
                df_last90 = df_last90.groupby(df["member"]).mean()
                df_last30 = df_last30.drop(['unique_per_day','dow_averages','month','dow'], axis=1)
                df_last90 = df_last90.drop(['unique_per_day','dow_averages','month','dow'], axis=1)
                df_last30.reset_index(level=0, inplace=True)
                df_last90.reset_index(level=0, inplace=True)

                common = pd.merge(df_last30, df_last90, on=['member'], how='inner')
                uncommon = df_last90[(~df_last90.member.isin(common.member))]
                uncommon.reset_index(inplace=True, drop=True)
                N_total_members = len(df_last90['member'])
                N_active_members = len(df_last30['member'])

                df.loc[df.index[i+skip_value],'percent_active'] = int(100 * N_active_members / N_total_members)

        # fig = plt.figure()
        fig, ax1 = plt.subplots(1, 1)
        fig.set_figheight(11)
        fig.set_figwidth(8)
        ax1.set_title("Percent of Active Members Over Time \nkeys scanned in last 30 days divided by keys scanned in last 90 days")
        ax1.set_xlabel("Date")
        ax1.set_ylabel("Percent (%)")
        ax1.plot(df['date'], df['percent_active'])

        fig.savefig("percent_active.png")



def plot_daily_uniques(df):
        """
        Generate a plot of the number of daily unique visitors versus time.
        """
        df = df.drop(['dow_averages','member','time'], axis=1)
        week_num = pd.to_numeric(df['week_num'])
        df['week_num'] = week_num
        print(df)

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



        last_dow = 6 # monday = 0, sunday = 6
        weekly_sum = 0
        weekly_sums = []
        for index,row in df_time_filtered.iterrows():
                if row.dow == 0 and last_dow == 6:
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

        # fig = plt.figure()
        fig, ((ax1, ax2),(ax3,ax4)) = plt.subplots(2, 2)
        fig.set_figheight(11)
        fig.set_figwidth(8)

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
        fig.subplots_adjust(hspace=.4)

        import matplotlib.dates as mdates
        last30Fmt = mdates.DateFormatter('%b-%d')
        myFmt = mdates.DateFormatter('%b-%y')
        ax1.xaxis.set_major_formatter(last30Fmt)
        ax2.xaxis.set_major_formatter(myFmt)
        ax3.xaxis.set_major_formatter(myFmt)
        ax4.xaxis.set_major_formatter(myFmt)
        fig.savefig("daily_uniques.png")

        # Weekly Data
        fig, ((ax1, ax2),(ax3,ax4)) = plt.subplots(2, 2)
        fig.set_figheight(11)
        fig.set_figwidth(8)
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
        ax1.yaxis.set_major_locator(MaxNLocator(integer=True)) # force integer xaxis
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
        fig.suptitle('Number of Unique (per day) Member Visits per Week\nPlot Generated ' + this_year, fontsize=14)
        fig.subplots_adjust(hspace=.4)
        ax1.set_xlabel("Weeks ago")
        ax2.set_xlabel("Weeks ago")
        ax3.set_xlabel("Weeks ago")
        ax4.set_xlabel("Weeks ago")
        fig.savefig("weekly_visits_"+datetime.now().strftime('%Y-%m-%d')+".png")


        # Monthly for seasonal trends
        this_month = datetime.today().strftime("%-m")
        df = df.groupby(['year','month'], as_index=False)['unique_per_day'].sum()
        df = df.rename(columns={'unique_per_day':'unique_per_month'})
        df['date'] = pd.to_datetime(df['year'].astype(str) + '-' + df['month'].astype(str))

        df = df[(df['month'].astype(str) != this_month) | (df['year'] != str(datetime.today().year))] # drop current month

        fig, ax1 = plt.subplots(1, 1)
        fig.set_figheight(11)
        fig.set_figwidth(8)
        ax1.bar(df['date'], df['unique_per_month'], width=np.timedelta64(20, 'D'))
        plt.grid(b=True, which='major', color='#666666', linestyle='-')
        myFmt = mdates.DateFormatter('%b-%y')
        ax1.xaxis.set_major_formatter(myFmt)
        fig.suptitle("Number of Visits per Month \nunique per day filter")
        fig.savefig("monthly_visits_"+datetime.now().strftime('%Y-%m-%d')+".png")



def generate_pdf(uncommon, dow_data):
        # convert numeric days to words
        dayOfWeek={0:'Monday', 1:'Tuesday', 2:'Wednesday', 3:'Thursday', 4:'Friday', 5:'Saturday', 6:'Sunday'}
        dow_data = pd.DataFrame({'day':dow_data.index, 'avg_visits':dow_data.values})
        dow_data['weekday'] = dow_data['day'].map(dayOfWeek)

        from fpdf import FPDF

        pdf = FPDF()
        pdf.add_page()
        pdf.add_font('DejaVu', '', '/usr/share/fonts/truetype/dejavu/DejaVuSansCondensed.ttf', uni=True)
        pdf.set_font('DejaVu', '', 16)
        pdf.cell(40, 10, 'Vector Space Member Data Report')
        pdf.cell(10, 40, datetime.now().strftime('%m-%d-%Y'))

        pdf.add_page()
        pdf.set_font('DejaVu', '', 10)
        pdf.image('percent_active.png', x = None, y = None, w=180, type = '', link = '')

        pdf.add_page()
        pdf.set_font('DejaVu', '', 10)
        pdf.cell(40, 20, 'The following people have keyed into the space in the last 90 days, but not in the last 30 days.',0,2,"L")
        for i in range(0, len(uncommon)):
                if i%2 == 0:
                        pdf.cell(50, 8, '%s' % (uncommon['member'].loc[i]), 0, 0, 'C')
                else:
                        pdf.cell(50, 8, '%s' % (uncommon['member'].loc[i]), 0, 2, 'C')
                        pdf.cell(-50)


        pdf.add_page()
        pdf.cell(40, 20, 'Average number of unique entries by day of week.',0,2,"L")

        dow_data = dow_data.drop(columns=['day'])
        dow_data = dow_data.set_index('avg_visits')
        data = dow_data.sort_values(by=['avg_visits'], ascending = False).to_string()

        for line in data.splitlines()[1:]:
                pdf.cell(50, 8, '%s' % (line), 0, 2, 'R')

        pdf.add_page()
        today = datetime.now().strftime('%Y-%m-%d')
        pdf.image('weekly_visits_'+today+'.png', x = None, y = None, w=180, type = '', link = '')
        pdf.add_page()
        pdf.image('daily_uniques.png', x = None, y = None, w=180, type = '', link = '')
        pdf.add_page()
        pdf.image('monthly_visits_'+today+'.png', x = None, y = None, w=180, type = '', link = '')
        pdf.add_page()
        pdf.image('unique_monthly_visits_'+today+'.png', x = None, y = None, w=180, type = '', link = '')

        Path("./reports").mkdir(parents=True, exist_ok=True)
        pdf.output('./reports/member-data_'+today+'.pdf', 'F')

        # cleanup plots
        fileList = glob.glob('/vsfs01/home/aspontarelli/Documents/member_data_analysis/*.png')
        for filePath in fileList:
                try:
                        os.remove(filePath)
                except:
                        print("Error while deleting file : ", filePath)


def plot_time_of_day(df_last52):
        print(df_last52)

        bins = [-1,2,4,6,8,10,12,14,16,18,20,22,24] # first bin isn't inclusive??
        # hour = df_last52['time'].str[0:2].astype(int)
        print(pd.cut(df_last52['time'].str[0:2].astype(int), bins))

        hourly_last52 = df_last52.groupby([df_last52['dow'], pd.cut(df_last52['time'].str[0:2].astype(int), bins)])


        xvals = []
        days=["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
        for i in range(7):
                for j in range(12):
                        xvals.append(days[i])
        yvals = []
        for i in range(7):
                for j in range(12):
                        # print(bins[j]+1)
                        yvals.append(bins[j]+1)

        size = hourly_last52.size().unstack().fillna(0).stack()
        # size = new.stack()
        # Use this to review accuracy
        #for value in hourly_last52:
        #        print(value)

        fig, ax = plt.subplots()
        cm = plt.cm.get_cmap('coolwarm')
        sc = ax.scatter(xvals, yvals, c=size, s=size*4, cmap=cm)
        ax.set_xlabel("Day of Week")
        ax.set_ylabel("Hour of Day")
        ax.set_title("Hourly Visits, Last 52 Weeks")

        fig.tight_layout()
        plt.colorbar(sc)

        fig.savefig("time_of_day_"+datetime.now().strftime('%Y-%m-%d')+".png")


def plot_monthly_uniques(df):
        """
        Generate plot of unique monthly visits. If I visit 10 times in a month, counts as 1 visit.
        """
        s = df.groupby(['year',"month"])['member'].nunique()
        df2 = s.to_frame()
        df3 = df2.reset_index( level = [0 , 1] )
        df3['date'] = pd.to_datetime(df3.year.astype(str) + '/' + df3.month.astype(str) + '/01')

        fig, ax1 = plt.subplots(1, 1)
        fig.set_figheight(11)
        fig.set_figwidth(8)
        ax1.bar(df3['date'], df3['member'], width=np.timedelta64(20, 'D'))
        ax1.set_ylabel("Members")
        plt.grid(b=True, which='major', color='#666666', linestyle='-')
        fig.suptitle("Number of Unique Visits per Month \nunique per month filter")
        fig.savefig("unique_monthly_visits_"+datetime.now().strftime('%Y-%m-%d')+".png")


def prep_and_clean_data(df):
        return df


def main():
        get_database()

        cnx = sqlite3.connect('rfid.db')
        df = pd.read_sql_query("SELECT * FROM logs", cnx)

        df = df.drop(columns=['_etag','_updated','id','uuid','resource','granted','reason'])
        df['_created']=pd.to_datetime(df['_created'])
        df['_created'] = df['_created'] + pd.Timedelta(hours=-4) # log times do not match local system time, even in the .db file
        df['date'] = df["_created"].dt.strftime("%Y-%m-%d")
        df['time'] = df["_created"].dt.strftime("%H:%M")
        df['year'] = df["_created"].dt.strftime("%Y")

        df = df.drop(columns=['_created','uuid_bin'])

        df = df.replace({'member': {'': np.nan}}).dropna(subset=['member'])

        df = df.dropna(axis=0, how='any', thresh=None, subset=['member'], inplace=False)

        df['date']=pd.to_datetime(df['date'])
        df['week_num'] = df['date'].apply(lambda x: x.strftime("%U")) # 12/31 counts as week 52
        df['month'] = df["date"].dt.month # month_name()
        df['dow'] = df["date"].dt.weekday

        df['unique_per_day'] = df['member'].groupby(df["date"]).transform('nunique')
        df['dow_averages'] = df['unique_per_day'].groupby(df["dow"]).transform('mean')
        df = df.round({'dow_averages': 1})

        df = remove_daily_duplicates(df)

        yesterday = datetime.today() - timedelta(days=1)
        df_last52 = df[(df['date'] > (datetime.today() - timedelta(weeks=52)))& (df['date'] < yesterday)]
        df_last52.drop(columns=['dow_averages'])
        df_last52['dow_averages'] = df_last52['unique_per_day'].groupby(df_last52["dow"]).transform('mean')
        df_last52 = df_last52.round({'dow_averages': 1})
        dow_data_last52 = df_last52['dow_averages'].groupby(df_last52['dow']).mean()
        print("Average number of unique entries by day of week:")
        print(dow_data_last52.sort_values(ascending=False))

        plot_time_of_day(df_last52)
        plot_daily_uniques(df)
        plot_monthly_uniques(df)

        uncommon = find_inactive_members(df)
        plot_active_members(df)

        generate_pdf(uncommon, dow_data_last52)
        print("FINISHED")


if __name__ == '__main__':
        main()
