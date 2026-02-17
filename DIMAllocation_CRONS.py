"""
    Module to allocate DIM incidents to DIM analysts.


"""
import os
import sys

# Added by Suyanana - Force DEV environment for cron testing
os.environ["ENV"] = "dev"  # FORCE DEV ENVIRONMENT
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "django_project.settings")

import django
from requests.models import HTTPError
sys.path.append("/apps/opt/django_project/")
django.setup()

from constants.apiConstants import GIS_TOOLKIT_BASE_URL, HEADERS
from utilities.oauth_authentication import generate_bearer_token
from django.conf import settings
from dim.DIMMatterMostWebHook import post_to_mattermost
import datetime
import math
import requests
import time
import pytz
import pandas as pd
import random

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)

import more_itertools as mit
from django.template.loader import render_to_string
from functools import lru_cache
from time import sleep
import collections
from collections import deque
from itertools import cycle
from EmailExchange import EmailExchange
from configReader import ConfigSectionMap
from logger import logFactory
from SharePointList import SharePointList
from vontu.VontuRESTPy import VontuRESTPy
import json
from vontu.models import VontuDLP
from constants.global_constants import RECIPIENTS

log = logFactory("dim")
# log.setLevel("DEBUG")


class DIMAllocation:
    # DIM Handles everything in ET, all times are translated to ET
    def __init__(self):
        self.date = datetime.datetime.strftime(datetime.datetime.today(), "%Y-%m-%d")
        self.username = ConfigSectionMap("account")["user"].decode("utf-8")
        self.password = ConfigSectionMap("account")["pass"].decode("utf-8")
        self.toolkit_token = ConfigSectionMap("account")["token"].decode("utf-8")
        self.toolkit_url = f"{GIS_TOOLKIT_BASE_URL}/v1"
        self.vontu_host = ConfigSectionMap("vontu")["enforce-prod"].decode("utf-8")

        self.vontu_client = self.get_vontu_client()
        self.sharepoint_site = "https://sharepoint3.bankofamerica.com/sites/Braswell/"
        self.ooo_calendar = {"list_name": "DIM Analysis Calendar", "view_name": "Calendar View"}
        self.respbox_calendar = {"list_name": "DIM Response Box Rotation", "view_name": "Calendar"}
        self.opscall_calendar = {"list_name": "Ops Call Calendar", "view_name": "Calendar"}
        self.filepath = "/apps/opt/django_project/reports/"
        self.filename = "dim_allocation_{}-{}.csv"
        if settings.ENV in ['dev', 'uat']:
            self.recipients = RECIPIENTS
            self.vontu_report = "4057"
            self.channel = "qc-test"
            self.username = "testing_dim_allocation"
        else:
            self.recipients = [
                "dg.gis_info_protect_dim@bofa.com"
            ]
            self.vontu_report = "131947"
            self.channel = "dim-health-metrics"
            self.username = "dim_health_metrics"

        self.icon_url = "https://www.mattermost.org/wp-content/uploads/2016/04/icon.png"

        # Flatten values used in case there are any nested lists in respbox/training schedule
        # https://stackoverflow.com/questions/2158395/flatten-an-irregular-list-of-lists
        # Is there a better way to store this?
        self.flatten = lambda *args: (
            result
            for mid in args
            for result in (self.flatten(*mid) if isinstance(mid, (tuple, list)) else (mid,))
        )

    def get_vontu_client(self):
        """Get client for interacting with vontu API.

        Returns:
            VontuRESTPy.VontuRESTPy : Client for interacting w/ Vontu API.
        """
        # As part of GISCHAI-2147: we are getting rest connection
        vontu = VontuRESTPy("DIME")
        vontu.body.append({"name": "detectionDate"})
        return vontu

    def get_eastern_offset(self):
        """Gets utc to eastern offset.

        DIM standardizes everything in eastern. Check for DST and return offset.

        Returns:
            int: Representing hour difference from UTC to Eastern
        """
        dt = datetime.datetime.now()
        tz = pytz.timezone("US/Eastern")
        aware_dt = tz.localize(dt)
        if aware_dt.dst() != datetime.timedelta(0, 0):
            eastern_offset = 4
        else:
            eastern_offset = 5
        log.debug("Retrieving current UTC -> ETC offset", extras={"offset": eastern_offset})
        return eastern_offset

    def get_analysts(self):
        """Get DIM analyst data from Sharepoint.

        Returns:
            pd.DataFrame : Containing all DIM analysts names, emails, location,
            utilization rate, timezone, and to_utc conversion
        """
        log.info("Retrieving base analyst list.")
        list_name = "DIM Analysts Daily"
        view_name = "All Items"
        sp = SharePointList(self.sharepoint_site, list_name, view_name)
        analysts = sp.get_list_items()
        analysts = pd.DataFrame(analysts)

        ### Added by Suyana 02/13/2026 - Extended hours support with missing column handling
        # EXISTING TIMES: 9am, 10am, 12pm, 2pm, 4pm, 5:30pm, 6:30pm EST (all preserved)
        # NEW TIMES ADDED: 8pm, 9pm, 10:30pm, 12am, 1am, 2am, 3am, 4am, 5am, 6am, 7am, 8am EST
        # Map allocation times to SharePoint column names
        hour = self.allocation_time.hour
        minute = self.allocation_time.minute
        
        # Special handling for half-hour times (10:30pm, 5:30pm, 6:30pm)
        if minute == 30:
            if hour == 22:  # 10:30pm
                column_name = "10:30PM ET"
            elif hour == 17:  # 5:30pm
                column_name = "5:30PM ET"
            elif hour == 18:  # 6:30pm
                column_name = "6:30PM ET"
            else:
                # For other half-hour times, try standard format
                if hour == 0:
                    column_name = "12:30AM ET"
                elif hour < 12:
                    column_name = f"{hour}:30AM ET"
                elif hour == 12:
                    column_name = "12:30PM ET"
                else:
                    column_name = f"{hour-12}:30PM ET"
        else:
            # Handle regular hour times including new overnight hours
            if hour == 0:  # 12am midnight
                column_name = "12AM ET"
            elif hour < 12:  # 1am-11am
                column_name = f"{hour}AM ET"
            elif hour == 12:  # 12pm noon
                column_name = "12PM ET"
            else:  # 1pm-11pm
                column_name = f"{hour-12}PM ET"
        
        ### Added by Suyana 02/13/2026 - Check if column exists, use default if missing
        if column_name in analysts.columns:
            real_allocation_percentage = analysts[column_name]
            print(f"[CRON INFO] Using column '{column_name}' for allocation at {self.allocation_time}")
        else:
            # Default allocation percentage when column is missing
            default_percentage = 50
            print(f"[CRON WARNING] Column '{column_name}' not found in SharePoint, using default {default_percentage}%")
            print(f"[CRON INFO] Available columns: {', '.join([col for col in analysts.columns if 'ET' in col])}")
            real_allocation_percentage = default_percentage

        log.debug("Retrieved base analyst list.", extras={"analysts": analysts["Analyst"]})

        # Sharepoint returns Allocation as a string, need to convert to float
        analysts["Allocation Percentage"] = pd.to_numeric(real_allocation_percentage)

        # Add timezone info
        log.debug("Adding timezone info.")
        analysts[["timezone", "to_utc"]] = analysts.apply(
            lambda row: pd.Series(self._get_analyst_timezone(row)), axis=1
        )
        # Expand Special Schedule
        log.debug("Parsing Schedules.")

        analysts["Scheduled"] = analysts.apply(lambda row: self._parse_schedule(row), axis=1)

        return analysts

    def _get_analyst_timezone(self, row):
        """Get timezone and utc conversion for analyst from GISToolkit.

        Args:
            row (pd.DataFrame.Series): must contain 'Email' column w/ analyst email.

        Returns:
            Tuple : Of timezone and utc conversion.
        """
        email = row["Email"]
        headers = HEADERS(generate_bearer_token())
        params = {"returnType": "JSON"}

        endpoint = "{}/associates/?workEmail={}".format(self.toolkit_url, email)
        try:  # TODO: Add backoff
            log.debug("Querying GISToolkit.", extras={"endpoint": endpoint})
            response = requests.get(endpoint, verify=False, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()["data"][0]
            log.debug("Successfully queried GISToolkit.", extras={"data": data})
            timezone = data["displayTimeZoneLong"].split()[0]
            to_utc = data["timeZone"]
            return (timezone, to_utc)
        except KeyError as e:
            log.error(
                "Error retrieving data from GISToolkit.", extras={"analyst": email, "error": e}
            )
            text = f"Error retrieving data from GISToolkit.Analyst: {email}, error: {e}"
            post_to_mattermost(self.channel, self.username, self.icon_url, text)
        except HTTPError as e:
            log.error(
                "Error retrieving data from GISToolkit.", extras={"analyst": email, "error": e}
            )
            text = f"Error retrieving data from GISToolkit.Analyst: {email}, error: {e}"
            post_to_mattermost(self.channel, self.username, self.icon_url, text)

    def _parse_schedule(self, row):
        """Checks if analyst is regularly scheduled for the current day.

        Args:
            row (pd.DataFrame.Series) : Containing 'Special Schedule' in XXX-XXX format
                ie. Mon-Fri

        Returns:
            bool: True if analyst is scheduled, False otherwise.
        """
        log.debug("Processing special schedules.")
        # SharePointList.py doesnt return column if all values null
        # Prevent this from happening when no one is on a special schedule.
        if "Special Schedule" not in row:
            row["Special Schedule"] = None
        # Check for NAN values and set to Mon-Fri if found
        if row["Special Schedule"] != row["Special Schedule"] or row["Special Schedule"] is None:
            row["Special Schedule"] = "Mon-Fri"

        start_day = row["Special Schedule"].split("-")[0]
        start_day = time.strptime(start_day, "%a").tm_wday
        end_day = row["Special Schedule"].split("-")[1]
        end_day = time.strptime(end_day, "%a").tm_wday

        # Get a list of available days in int format
        available_days = []
        day = start_day
        while day <= 6:
            available_days.append(day)
            day += 1
            if day == end_day:
                available_days.append(day)
                break
        if start_day > end_day:
            day = 0
            while day <= end_day:
                available_days.append(day)
                day += 1
        weekday = datetime.date.today().weekday()
        log.debug(
            "Parsed schedule.",
            extras={"analyst": row["Analyst"], "Scheduled": available_days, "today": weekday}
        )
        if weekday in available_days:
            return True
        else:
            return False

    def get_available_timezones(self):
        """Get timezones that are available for this allocation.

        Start/End times are in Eastern.

        Args:
            allocation_time (datetime.time): Time for current allocation run.

        Returns:
            list: of timezone names availble for allocation.
        """
        available = []
        ### Added by Suyana 02/13/2026 - Extended hours for 24/7 support (8pm-8am added)
        # Updated timezone windows to support APAC/EMEA regions
        timezones = [
            {"name": "Greenwich", "start": 3, "end": 23},  # Extended for overnight support
            {"name": "Eastern", "start": 6, "end": 23},    # Extended to 11pm (was 5pm)
            {"name": "Central", "start": 7, "end": 23},    # Extended for evening hours
            {"name": "Mountain", "start": 8, "end": 23},   # Extended for evening hours
            {"name": "Pacific", "start": 9, "end": 23},    # Extended for evening hours
            {"name": "APAC", "start": 20, "end": 8},       # 8pm-8am EST for APAC support
            {"name": "EMEA", "start": 0, "end": 9},        # 12am-9am EST for EMEA support
        ]
        
        ### Added by Suyana 02/13/2026 - Handle overnight time ranges
        for timezone in timezones:
            start_time = datetime.time(timezone["start"])
            end_time = datetime.time(timezone["end"])
            
            # Handle overnight ranges (e.g., 8pm to 8am)
            if timezone["start"] > timezone["end"]:
                # Time range crosses midnight
                if self.allocation_time >= start_time or self.allocation_time < end_time:
                    available.append(timezone["name"])
            else:
                # Normal time range within same day
                if self.allocation_time >= start_time and self.allocation_time < end_time:
                    available.append(timezone["name"])
                    
        log.debug(
            "Retreived available timezones for allocation",
            extras={"timezones": available, "allocation_time": self.allocation_time},
        )
        return available

    @lru_cache(maxsize=10)
    def get_calendar_data(self, list_name, view_name):
        """Get events from specified sharepoint calendar for current day.

        Events are used to determine who is available for allocation.

        Args:
            list_name (str): Name of sharepoint list to retrieve.
            view_name (str): Name of view to retrieve for sharepoint list provided

        Returns:
            pd.DataFrame : Containing calendar events from specified calender for today.
        """
        query = "<Query><Where><DateRangesOverlap><FieldRef Name='EventDate' />\
            <FieldRef Name='EndDate' /><FieldRef Name='RecurrenceID' /><Value Type='DateTime'>\
            <Today /></Value></DateRangesOverlap></Where>\
            <OrderBy><FieldRef Name='EventDate' /></OrderBy></Query>"
        query_options = "<QueryOptions><CalendarDate>{}</CalendarDate>\
            <RecurrencePatternXMLVersion>v3</RecurrencePatternXMLVersion>\
            <ExpandRecurrence>TRUE</ExpandRecurrence>\
            <DateInUtc>TRUE</DateInUtc></QueryOptions>".format(
            self.date
        )
        sp = SharePointList(self.sharepoint_site, list_name, view_name)
        data = sp.get_list_items(
            query=query,
            query_options=query_options,
        )
        data = pd.DataFrame(data)

        return data

    def get_analysts_opscall(self):
        """Parse sharepoint calendar data for analyst who is on Ops call currently.

        Sets self.opscall_raw for use in report later
        Returns:
            None
        """
        self.opscall_raw = self.get_calendar_data(**self.opscall_calendar)
        return None

    def get_analysts_ooo(self):
        """Parse sharepoint calendar data for analysts who are out of office.

        Also sets self.ooo_raw attribute for use in report later.
        Returns:
            list: Of analysts emails.
        """
        # Sharepoint stores all day events timestamps differently than non-all day events
        # Need remove items in the future sharepoint returns when querying for 'Today'
        # Add timestamp info otherwise comparison will drop items with start date
        # equal to today.
        ooo = []
        comparison_date = self.date + "T23:59:59Z"
        ooo_raw = self.get_calendar_data(**self.ooo_calendar)
        ooo_raw = ooo_raw[ooo_raw["Start Time"] <= comparison_date]
        self.ooo_raw = ooo_raw
        for index, row in ooo_raw.iterrows():
            start_time = pd.to_datetime(row["Start Time"]).to_pydatetime()
            end_time = pd.to_datetime(row["End Time"]).to_pydatetime()
            if start_time.time() == datetime.time(0, 0) and end_time.time() == datetime.time(
                23, 59
            ):
                ooo.append(row["Analyst"])
            else:
                start_time = (start_time - datetime.timedelta(hours=self.eastern_offset)).time()
                end_time = (end_time - datetime.timedelta(hours=self.eastern_offset)).time()
                if self.allocation_time >= start_time and self.allocation_time <= end_time:
                    ooo.append(row["Analyst"])
        return ooo

    def get_analysts_training(self):
        """Parse sharepoint calendar data for analysts who are training.

        Also sets self.training_raw attribute for use in report later.
        Returns:
            list: Of analysts emails.
        """
        training = []
        exempt_raw = self.get_calendar_data(**self.respbox_calendar)
        analysts_training = exempt_raw[exempt_raw["Title"].str.contains("(?i)training")]
        self.training_raw = analysts_training
        for index, row in analysts_training.iterrows():
            start_time = pd.to_datetime(row["Start Time"]).to_pydatetime()
            start_time = (start_time - datetime.timedelta(hours=self.eastern_offset)).time()
            end_time = pd.to_datetime(row["End Time"]).to_pydatetime()
            end_time = (end_time - datetime.timedelta(hours=self.eastern_offset)).time()
            if self.allocation_time >= start_time and self.allocation_time <= end_time:
                training.append(row["Analyst(s)"])

        analysts_training = list(self.flatten(training))
        log.debug("Retreived analysts currently training.", extras={"analysts": analysts_training})
        return analysts_training

    def get_analysts_respbox(self):
        """Parse sharepoint calendar data for analysts on response box duty.

        Also sets self.respbox_raw attribute for use in report later.
        Returns:
            list: Of analysts emails.
        """
        respbox = []
        exempt_raw = self.get_calendar_data(**self.respbox_calendar)
        analysts_respbox = exempt_raw[exempt_raw["Title"].str.contains("(?i)Response Box")]
        self.respbox_raw = analysts_respbox
        for index, row in analysts_respbox.iterrows():
            start_time = pd.to_datetime(row["Start Time"]).to_pydatetime()
            start_time = (start_time - datetime.timedelta(hours=self.eastern_offset)).time()
            end_time = pd.to_datetime(row["End Time"]).to_pydatetime()
            end_time = (end_time - datetime.timedelta(hours=self.eastern_offset)).time()
            if self.allocation_time >= start_time and self.allocation_time <= end_time:
                analysts = row["Analyst(s)"]
                respbox.append(analysts)
        log.debug("Retreived analysts on response box duty.", extras={"analysts": analysts_respbox})
        respbox = list(self.flatten(respbox))
        return respbox

    def assign_incidents(self):
        """Assigns incidents to analysts based on utilization.

        Loops through list of analysts and if the
        ((loop count * analysts utilization rate) - the current amount of incs assigned to that analyst
        is greater than 1 it assigns an incident to the analyst. All analysts are assigned an inc
        on the first loop to ensure analysts w/ low utilization still get something assigned.

        Returns:
            None : Assigns directly to the objects analysts attribute
        """
        to_dataframe = pd.DataFrame(self.inc_details)
        to_assign = to_dataframe['incidentId'].tolist()
        log.info("Assigning Incidents.", extras={"total_incs": len(to_assign)})
        count = 0
        self.analysts["Assigned"] = [[] for _ in range(len(self.analysts))]
        log.debug("Starting Assignment loop.")

        while len(to_assign) > 0:
            count += 1
            log.debug("Running loop.", extras={"round": count})
            for index, row in self.analysts.iterrows():
                rate = row["Allocation Percentage"]
                # This is due to sharepoint not being consistent with whole number vs percentage
                if rate > 1:
                    rate = rate / 100
                else:
                    rate

                # On first loop assign an incident to everyone regardless of threshold
                if count == 1 and rate > 0:
                    if len(to_assign) >= 1:
                        inc_id = to_assign.pop()
                        self.analysts.at[index, "Assigned"].append(inc_id)
                        log.debug(
                            "Assigned incident.",
                            extras={
                                "analyst": row["Analyst"],
                                "Assigned": inc_id,
                                "utilization": row["Allocation Percentage"],
                            },
                        )
                    else:
                        log.debug("No more incidents to assign. Breaking loop.")
                        break

                else:
                    # On subsequent runs we only assign an incident if the threshold is at least 1
                    # threshold is rate of utilization times the loop count minus how many the analyst
                    # has assigned.
                    threshold = (rate * count) - len(row["Assigned"])
                    log.debug(
                        "Checking threshold.",
                        extras={"analysts": row["Analyst"], "threshold": threshold},
                    )
                    if threshold >= 1:
                        if len(to_assign) >= 1:
                            inc_id = to_assign.pop()
                            self.analysts.at[index, "Assigned"].append(inc_id)
                            log.debug(
                                "Assigned incident.",
                                extras={
                                    "analyst": row["Analyst"],
                                    "Assigned": inc_id,
                                    "utilization": row["Allocation Percentage"],
                                },
                            )
                        else:
                            log.debug("No more incidents to assign. Breaking loop.")
                            break
                    else:
                        log.debug(
                            "Threshold not met, not assigning incident",
                            extras={"analyst": row["Analyst"], "round": count},
                        )

    def create_report(self):
        """Builds report of analyst/assigned incidents to be emailed to DIM.

        Returns:
            None: Creates a report at self.filepath/self.filename
        """
        data = self.analysts[["Email", "Assigned"]]
        df = pd.DataFrame(columns=["Id", "Policy", "Date/Time", "Sender", "Analyst"])

        for index, row in data.iterrows():
            df = df.append(
                pd.DataFrame({"Id": row["Assigned"], "Analyst": row["Email"]}),
                ignore_index=True,
                sort=False,
            )

        #Get count of how many incidents per person
        piv_table = df.pivot_table(index="Analyst", aggfunc="size")
        analyst_counts = piv_table.to_dict()

        def shuffle_analysts(capacity_dict):
            analyst_keys = list(capacity_dict.keys())
            random.shuffle(analyst_keys)
            return {analyst: capacity_dict[analyst] for analyst in analyst_keys}

        shuffled_analysts = shuffle_analysts(analyst_counts)

        #Create Report structure
        for index, row in df.iterrows():
            inc_id = row["Id"]
            inc_details = [inc for inc in self.inc_details if inc["incidentId"] == inc_id]
            if inc_details:
                inc_details = inc_details[0]
                df.at[index, "Policy"] = inc_details["policyName"]
                df.at[index, "Date/Time"] = inc_details["detectionDateTime"]
                df.at[index, "Sender"] = inc_details["networkSenderIdentifier"]

        df['Analyst'] = None
        sender_inc_count = df.groupby('Sender').size().reset_index(name='count')
        avg_incidents_per_sender = sender_inc_count['count'].mean()

        high_senders = sender_inc_count[sender_inc_count['count'] > avg_incidents_per_sender]['Sender'].tolist()
        low_senders = sender_inc_count[sender_inc_count['count'] <= avg_incidents_per_sender]['Sender'].tolist()

        def assign_senders_to_analysts(senders_list, capacity_dict):
            senders_list = sorted(senders_list, key=lambda sender: df[df['Sender'] == sender].shape[0], reverse=True)
            unassigned_senders = []
            analyst_cycle = cycle(capacity_dict.keys())

            for sender in senders_list:
                sender_incidents = df[df['Sender'] == sender]
                num_incidents = sender_incidents.shape[0]
                assigned_count = 0

                while assigned_count < num_incidents:
                    analyst = next(analyst_cycle)
                    total_incidents_assigned = df[df['Analyst'] == analyst].shape[0]
                    remaining_capacity = capacity_dict[analyst] - total_incidents_assigned

                    if remaining_capacity > 0:
                        incidents_to_assign = min(remaining_capacity, num_incidents - assigned_count)
                        df.loc[sender_incidents.index[assigned_count:assigned_count + incidents_to_assign], 'Analyst'] = analyst
                        assigned_count += incidents_to_assign

                if assigned_count < num_incidents:
                    unassigned_senders.append(sender)

            return unassigned_senders

        unassigned_high = assign_senders_to_analysts(high_senders, shuffled_analysts)
        unassigned_low = assign_senders_to_analysts(low_senders, shuffled_analysts)

        def assign_remaining_senders(unassigned_senders, capacity_dict):
            analyst_cycle = cycle(capacity_dict.keys())
            for sender in unassigned_senders:
                sender_incidents = df[df['Sender'] == sender].shape[0]

                for _ in range(len(capacity_dict)):
                    analyst = next(analyst_cycle)
                    total_incidents_assigned = df[df['Analyst'] == analyst].shape[0]

                    if total_incidents_assigned + sender_incidents <= capacity_dict[analyst]:
                        df.loc[df['Sender'] == sender, 'Analyst'] = analyst
                        break

        if unassigned_high:
            assign_remaining_senders(unassigned_high, shuffled_analysts)
        if unassigned_low:
            assign_remaining_senders(unassigned_low, shuffled_analysts)

        df = df.sort_values(by=['Analyst', 'Sender', 'Date/Time']).reset_index(drop=True)
        df.to_csv(self.filepath + self.filename.format(self.date, self.allocation_time))

    def build_email_body(self):
        """Creates and formats HTML string for email body.
        Uses django templating to build body.

        Returns:
            str : containing HTML formatted data.
        """

        def _convert_time(row):
            """Convert datetime to Eastern and to 12 hour timestamp.

            Args:
                row (pd.Series): Containing 'start' and 'end' times

            Returns:
                start (str): Beginning of calendar event time
                end (str): End of calendar event time
            """
            start = row["start"]
            end = row["end"]
            if start.split("T")[1] == "00:00:00Z" and end.split("T")[1] == "23:59:00Z":
                start = "All"
                end = "Day"
                return start, end

            start = datetime.datetime.strptime(start, "%Y-%m-%dT%H:%M:%SZ")
            start -= datetime.timedelta(hours=self.eastern_offset)
            start = start.strftime("%I:%M %p")

            end = datetime.datetime.strptime(end, "%Y-%m-%dT%H:%M:%SZ")
            end -= datetime.timedelta(hours=self.eastern_offset)
            end = end.strftime("%I:%M %p")

            return start, end

        try:
            amount_assigned = self.analysts['Assigned'].str.len().sort_values(ascending=False)
        except:
            amount_assigned = 0

        training_schedule = self.training_raw[["Analyst(s)", "Start Time", "End Time"]].copy()
        if len(training_schedule) >= 1:
            training_schedule.columns = ["analyst", "start", "end"]
            for index, row in training_schedule.iterrows():
                start, end = _convert_time(row)
                training_schedule.at[index, "start"] = start
                training_schedule.at[index, "end"] = end
            training_schedule = json.loads(training_schedule.to_json(orient="records"))

        ooo_schedule = self.ooo_raw[["Analyst", "Start Time", "End Time"]].copy()
        if len(ooo_schedule) >= 1:
            ooo_schedule.columns = ["analyst", "start", "end"]
            for index, row in ooo_schedule.iterrows():
                start, end = _convert_time(row)
                ooo_schedule.at[index, "start"] = start
                ooo_schedule.at[index, "end"] = end
            ooo_schedule = json.loads(ooo_schedule.to_json(orient="records"))

        respbox_schedule = self.respbox_raw[["Analyst(s)", "Start Time", "End Time"]].copy()
        if len(respbox_schedule) >= 1:
            respbox_schedule.columns = ["analyst", "start", "end"]
            for index, row in respbox_schedule.iterrows():
                start, end = _convert_time(row)
                respbox_schedule.at[index, "start"] = start
                respbox_schedule.at[index, "end"] = end
            respbox_schedule = json.loads(respbox_schedule.to_json(orient="records"))

        self.retry(self.get_analysts_opscall)
        opscall_schedule = (
            self.opscall_raw[["Title"]].copy()
            if self.opscall_raw is not None and "Title" in self.opscall_raw
            else pd.DataFrame([{"Title": "No ops call assigned for now."}])
        )

        # GISCHAI-2147 -removed_incidents was used for SOAP issues(list of incidents for
        # hence we are assigning empty to this variable
        incidents_error_getting_details = []

        body_attributes = {
            "amount_assigned": amount_assigned,
            "training_schedule": training_schedule,
            "ooo_schedule": ooo_schedule,
            "respbox_schedule": respbox_schedule,
            "opscall_schedule": opscall_schedule,
            "errored_incidents": incidents_error_getting_details
        }

        body = render_to_string("allocation_email.html", {**body_attributes})
        return body

    def send_email(self, body):
        """Email assignment report to DIM analysts.

        Emails report to recipients and subsequently deletes it from disk.

        Args:
            body (string): HTML to send in message body.
        Returns:
            None: Emails report from self.filepath/self.filename to self.recipients
        """
        #filename = self.filename.format(self.date, self.allocation_time)
        filename = self.filename.format(self.date, self.allocation_time)
        if settings.ENV == 'dev':
            subject = "Testing DEV DIM Allocation {} {}".format(self.date, self.allocation_time)
        elif settings.ENV == 'uat':
            subject = "Testing UAT DIM Allocation {} {}".format(self.date, self.allocation_time)
        else:
            subject = "DIM Allocation {} {}".format(self.date, self.allocation_time)
        email_exchange = EmailExchange()
        email_exchange.connect()
        email_exchange.create_email()
        email_exchange.add_attachment(filename=filename, file_path=self.filepath)
        email_exchange.send_email(subject, body, recipients=self.recipients)
        email_exchange.disconnect()
        os.remove(self.filepath + filename)

    def create_empty_report_with_message(self, message):
        """Creates an empty report with explanation message.
        
        Args:
            message: Explanation of why report is empty
        """
        ### Added by Suyana 02/16/2026 - Create empty report when no analysts available
        import csv
        filename = self.filename.format(self.date, self.allocation_time)
        filepath = os.path.join(self.filepath, filename)
        
        with open(filepath, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["DIM Allocation Report"])
            writer.writerow([f"Date: {self.date}"])
            writer.writerow([f"Time: {self.allocation_time} EST"])
            writer.writerow([])
            writer.writerow(["Status:", message])
            writer.writerow([])
            writer.writerow(["Id", "Policy", "Date/Time", "Sender", "Analyst"])
            writer.writerow(["No incidents allocated - No analysts available"])
        
        log.info(f"Created empty report with message: {message}")
        print(f"[CRON INFO] Empty report created at: {filepath}")
    
    def send_no_analysts_email(self):
        """Send email notification when no analysts are available."""
        ### Added by Suyana 02/16/2026 - Send email when no analysts available
        if settings.ENV == 'dev':
            subject = "Testing DEV DIM Allocation {} {} - No Analysts Available".format(self.date, self.allocation_time)
        elif settings.ENV == 'uat':
            subject = "Testing UAT DIM Allocation {} {} - No Analysts Available".format(self.date, self.allocation_time)
        else:
            subject = "DIM Allocation {} {} - No Analysts Available".format(self.date, self.allocation_time)
        
        body = """
        <h3>DIM Allocation Alert</h3>
        <p><b>Date:</b> {}</p>
        <p><b>Time:</b> {} EST</p>
        <p><b>Status:</b> No analysts available for incident allocation</p>
        <br>
        <p><b>Reason:</b> All analysts are currently unavailable due to one or more of the following:</p>
        <ul>
            <li>Out of Office (OOO)</li>
            <li>In Training</li>
            <li>On Response Box Duty</li>
            <li>Have 0% allocation percentage</li>
            <li>Not scheduled for current time</li>
        </ul>
        <br>
        <p>An empty report has been generated for record keeping.</p>
        <p>Please review analyst schedules and allocation percentages in SharePoint.</p>
        """.format(self.date, self.allocation_time)
        
        try:
            email_exchange = EmailExchange()
            email_exchange.connect()
            email_exchange.create_email()
            email_exchange.send_email(subject, body, recipients=self.recipients)
            email_exchange.disconnect()
            log.info("Sent no analysts available notification email")
            print("[CRON INFO] No analysts email notification sent successfully")
        except Exception as e:
            log.error(f"Failed to send no analysts email: {e}")
            print(f"[CRON ERROR] Failed to send email: {e}")
    
    def send_no_inc_email(self):
        """Email assignment report to DIM analysts.

        Emails report to recipients and subsequently deletes it from disk.

        Args:
            body (string): HTML to send in message body.
        Returns:
            None: Emails report from self.filepath/self.filename to self.recipients
        """
        if settings.ENV == 'dev':
            subject = "Testing DEV DIM Allocation {} {}".format(self.date, self.allocation_time)
        elif settings.ENV == 'uat':
            subject = "Testing UAT DIM Allocation {} {}".format(self.date, self.allocation_time)
        else:
            subject = "DIM Allocation {} {}".format(self.date, self.allocation_time)
        body = "No available incidents for allocation"
        email_exchange = EmailExchange()
        email_exchange.connect()
        email_exchange.create_email()
        email_exchange.send_email(subject, body, recipients=self.recipients)
        email_exchange.disconnect()

    def inc_case_filtered(self, inc):
        """
        Here, I need to access each inc.incidentID and check if it is in the VontuDLP.
        If an inc is in the table and marked as 'correlation data': drop it
        If an inc is not in our table: drop it.
        """
        inc_id = inc["incidentId"]
        model_inc = VontuDLP.objects.filter(incidentID=inc_id, protocol='SMTP')
        if len(model_inc) == 0:
            return False
        elif model_inc[0].cstatus == 'Correlation Data':
            return False
        return True

    def run_allocation(self):
        """Main function to orchestrate allocation."""
        log.info("Starting DIM Allocation.")
        self.response, self.inc_details = self.vontu_client.get_report_data(self.vontu_report)
        
        # Added by Suyanana - Limit to 10 incidents for faster testing
        print(f"[CRON TEST] Retrieved {len(self.inc_details)} incidents from Vontu")
        if len(self.inc_details) > 10:
            self.inc_details = self.inc_details[:10]
            print(f"[CRON TEST] Limited to 10 incidents for testing")
        
        filtered_inc = []
        log.info("Retrieved incident list.", extras={"inc_total": len(self.inc_details)})
        for inc in self.inc_details:
            string = inc.pop("detectionDate")
            date_time = string.replace("T", " ")
            FMT = '%Y-%m-%d %H:%M:%S'
            inc["detectionDateTime"] = datetime.datetime.strptime(date_time, FMT)
            if self.inc_case_filtered(inc):
                filtered_inc.append(inc)
        log.info("Retrieved filtered list.", extras={"inc_total": len(filtered_inc)})
        self.inc_details = filtered_inc

        #self.inc_details = self.vontu_client.get_incident_details(self.inc_ids)[0]
        log.info("Retrieved incident details.")

        # Get Time for current allocation in Eastern
        self.eastern_offset = self.get_eastern_offset()
        self.allocation_time = (
            (datetime.datetime.now() - datetime.timedelta(hours=self.eastern_offset))
            .time()
            .replace(microsecond=0)
        )

        if len(filtered_inc) == 0:
            self.send_no_inc_email()
            log.info("No available incidents for allocation, Sent Email")
            sys.exit(0)

        log.info("Set allocation time.", extras={"alloc_time": self.allocation_time})

        self.analysts = self.retry(self.get_analysts)
        log.info("Retrieved base analysts list.", extras={"analyst_count": len(self.analysts)})

        # Remove analysts not Regularly Scheduled for the day
        self.analysts = self.analysts[self.analysts["Scheduled"]]
        log.info(
            "Removed analysts not regularly scheduled.", extras={"analyst_count": len(self.analysts)}
        )

        # Remove analysts OOO
        self.analysts_ooo = self.retry(self.get_analysts_ooo)
        self.analysts = self.analysts[~self.analysts["Email"].isin(self.analysts_ooo)]
        log.info("Removed analysts OOO.", extras={"analyst_count": len(self.analysts)})

        # Remove analysts exempt for Training
        self.analysts_training = self.retry(self.get_analysts_training)
        self.analysts = self.analysts[~self.analysts["Email"].isin(self.analysts_training)]
        log.info("Removed analysts training.", extras={"analyst_count": len(self.analysts)})

        # Remove analysts exempt for Response Box Duty
        self.analysts_respbox = self.retry(self.get_analysts_respbox)
        self.analysts = self.analysts[~self.analysts["Email"].isin(self.analysts_respbox)]
        log.info("Removed analysts on respbox duty.", extras={"analyst_count": len(self.analysts)})

        # Remove analysts w/ 0% utilization rate
        self.analysts = self.analysts[self.analysts["Allocation Percentage"] != 0]
        log.info("Removed analysts w/ 0 utilization.", extras={"analyst_count": len(self.analysts)})
        
        ### Added by Suyana 02/16/2026 - Handle case when no analysts available after filtering
        if len(self.analysts) == 0:
            log.warning("No analysts available after all filtering!")
            print(f"[CRON WARNING] No analysts available at {self.allocation_time} EST")
            print("[CRON INFO] Reasons: All analysts are either OOO, in training, on respbox duty, or have 0% allocation")
            
            # Create empty report with explanation
            self.analysts = pd.DataFrame(columns=["Email", "Assigned"])
            self.create_empty_report_with_message(
                f"No analysts available for allocation at {self.allocation_time} EST. "
                f"All analysts are either OOO, in training, on response box duty, or have 0% allocation."
            )
            # Send notification email
            self.send_no_analysts_email()
            log.info("No analysts available for allocation. Sent notification email and created empty report.")
            sys.exit(0)

        self.assign_incidents()
        log.info(
            "Assigned Incidents.",
            extras={"inc_total": len(self.inc_details), "analyst_count": len(self.analysts)},
        )

    def retry(self, func, retries=3):
        for i in range(retries):
            try:
                sleep(5)
                results = func()
                log.info("Completed function in retry loop.", extras={"retries": i})
                return results
            except AttributeError as e:
                log.error("Errored while trying to complete function in retry loop.", extras={"error": e})
                if i == (retries - 1):
                    # Handle opscall_raw separately since it is only required for reporting
                    # If the retried function is get_analysts_opscall, set opscall_raw to None
                    # This prevents an error from interrupting the allocation process and handling
                    if func.__name__ == 'get_analysts_opscall':
                        self.opscall_raw = None
                        log.info("Max retries reached, setting opscall_raw to default value 'None'")
                        return
                    log.error("Max retries reached, raising error.")
                    raise
                else:
                    continue


def main():
    retries = 0
    status = ""

    while retries != 3:
        try:
            # sleep(500)  # Commented by Suyanana - No need to wait for testing
            dim_alloc = DIMAllocation()
            dim_alloc.run_allocation()
            body = dim_alloc.build_email_body()
            dim_alloc.create_report()
            dim_alloc.send_email(body)
            retries = 3
            status = "Running"

        except Exception as e:
            import traceback
            trace = traceback.format_exc()
            log.error("Unexpected Error!", extras={"error": e, "trace": trace})
            retries += 1
            status = "Failed"

    if status == "Failed" and retries == 3:
        # Fixed by Suyanana - 'e' variable was not defined here
        log.error("Max retries reached. Allocation failed after 3 attempts")
        # Don't post to mattermost in test mode
        # text = f"Error in DIM Allocation after 3 retries"
        # post_to_mattermost(self.channel, self.username, self.icon_url, text)


if __name__ == "__main__":
    main()
