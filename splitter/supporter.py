import csv
import os
import logging
import os.path
import pathlib
import threading

from base import ReaderBase, SaverBase

class SupporterReader (ReaderBase):
    """
    Read supporter records at a nominal rate of 500 per chunk. Output them
    singly to the output queues.
    """

    def __init__(self, **kwargs):
        """
        Initialize a SupporterReader object

        Params in kwargs:

        :threadID: numeric cardinal thread identifier
        :cred:     login credentials (from the YAML file)
        :session:  requests object to read from Salsa
        :cond:     conditions to use to filter supporters
        :supporterSaveQueue: supporter save queue
        :groupsQueue:   group save queue
        :donationQueue:     donation sqve queue
        :start:    starting offset in the Salsa database
        :exitFlag: boolean indicating when processing should stop
        """
        ReaderBase.__init__(self, **kwargs)

        x = []
        for k, v in SupporterMap.items():
            if v :
                x.append(v)
        self.incl = ",".join(x)

    def run(self):
        """
        Run the thread.  Overrides Threading.run()
        """

        self.log.info('starting')
        self.process_data()
        self.log.info("Ending  " + self.threadName)

    def process_data(self):
        """
        Read supporters from the database.  Queue them up individually
        to the output queue(s).
        """
        offset = self.offset
        count = 500
        while count == 500:
            payload = {'json': True,
                       "limit": f"{offset},{count}",
                       'object': 'supporter',
                       'condition': self.cond,
                       'include': self.incl}
            u = f"https://{self.cred['host']}/api/getObjects.sjs"
            self.log.info(f"reading {count} from {offset:7d}")
            r = self.session.get(u, params=payload)
            j = r.json()

            # Iterate through the records and push each onto the output queues.
            for supporter in j:
                if supporter["Receive_Email"] != "Unsubscribed":
                    self.supporterSaveQueue.put(supporter)
                    # Donations processor will write donation records for all supporters
                    # with emails.  Donations will queue up supporter records for
                    # supporters with donation history.
                    self.donationQueue.put(supporter)
                self.groupsQueue.put(supporter)

            count = len(j)
            offset += count

class SupporterSaver (SaverBase):
    """
    Accepts Group records from a queue, then write them to a CSV file.
    """

    def __init__(self, **kwargs):
        """
        Initialize a GroupSaver instance
        """
        SaverBase.__init__(self, **kwargs)

    def getFieldMap(self):
        """
        Specify the output fields for the CSV file.  Overrides SaverBase.getFieldMap();
        """
        return SupporterMap

    def process_data(self):
        """
        Accept a supporter from the supporter save queue and save it to a CSV
        file.
        """

        count = self.maxRecs
        while not self.exitFlag:
            supporter = self.supporterSaveQueue.get()
            if not supporter:
                continue
            if count >= self.maxRecs:
                count = 0
                self.openFile()
            # csv writer complains if there's stuff in the record
            # that's not going to be written
            if 'object' in supporter:
                del supporter['object']
            if 'key' in supporter:
                del supporter['key']
            # Classic-to-Engage fixes.
            if supporter['Receive_Email'] > '0':
                supporter['Receive_Email'] = "Subscribed"
            else:
                supporter['Receive_Email'] = "Unsubscribed"

            # Create a new dict of Engage headers and Classic values.
            m = {}
            for k in SupportMapOrder:
                v = self.getFieldMap()[k]
                if v:
                    m[k] = str.strip(supporter[v])
            try:
                self.writer.writerow(m)
                count = count + 1
            except UnicodeEncodeError:
                self.log.error(f"{self.threadName}_{self.threadID:02d}: UnicodeEncodeError on {supporter}")
# QZ 25-Jan-2019
SupporterMap = {
        "email":           "Email",
        "title":           "Title",
        "firstName":       "First_Name",
        "middleName":      "MI",
        "lastName":        "Last_Name",
        "suffix":          "Suffix",
        "status":          "Receive_Email",
        "addressLine1":    "Street",
        "addressLine2":    "Street_2",
        "city":            "City",
        "state":           "State",
        "country":         "Country",
        "postalCode":      "Zip",
        "homePhone":       "Phone",
        "cellPhone":       "Cell_Phone",
        "workPhone":       "Work_Phone",
        "languageCode":    "Language_Code",
        "externalID":      "supporter_KEY",
        "supporter_KEY":   "supporter_KEY",
        "Date_Created":    "Date_Created",
        "Last_Modified":   "Last_Modified",
        "CC_Called_Ford_Thanksgiving_20171117": "cc_called_ford_thanksgiving_20171117",
        "CW_-_2017_tax_activist_rally": "cw___2017_tax_activist_rally",
        "cw_ac_flickr_yes_no": "cw_flickr_yes_no",
        "cw_ac_google+_yes_no": "cw_google__yes_no",
        "cw_ac_instagram_yes_no": "cw_instagram_yes_no",
        "cw_ac_linkedin_yes_no": "cw_linkedin_yes_no",
        "cw_ac_other_social_media_blank_field": "cw_other_social_media_blank_field",
        "cw_ac_reddit_yes_no": "cw_reddit_yes_no",
        "cw_ac_social_media_facebook_question_1": "cw_social_media_facebook_question_1",
        "cw_ac_tumblr_yes_no": "cw_tumblr_yes_no",
        "cw_ac_twitter_yes_no": "cw_twitter_yes_no",
        "cw_ac_youtube_yes_no": "cw_youtube_yes_no",
        "cw_cleanbudget_noriders_grassroots_opt-in_2016": "cw_cleanbudget_noriders_grassroots_opt_in_2016",
        "CW_FR_persnl_story": "cw_fr_persnl_story",
        "cw_online_video_call_skype_google_hangout_etc": "cw_online_video_call_skype_google_hangout_etc",
        "cw_SYRACUSE_ny_single_payer_hearing": "cw_syracuse_ny_single_payer_hearing",
        "DIFP_community_meeting_hosts_CF_20161125": "difp_community_meeting_hosts_cf_20161125",
        "DIFP_FL_second_chances_volunteer_20171010": "difp_fl_second_chances_volunteer_20171010",
        "DIFP_Organize_a_Democracy_Team_(a_local_group)_to_do_ongoing_work_to_support_the_effort.": "difp_organize_a_democracy_team__a_local_group__to_do_ongoing_work_to_support_the_effort_",
        "difp_polling_captain_for_petition_gathering_": "difp_polling_captain_for_petition_gathering_",
        "DIFP_Target_State_I_can_help_pass_a_local_resolution_calling_for_a_constitutional_amendment": "difp_target_state_i_can_help_pass_a_local_resolution_calling_for_a_constitutional_amendment",
        "DIFP_team_leaders_willing_to_be_listed_as_a_leader": "difp_team_leaders_willing_to_be_listed_as_a_leader",
        "DIFP-CF-Secure-Our-Vote-Coalition-20180315": "difp_cf_secure_our_vote_coalition_20180315",
        "DIFP-Demand-Democracy-Volunteers": "difp_demand_democracy_volunteers",
        "Election_day_petitioning_checkbox_volunteer_coordinator_20160922": "election_day_petitioning_checkbox_volunteer_coordinator_20160922",
        "Florida_voting_rights_text_opt_in_20170801": "florida_voting_rights_text_opt_in_20170801",
        "I_want_to_meet_with_my_representative_this_month.": "i_want_to_meet_with_my_representative_this_month_",
        "johnson_amendment_in_district_meetings": "johnson_amendment_in_district_meetings",
        "Mueller_text_opt_in_20170728": "mueller_text_opt_in_20170728",
        "shareholder_activist_yes/no": "shareholder_activist_yes_no",
        "texas-other-electric-provider": "texas_other_electric_provider",
        "texas-what-is-your-electric-provider": "texas_what_is_your_electric_provider",
        "TX_Austin_Council_District": "austin_council_district",
        "TX_El_Paso_Council_District": "tx_el_paso_council_district",
        "TX_Leg_Action_Call": "tx_leg_action_call",
        "TX_Leg_Action_Email": "tx_leg_action_email",
        "TX_Leg_Action_LTE": "tx_leg_action_lte",
        "TX_Leg_Action_Public_Hearing_ATX": "tx_leg_action_public_hearing_atx",
        "TX_Leg_Action_Visit_Local": "tx_leg_action_visit_local",
        "TX_Pedernales_customer_yes/no_": "texas_pedernales_customer_2",
        "TX_San_Antonio_Council_District": "san_antonio_city_council_district",
        "Uses_Facebook": "user_facebook_account",
        "Uses_Instagram": "user_instagram_account",
        "Uses_Twitter": "user_twitter_account",
        "Vanguard_Customer_yes_no": "vanguard_customer_yes_no",
        "Volunteer:_Organize_an_Event": "volunteer_organize_an_event",
        "zzzzzzzzzzDIFP:_PHONE_NUMBER_(Jonah_11/8/12)": "phone_number_for_third_anniversary"
}
SupportMapOrder = [
    "externalID",
    "email",
    "status",
    "title",
    "firstName",
    "middleName",
    "lastName",
    "suffix",
    "addressLine1",
    "addressLine2",
    "city",
    "state",
    "country",
    "postalCode",
    "homePhone",
    "cellPhone",
    "workPhone",
    "supporter_KEY",
    "Date_Created",
    "Last_Modified",
    "CC Called Ford Thanksgiving 20171117",
    "CW - 2017 tax activist rally",
    "cw ac flickr yes no",
    "cw ac google+ yes no",
    "cw ac instagram yes no",
    "cw ac linkedin yes no",
    "cw ac other social media blank field",
    "cw ac reddit yes no",
    "cw ac social media facebook question 1",
    "cw ac tumblr yes no",
    "cw ac twitter yes no",
    "cw ac youtube yes no",
    "cw cleanbudget noriders grassroots opt-in 2016",
    "CW FR persnl story",
    "cw online video call skype google hangout etc",
    "cw SYRACUSE ny single payer hearing",
    "DIFP community meeting hosts CF 20161125",
    "DIFP FL second chances volunteer 20171010",
    "DIFP Organize a Democracy Team (a local group) to do ongoing work to support the effort.",
    "difp polling captain for petition gathering ",
    "DIFP Target State I can help pass a local resolution calling for a constitutional amendment",
    "DIFP team leaders willing to be listed as a leader",
    "DIFP-CF-Secure-Our-Vote-Coalition-20180315",
    "DIFP-Demand-Democracy-Volunteers",
    "Election day petitioning checkbox volunteer coordinator 20160922",
    "Florida voting rights text opt in 20170801",
    "I want to meet with my representative this month.",
    "johnson amendment in district meetings",
    "Mueller text opt in 20170728",
    "shareholder activist yes/no",
    "texas-other-electric-provider",
    "texas-what-is-your-electric-provider",
    "TX Austin Council District",
    "TX El Paso Council District",
    "TX Leg Action Call",
    "TX Leg Action Email",
    "TX Leg Action LTE",
    "TX Leg Action Public Hearing ATX",
    "TX Leg Action Visit Local",
    "TX Pedernales customer yes/no ",
    "TX San Antonio Council District",
    "Uses Facebook",
    "Uses Instagram",
    "Uses Twitter",
    "Vanguard_Customer_yes_no",
    "Volunteer:_Organize_an_Event",
    "zzzzzzzzzzDIFP:_PHONE_NUMBER_(Jonah_11/8/12)"
]
