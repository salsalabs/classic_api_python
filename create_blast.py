# App to save a supporter and confirm the write.
import argparse
import requests
import yaml
import json

# Get the login credentials
parser = argparse.ArgumentParser(description='Create an email blast with hard-coded content.')
parser.add_argument('--login', dest='loginFile', action='store',
                    help='YAML file with login credentials')

args = parser.parse_args()
cred = yaml.load(open(args.loginFile))

content = """
<table class=adminlist>

    <tr valign="top">
    <th>HTML_Content</th><tr><td><table width="600" cellspacing="1" cellpadding="1" border="0" align="center">
    <tbody>
        <tr>
            <td>
<p style="text-align: center;"></p>
</td></tr><tr><td><div style="display: none; font-size: 1px; color: #ffffff; line-height: 1px; max-height: 0px; max-width: 0px; opacity: 0; overflow: hidden;">Renew your membership in CALPIRG today.</div><p><? var Member_Number=Supporter.supporter_KEY; var ROI_ID=Supporter.roi_family_id; var Member_Since=Supporter.cap_member_since; var Membership_Amount=Supporter.cap_membership_renewal_amount; var Current_Year=new Date().getFullYear(); var Landing_Page="https://calpirg.webaction.org/p/salsa/donation/common/public/?donate_page_KEY=22658&tag=email_blast:[[email_blast_KEY]]&utm_source=Salsa&utm_medium=Email&utm_campaign=USP4-FORG-0918&utm_content=RE2:01A:0HH-OOD&uid=[[supporter_KEY]]"; if ((typeof ROI_ID != "undefined") && ( ROI_ID != 0)) { Member_Number=ROI_ID; } if ((typeof Membership_Amount == "undefined") || (Membership_Amount <= 25)) { Membership_Amount=25; } Landing_Page=Landing_Page+"&amount="+Membership_Amount; var Open_Link = "<a href='" + Landing_Page + "'>"; var Close_Link = "</a>"; var Member_Record="<table id='callout' style='border: 2px solid #0000cc; margin-bottom: 5px; margin-left: 10px;' cellspacing='0' cellpadding='0' bgcolor='#ffffff' align='right' border='0'> <tbody> <tr> <td style='padding: 10px;' width='220'> <p style='font-size:16px;line-height:24px;color:#333;'><span style='font-weight:700'>Name:</span> [[First_Name]] [[Last_Name]]<br /> <span style='font-weight:700'>CALPIRG Member Number: </span>" + Member_Number + "<br />"; if (typeof Member_Since != "undefined") { var Member_Year = Member_Since.substring(0,4); } if ((typeof Member_Since != "undefined") && (parseInt(Member_Year) < Current_Year)) { Member_Record=Member_Record+"<span style='font-weight:700'>CALPIRG Member Since:</span> "+Member_Year+" <br /><span style='font-weight:700'>2018 Early Membership Renewal Status: "; } else { Member_Record=Member_Record+"<span style='font-weight:700'>2018 Membership Renewal Status: "; } Member_Record=Member_Record+"<span style='color:#ed1d24;'>PENDING</span></span></p> <div><a href='" + Landing_Page + "' style='background-color: #bf2232; border: 1px solid #bf2232; border-radius: 4px; color: #ffffff; display: inline-block; font-family: Verdana; font-size: 14px; font-weight: bold; line-height: 40px; text-align: center; text-decoration: none; width: 100%;'><span style='color: white;'>Renew Your Support</span></a></div> </td> </tr> </tbody></table>"; ?></p> <p><? print(Member_Record); ?></p> <p style="font-size:16px;line-height:24px;color:#333;">Hi [[First_Name]], <br><br> We're tackling major challenges in California and across the country. <br><br> Millions fall ill every year from antibiotic-resistant infections... yet industrial farms continue to recklessly give our life-saving medicines to healthy animals. <br><br> The Consumer Financial Protection Bureau is our best Wall Street watchdog... but some proposals hidden in next year's federal budget could prevent the agency from holding companies like Wells Fargo and Equifax accountable for wrongdoing. <br><br> Tens of thousands of new products are introduced every year... but reviews and online surveys rarely identify the hidden hazards and risks that can be found in these products. <br><br> <strong>It doesn't have to be this way.</strong> But if we're going to tackle these problems that affect millions of lives, we need to bring Californians together to put smart solutions in place. <br><br> <strong><? print(Open_Link); ?>Renew your membership in CALPIRG and together, we can win positive change.<? print(Close_Link); ?></strong> <br><br> <strong>You can save our antibiotics.</strong> By giving routine doses of antibiotics to livestock, industrial farms are contributing to antibiotic-resistant "superbugs" that kill at least 23,000 people in the U.S. every year. We've already pushed McDonald's, Subway and KFC to stop serving chicken raised on routine antibiotics. Now, with your support, we're calling on McDonald's, the country's biggest buyer of beef, to get our life-saving medicines out of its entire supply chain. <strong><? print(Open_Link); ?>Renew your support today >><? print(Close_Link); ?></strong> <br><br> <strong>You can defend the Consumer Bureau.</strong> After the Great Recession, we helped create the Consumer Financial Protection Bureau to make sure banks and other financial companies play by the rules. Now, this crucial agency is facing attacks from Congress and the Trump administration that could put consumers at risk and open the door to the same risk taking that led to the 2008 crash. With your support, we will continue defending the original mission of the Consumer Bureau. <strong><? print(Open_Link); ?>Renew your support today >><? print(Close_Link); ?></strong> <br><br> <strong>You can improve consumers' lives.</strong> For more than 40 years, our national network has been educating consumers about how to protect themselves and their rights. We work to get dangerous products off store shelves, end exploitative practices, and ensure a level playing field in the marketplace. And we advocate for new rules, institutions and policies to defend consumers from the power of special interests. With the support of our members, our watchdog team will continue protecting Californians from data breaches, dangerous toys and more. <strong><? print(Open_Link); ?>Renew your support today >><? print(Close_Link); ?></strong> <br><br> Member support makes it all possible -- we don't take a dime from big corporations, just people like you. <strong><? print(Open_Link); ?>Can you renew your membership today?<? print(Close_Link); ?></strong> <br><br> Thanks, <br><br> Emily Rusch<br>Executive Director <br><br> P.S. If you've already sent your renewal gift, thank you so much! Please disregard this email and we'll update your records as soon as possible.</p></td></tr><tr></tr><tr><td style="padding:0; border-top:2px solid #03226f;"><br /><p align="center"><a href="https://www.facebook.com/CALPIRG">Join us on Facebook</a> | <a href="https://twitter.com/CALPIRG">Follow us on Twitter</a><br />
                    <br />
                    California Public Interest Research Group, Inc., 1111 H St., Ste. 207, Sacramento, CA 95814, (916) 448-4516<br />
                    Member questions or requests call 1-800-838-6554.<br />
                    <br />
                    If you want us to stop sending you email then follow this link --<a href="http://pin.salsalabs.com/o/502/c/3/p/salsa/supporter/unsubscribe/public/?Email=[[Email]]&email_blast_KEY=[[email_blast_KEY]]" style="font-weight:bold;text-decoration:none;color:#2ba6cb;"> unsubscribe</a>.</p>
                    </td>
                </tr>
            </tbody>
        </table>
</td></tr>

</table>
"""

# Authenticate
payload = {
    'email': cred['email'],
    'password': cred['password'],
    'json': True }
s = requests.Session()
u = 'https://' + cred['host'] + '/api/authenticate.sjs'
r = s.get(u, params=payload)
j = r.json()
if j['status'] == 'error':
    print('Authentication failed: ', j)
    exit(1)

print('Authentication: ', j)

# Save this supporter to the database.  Note that Salsa will update an existing
# record if the email address matches.
payload = {
    'json': True,
    'object': 'email_blast',
    'HTML_Content': content,
}

# Note 1: `/save` must use HTTP POST.  The payload is stored in the body. The
# body is huge (2MB) and it's unlikely that an API call will overrun it.
#
# NOte 2: Results are always returned as a list, even if only one record is
# saved.
u = 'https://' + cred['host'] +'/save'
r = s.post(u, data=payload)
j = r.json()
print('Save: ', r.json())
key = j[0]["key"]
print("Saved with key: ", key)

# Read to confirm the modification.  The `getObject.sjs` call returns a single
# record as a dictionary.
payload = {
    'json': True,
    'object': 'supporter',
    'key': key }
u = 'https://' + cred['host'] +'/api/getObject.sjs'
r = s.get(u, params=payload)
supporter = r.json()
print('Insert confirmation: content is ')

f = '{:10}{:10} {:10} {:20}'
print(f.format(supporter["supporter_KEY"],
    supporter["First_Name"],
    supporter["Last_Name"],
    supporter["Email"]
    ))
