import requests
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from dotenv import load_dotenv
import os
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# --- CONFIGURATION ---

# Google
GOOGLE_SCOPES = ['https://www.googleapis.com/auth/admin.directory.user.readonly']
GOOGLE_CREDENTIALS_FILE = 'credentials.json'

google_admin_user = os.getenv("GOOGLE_ADMIN_USER")
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
servicetitan_api_key = os.getenv("SERVICETITAN_API_KEY")
st_tenant_id = os.getenv("SERVICETITAN_TENANT_ID")

# --- FUNCTIONS ---

def get_google_users():
  credentials = service_account.Credentials.from_service_account_file(
    GOOGLE_CREDENTIALS_FILE,
    scopes=GOOGLE_SCOPES,
    subject=google_admin_user
  )
  service = build('admin', 'directory_v1', credentials=credentials)
  results = service.users().list(customer='my_customer', maxResults=200, orderBy='email').execute()
  return results.get('users', [])

def get_calendars():
  scopes = ['https://www.googleapis.com/auth/calendar.readonly']
  credentials = service_account.Credentials.from_service_account_file(
    GOOGLE_CREDENTIALS_FILE,
    scopes=scopes,
    subject=google_admin_user
  )
  service = build('calendar', 'v3', credentials=credentials)

  calendar_ids = []
  page_token = None

  while True:
    response = service.calendarList().list(pageToken=page_token).execute()
    print(json.dumps(response, indent=2))
    for calendar in response.get('items', []):
      calendar_ids.append({
        'id': calendar['id'],
        'summary': calendar.get('summary', '(no summary)')
      })
    page_token = response.get('nextPageToken')
    if not page_token:
      break

  return calendar_ids

def get_user_calendar_service(user_email):
  scopes = ['https://www.googleapis.com/auth/calendar.readonly']
  credentials = service_account.Credentials.from_service_account_file(
    GOOGLE_CREDENTIALS_FILE,
    scopes=scopes
  ).with_subject(user_email)  # ← Impersonate the user
  return build('calendar', 'v3', credentials=credentials)

def get_calendar_events(user_email):
  calendar_service = get_user_calendar_service(user_email)
  events = calendar_service.events().list(calendarId=user_email).execute()

  return events['items']

def find_personal_events(user_email):
  all_events = get_calendar_events(user_email)

  num_personal_events = 0
  personal_events = []
  for event in all_events:
    # print(event)
    # print('-------------------')
    if 'summary' in event.keys():
      if 'doctors' in event['summary'].lower() or 'unavailable' in event['summary'].lower():
        personal_event = {
          'googe_id': event['id'],
          'google_email': user_email,
          'created': event['created'],
          'updated': event['updated'],
          'creator_email': event['creator']['email'],
          'organizer_email': event['organizer']['email'],
          'summary': event['summary']
        }
        if 'description' in event:
          personal_event['description'] = event['description']
        if 'dateTime' in event['start']:
          personal_event['start_dateTime'] = event['start']['dateTime']
        if 'dateTime' in event['end']:
          personal_event['end_dateTime'] = event['end']['dateTime']
        if 'date' in event['start']:
          personal_event['start_date'] = event['start']['date']
        if 'date' in event['end']:
          personal_event['end_date'] = event['end']['date']
        if 'timeZone' in event['start']:
          personal_event['time_zone'] = event['start']['timeZone']
        # print(event)
        # print('---------------------')
        personal_events.append(personal_event)
        num_personal_events += 1
  # print(f'Num personal events: {num_personal_events}')
  return personal_events

## Read in and compare saved personal events with new batch of personal events
## Create or update ST event as necessary, return updated list of events to save to file
def find_and_compare_events():
  with open('personal_events_by_user.json', 'r') as file:
    saved_personal_events_by_user = json.load(file)
    saved_users_personal_events = saved_personal_events_by_user[user]
  for user in all_personal_events.keys():
    users_personal_events = all_personal_events[user]
    event_found = False
    for event in users_personal_events:
      for saved_event in saved_users_personal_events:
        if event['google_id'] == saved_event['google_id']:
          ## TODO: CHECK IF DATES AND TIMES ARE THE SAME
          event_found = True
          break
      if not event_found:
        ## TODO: create event in ST and save event to saved_personal_events_by_user object
        pass
  
  return saved_personal_events_by_user


## ServiceTitan functions
def login_to_st():
  print("Logging in to ServiceTitan...")

  # Integration Endpoint
  # url = "https://auth-integration.servicetitan.io/connect/token"

  # Production Endpoint
  url = "https://auth.servicetitan.io/connect/token"

  payload = f"grant_type=client_credentials&client_id={client_id}&client_secret={client_secret}"
  headers = {"Content-Type": "application/x-www-form-urlencoded"}

  response = requests.request("POST", url, data=payload, headers=headers)
  access_token = response.json()['access_token']

  print("Logged in!")

  return(access_token)

def get_servicetitan_technicians(access_token):
  SERVICETITAN_API_URL = 'https://api.servicetitan.io/settings/v2/tenant/{st_tenant_id}/technicians'

  headers = {
    'Authorization': access_token,
    'ST-App-Key': servicetitan_api_key,
    'Content-Type': 'application/json'
  }
  technicians = []
  offset = 0
  limit = 100

  while True:
    response = requests.get(f"{SERVICETITAN_API_URL}?offset={offset}&limit={limit}", headers=headers)
    if response.status_code != 200:
      raise Exception(f"ServiceTitan API error: {response.status_code} - {response.text}")

    data = response.json()
    technicians.extend(data.get('data', []))

    if len(data.get('data', [])) < limit:
      break
    offset += limit

  return technicians

def create_new_non_job_event(personal_event, access_token):
  url = f"https://api.servicetitan.io/dispatch/v2/tenant/{st_tenant_id}/non-job-appointments"
  headers = {
    "Authorization": access_token, 
    "ST-App-Key": servicetitan_api_key
  }

  with open('user_matches.json', 'r') as file:
    user_matches = json.load(file)
    st_id = user_matches[personal_event['google_email']]['servicetitan_id']
    print(st_id)

  start = datetime.fromisoformat(personal_event['start_dateTime'][:-6])
  end = datetime.fromisoformat(personal_event['end_dateTime'][:-6])
  duration = end - start
  payload = {
    "technicianId": st_id,
    "start": start,
    "duration": duration,
    "name": personal_event['summary'],
    "summary": personal_event['description'],
    "removeTechnicianFromCapacityPlanning": True
  }
  response = requests.request("POST", url, data=payload, headers=headers)
  # print(response.text)


def match_users_and_techs(google_users, technicians):
  matches = {}

  for user in google_users:
    g_email = user['email'].lower().strip()
    g_name = user['name'].strip().lower().strip()
    alias_email = g_email.split('@')
    alias_email = alias_email[0] + '+1@' + alias_email[1]

    for tech in technicians:
      t_email = tech['email']
      if t_email:
        t_email = t_email.lower().strip()
      t_name = tech['name'].lower().strip()

      if g_email == t_email or alias_email == t_email or g_name in t_name:
        matches[g_email] = {
          'google_id': user['id'],
          'google_email': g_email,
          'google_name': g_name,
          'servicetitan_id': tech['id'],
          'servicetitan_name': t_name,
          'servicetitan_email': t_email
        }
        break  # stop at first match for each Google user

  return matches

def find_non_matching_users(google_users, technicians, matches):
  non_matches = []
  # for user in google_users:
  #   match_found = False
  #   for match in matches:
  #     if user['id'] == match['google_id']:
  #       match_found = True
  #       break
  #   if not match_found:
  #     non_match = {
  #       'system': 'google',
  #       'id': user['id'],
  #       'name': user['name'],
  #       'email': user['email']
  #     }
  #     non_matches.append(non_match)
  
  for tech in technicians:
    match_found = False
    for user in matches.keys():
      if tech['id'] == user['servicetitan_id']:
        match_found = True
        break
    if not match_found:
      non_match = {
        'system': 'servicetitan',
        'id': tech['id'],
        'name': tech['name'],
        'email': tech['email']
      }
      non_matches.append(non_match)
  
  return non_matches

# --- MAIN SCRIPT ---

if __name__ == "__main__":
  access_token = login_to_st()
  ## Get list of Users from Google Workspace
  # print("Fetching Google Workspace users...")
  # google_users = get_google_users()
  # g_users = []
  # for user in google_users:
  #   new_user = {
  #     'name': user['name']['fullName'],
  #     'id': user['id'],
  #     'email': user['primaryEmail']
  #   }
  #   g_users.append(new_user)

  #   with open('google_users.json', 'w') as file:
  #     json.dump(g_users, file, indent=2)

  ## Get list of calendars in Google Workspace
  # print("Fetching Google Workspace calendars...")
  # google_calendars = get_calendars()
  # print(len(google_calendars))
  # n = 1
  # for calendar in google_calendars:
  #   print(f'Calendar {n}')
  #   # print(calendar['name']['fullName'])
  #   # print(calendar['primaryEmail'])
  #   # print(calendar['id'])
  #   print(calendar)
  #   print('--------------------------')
  #   n += 1

  ## Get list of events from google calendars
  # with open('user_matches.json', 'r') as file:
  #   matches = json.load(file)
  # all_personal_events = {}
  # for user_email in matches.keys():
  #   print(matches[user_email]['google_name'])
  #   print(user_email)
  #   personal_events = find_personal_events(user_email)
  #   all_personal_events[user_email] = personal_events

  # with open('personal_events_by_user.json', 'w') as file:
  #   json.dump(all_personal_events, file, indent=2)

  # events = get_calendar_events('enes@amstillroofing.com')
  # for event in events:
  #   print(event)
  
  # n = 1
  # for event in events['items']:
    # print(f'Technician {n}: {user.google_name}')
    # print(f'Event {n}')
    # print(event)
    # print(event['id'])
    # print(event['created'])
    # print(event['updated'])
    # print(event['creator']['email'])
    # print(event['organizer']['email'])
    # print(event['start']['dateTime'])
    # print(event['start']['timeZone'])
    # print(event['end']['dateTime'])
    # print(event['end']['timeZone'])
    # print('--------------------------')
    # n += 1

  ## We don't need this??
  # calendar_events = get_calendar_events('zac@amstillroofing.com')
  # n = 1
  # for event in calendar_events:
  #   # print(f'Technician {n}: {user.google_name}')
  #   print(f'Event {n}')
  #   print(event)
  #   # print(user['primaryEmail'])
  #   # print(user['id'])
  #   print('--------------------------')
  #   n += 1


  ## Get list of ServiceTitan technicians
  # print("Fetching ServiceTitan technicians...")
  # servicetitan_techs = get_servicetitan_technicians(access_token)
  # techs = []
  # for tech in servicetitan_techs:
  #   new_tech = {
  #     'id': tech['id'],
  #     'userId': tech['userId'],
  #     'name': tech['name'],
  #     'email': tech['email']
  #   }
  #   techs.append(new_tech)

  #   with open('st_techs.json', 'w') as file:
  #     json.dump(techs, file, indent=2)

  # print("Matching users...")
  # with open('google_users.json', 'r') as file:
  #   google_users = json.load(file)
  # with open('st_techs.json', 'r') as file:
  #   st_techs = json.load(file)
  # matches = match_users_and_techs(google_users, st_techs)
  # with open('user_matches.json', 'w') as file:
  #   json.dump(matches, file, indent=2)
  
  # with open('google_users.json', 'r') as file:
  #   google_users = json.load(file)
  # print(f'Num google_users: {len(google_users)}')
  # with open('st_techs.json', 'r') as file:
  #   st_techs = json.load(file)
  # print(f'Num st_techs: {len(st_techs)}')
  # with open('user_matches.json', 'r') as file:
  #   matches = json.load(file)
  # print(f'Num matches: {len(matches)}')
  # with open('non_matches.json', 'r') as file:
  #   non_matches = json.load(file)
  # print(f'Num non-matches: {len(non_matches)}')

  # with open('user_matches.json', 'r') as file:
  #   matches = json.load(file)
  # non_matches = find_non_matching_users(google_users, st_techs, matches)
  # with open('non_matches.json', 'w') as file:
  #   json.dump(non_matches, file, indent=2)

  # for match in matches:
  #   print(match)

  # print(f"\nTotal matches found: {len(matches)}")

  ## Create new non-job event in ST
  create_new_non_job_event({
    "googe_id": "_88q38c9k64r48ba388s44b9k6533cba270r30ba26gqk6dq384sj0cpm8o",
    "google_email": "will@amstillroofing.com",
    "created": "2019-12-27T03:59:35.000Z",
    "updated": "2019-12-27T03:59:35.693Z",
    "creator_email": "adam@amstillroofing.com",
    "organizer_email": "adam@amstillroofing.com",
    "summary": "TEST",
    "description": "this is a test",
    "start_dateTime": "2025-06-01T13:00:00-06:00",
    "end_dateTime": "2025-06-01T14:00:00-06:00",
    "time_zone": "UTC"
  }, access_token)