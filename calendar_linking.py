from requests.exceptions import SSLError
import requests
import json
import time
from google.oauth2 import service_account
from googleapiclient.discovery import build
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta, timezone

# Load environment variables from .env file
load_dotenv()

# --- CONFIGURATION ---
DEBUG = False
TESTING = False

# Google
GOOGLE_SCOPES = ['https://www.googleapis.com/auth/admin.directory.user.readonly']
GOOGLE_CREDENTIALS_FILE = 'credentials.json'

google_admin_user = os.getenv("GOOGLE_ADMIN_USER")
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
servicetitan_api_key = os.getenv("SERVICETITAN_API_KEY")
st_tenant_id = os.getenv("SERVICETITAN_TENANT_ID")

# --- FUNCTIONS ---

## Setup function only for first time program is run
## Finds and matches ST techs and Google users, saves personal events from techs calendars
def setup(access_token):
  if not os.path.isfile('google_users.json'):
    google_users = get_google_users()
    with open('google_users.json', 'w') as file:
      json.dump(google_users, file, indent=2)
      
  if not os.path.isfile('st_techs.json'):
    st_techs = get_st_technicians(access_token)
    with open('st_techs.json', 'w') as file:
      json.dump(st_techs, file, indent=2)
      
  if not os.path.isfile('user_matches.json'):
    matches = match_users_and_techs(google_users, st_techs)
    with open('user_matches.json', 'w') as file:
      json.dump(matches, file, indent=2)
      
  if not os.path.isfile('personal_events_by_user.json'):
    save_personal_events(matches)

def get_google_users():
  print('Getting Google Users')
  credentials = service_account.Credentials.from_service_account_file(
    GOOGLE_CREDENTIALS_FILE,
    scopes=GOOGLE_SCOPES,
    subject=google_admin_user
  )
  service = build('admin', 'directory_v1', credentials=credentials)
  results = service.users().list(customer='my_customer', maxResults=200, orderBy='email').execute()
  results_users = results.get('users', [])
  users = {}
  for user in results_users:
    new_user = {
      'name': user['name']['fullName'],
      'id': user['id'],
      'email': user['primaryEmail'],
      'active': True
    }
    users[user['primaryEmail']] = new_user
  return users

## Finds users in Google, compares to previously saved users, updates saved users as necessary
def update_google_users():
  ## Get list of Users from Google Workspace
  print("Fetching Google Workspace users...")
  google_users = get_google_users()
  with open('google_users.json', 'r') as file:
    saved_users = json.load(file)
  for user_email in saved_users.keys():
    if user_email not in google_users.keys():
      ## User no longer in Google, mark as inactive and save to google_users
      saved_users[user_email]['active'] = False
      google_users[user_email] = saved_users[user_email]

  with open('google_users.json', 'w') as file:
    json.dump(google_users, file, indent=2)

  return google_users

def get_calendars():
  ## Get list of calendars in Google Workspace
  print("Fetching Google Workspace calendars...")
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
    # print(json.dumps(response, indent=2))
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
  # Calculate one month ago (30 days)
  one_month_ago = datetime.now(timezone.utc) - timedelta(days=30)

  # Convert to RFC3339 format (required by Google Calendar API)
  time_min = one_month_ago.isoformat()
  # time_min = '2025-06-14T00:00:00-06:00' ## find events as early as June 1, 2025
  # time_max = '2025-06-22T00:00:00-06:00' 

  calendar_service = get_user_calendar_service(user_email)
  events = calendar_service.events().list(calendarId=user_email, timeMin=time_min).execute()

  return events['items']

def find_personal_events(user_email):
  all_events = get_calendar_events(user_email)

  num_personal_events = 0
  personal_events = []
  for event in all_events:
    if 'summary' in event.keys():
      if event['summary'].lower().strip().startswith('unavailable') or event['summary'].lower().strip().startswith('personal') or event['summary'].lower().strip().startswith('ooo') or event['summary'].lower().strip().startswith('apitest'):
        print(event['summary'])
        if 'dateTime' in event['start'].keys():
          print(event['start']['dateTime'])
        else:
          print(event['start']['date'])
        personal_event = {
          'google_id': event['id'],
          'servicetitan_id': '-1',
          'google_email': user_email,
          'created': event['created'],
          'updated': event['updated'],
          'creator_email': event['creator']['email'],
          'organizer_email': event['organizer']['email'],
          'summary': event['summary']
        }
        if 'description' in event:
          personal_event['description'] = event['description']
        else:
          personal_event['description'] = ''
        if 'dateTime' in event['start']:
          personal_event['start_dateTime'] = event['start']['dateTime']
          personal_event['end_dateTime'] = event['end']['dateTime']
          personal_event['all_day'] = False
        else:
          personal_event['start_date'] = event['start']['date']
          personal_event['end_date'] = event['end']['date']
          personal_event['all_day'] = True
        if 'timeZone' in event['start']:
          personal_event['time_zone'] = event['start']['timeZone']
        personal_events.append(personal_event)
        num_personal_events += 1
  return personal_events

def save_personal_events(matches):
  ## Get list of events from google calendars
  ## SHOULD ONLY BE CALLED ONCE AT THE BEGINNING OF THE AUTOMATION FOR SETUP
  ## AFTER SETUP, CALL FIND AND COMPARE EVENTS
  all_personal_events = {}
  for user_email in matches.keys():
    print(matches[user_email]['google_name'])
    print(user_email)
    personal_events = find_personal_events(user_email)
    all_personal_events[user_email] = personal_events

  with open('personal_events_by_user.json', 'w') as file:
    json.dump(all_personal_events, file, indent=2)

## Read in and compare saved personal events with new batch of personal events
## Create or update ST event as necessary, return updated list of events to save to file
def find_and_add_or_update_events(access_token):
  print('Finding new or changed personal events in Google')
  ## read in saved personal events and google users
  with open('personal_events_by_user.json', 'r') as file:
    saved_personal_events_by_user = json.load(file)
  with open('user_matches.json') as file:
    user_matches = json.load(file)
  ## iterate through active google users
  for user_email in user_matches.keys():
    if not user_matches[user_email]['active']: continue

    ## DEBUG
    # if user_email != 'sam@amstillroofing.com': continue

    ## get events for this user from google
    user_personal_events = find_personal_events(user_email)

    ## compare events from google with saved events for user
    for event in user_personal_events:
      event_found = False
      found_event = None
      for saved_event in saved_personal_events_by_user[user_email]:
        if event['google_id'] == saved_event['google_id']:
          event_found = True
          found_event = saved_event
          break

      ## if event isn't already saved, create the event in ST (function also saves event to file)
      if not event_found:
        print(f'Creating event in ServiceTitan and saving to file')
        st_id = create_new_non_job_event(event, access_token)
        ## Save event to saved_personal_events with st_id
        event['servicetitan_id'] = st_id
        print(event)
        saved_personal_events_by_user[user_email].append(event)
        
      ## if event is saved, check datetime to see if it changed, and if so, change in ST (function also updates saved event)
      elif not event['all_day']:
        if event['start_dateTime'] != found_event['start_dateTime'] or event['end_dateTime'] != found_event['end_dateTime']:
          print(f'Updating event in ServiceTitan and saving to file')
          tech_id = user_matches[user_email]['servicetitan_id']
          event['servicetitan_id'] = found_event['servicetitan_id']
          update_non_job_event(event, tech_id, access_token)
          found_event["summary"] = event['summary']
          found_event["start_dateTime"] = event['start_dateTime']
          found_event["end_dateTime"] = event['end_dateTime']

      elif event['all_day']:
        if event['start_date'] != found_event['start_date'] or event['end_date'] != found_event['end_date']:
          print(f'Updating event in ServiceTitan and saving to file')
          tech_id = user_matches[user_email]['servicetitan_id']
          event['servicetitan_id'] = found_event['servicetitan_id']
          update_non_job_event(event, tech_id, access_token)
          found_event["summary"] = event['summary']
          found_event["start_date"] = event['start_date']
          found_event["end_date"] = event['end_date']
      
      else:
        print(f'event already saved {event['summary']}') ## DEBUG
    
    ## delete saved events that are no longer in google (have been deleted in google)
    valid_ids = [event['google_id'] for event in user_personal_events]
    st_events_to_delete = [event for event in saved_personal_events_by_user[user_email] if event['google_id'] not in valid_ids]
    saved_personal_events_by_user[user_email] = [event for event in saved_personal_events_by_user[user_email] if event['google_id'] in valid_ids]

    for event in st_events_to_delete:
      print(f'Deleting ST event {event['servicetitan_id']}')
      delete_non_job_event(event, access_token)
    
  print('Saving personal events to file')
  with open('personal_events_by_user.json', 'w') as file:
    json.dump(saved_personal_events_by_user, file, indent=2)

## ServiceTitan functions
def login_to_st():
  print("Logging in to ServiceTitan...")

  # Production Endpoint
  url = "https://auth.servicetitan.io/connect/token"

  payload = f"grant_type=client_credentials&client_id={client_id}&client_secret={client_secret}"
  headers = {"Content-Type": "application/x-www-form-urlencoded"}

  response = requests.request("POST", url, data=payload, headers=headers)
  access_token = response.json()['access_token']

  print("Logged in!")

  return access_token

def get_st_technicians(access_token):
  print('Getting ServiceTitan Technicians')
  url = f'https://api.servicetitan.io/settings/v2/tenant/{st_tenant_id}/technicians'

  headers = {
    'Authorization': access_token,
    'ST-App-Key': servicetitan_api_key
  }
  technicians = []
  offset = 0
  limit = 100

  while True:
    try:
      response = requests.get(f"{url}?offset={offset}&limit={limit}", headers=headers)
      # response = requests.request("GET", f'{url}?offset={offset}&limit={limit}', data={}, headers=headers)
      # print(response.text)
      if response.status_code != 200:
        raise Exception(f"ServiceTitan API error: {response.status_code} - {response.text}")

      data = response.json()
      technicians.extend(data.get('data', []))

      if len(data.get('data', [])) < limit:
        break
      offset += limit
    except SSLError as e:
      print('---------------')
      print("SSL error occurred:", e)
    except Exception as e:
      print('---------------')
      print("An unexpected error occurred:", e)
  
  techs = {}
  for tech in technicians:
    new_tech = {
      'id': tech['id'],
      'userId': tech['userId'],
      'name': tech['name'],
      'email': tech['email'],
      'active': tech['active']
    }
    techs[tech['id']] = new_tech

  return techs

## Gets technicians from ServiceTitan, updates st_techs.json file
def update_st_technicians(access_token):
  ## Get list of technicians from ServiceTitan
  print("Updating ServiceTitan technicians...")
  st_techs = get_st_technicians(access_token)
  with open('st_techs.json', 'w') as file:
    json.dump(st_techs, file, indent=2)

  return st_techs

## Creates a non-job event in ServiceTitan and saves the new event to the personal_events_by_user.json file
def create_new_non_job_event(personal_event, access_token):
  ## TODO: figure out how to deal with all day events
  print('Creating non-job event in ServiceTitan')
  user_email = personal_event['google_email']
  url = f"https://api.servicetitan.io/dispatch/v2/tenant/{st_tenant_id}/non-job-appointments"
  headers = {
    "Authorization": access_token, 
    "ST-App-Key": servicetitan_api_key
  }

  with open('user_matches.json', 'r') as file:
    user_matches = json.load(file)
    tech_id = user_matches[user_email]['servicetitan_id']

  start = datetime.fromisoformat(personal_event['start_dateTime'])
  end = datetime.fromisoformat(personal_event['end_dateTime'])
  duration = end - start
  payload = {
    "technicianId": tech_id,
    "start": start,
    "duration": duration,
    "name": personal_event['summary'],
    "summary": personal_event['description'],
    "removeTechnicianFromCapacityPlanning": True
  }
  response = requests.request("POST", url, data=payload, headers=headers)
  print(f'ST event created: {response.json()['id']}')
  return response.json()['id']

## Updates non-job event with new data
def update_non_job_event(personal_event, tech_id, access_token):
  ## TODO: figure out how to deal with all day events
  print('Updating non-job event in ServiceTitan')
  user_email = personal_event['google_email']
  st_event_id = personal_event['servicetitan_id']
  url = f"https://api.servicetitan.io/dispatch/v2/tenant/{st_tenant_id}/non-job-appointments/{st_event_id}"
  headers = {
    "Authorization": access_token, 
    "ST-App-Key": servicetitan_api_key
  }

  start = datetime.fromisoformat(personal_event['start_dateTime'])
  end = datetime.fromisoformat(personal_event['end_dateTime'])
  duration = end - start
  payload = {
    "technicianId": tech_id,
    "start": start,
    "duration": duration,
    "name": personal_event['summary'],
    "summary": personal_event['description'],
    "removeTechnicianFromCapacityPlanning": True
  }
  response = requests.request("PUT", url, data=payload, headers=headers)

  ## update event
  with open('personal_events_by_user.json', 'r') as file:
    personal_events_by_user = json.load(file)
    for event in personal_events_by_user[user_email]:
      if event['servicetitan_id'] == st_event_id:
        event['summary'] = personal_event['summary']
        event['start_dateTime'] = personal_event['start_dateTime']
        event['end_dateTime'] = personal_event['end_dateTime']
        break
  with open('personal_events_by_user.json', 'w') as file:
    json.dump(personal_events_by_user, file, indent=2)

  print('--------------')
  print(json.dumps(response.json(), indent=2))
  print('--------------')
  print(f'ST event updated: {response.json()['id']}')
  # print(json.dumps(response.json(), indent=2))
  return response.json()['id']

## Deletes specified non-job event 
def delete_non_job_event(personal_event, access_token):
  print('Deleting non-job event in ServiceTitan')
  user_email = personal_event['google_email']
  st_event_id = personal_event['servicetitan_id']
  url = f"https://api.servicetitan.io/dispatch/v2/tenant/{st_tenant_id}/non-job-appointments/{st_event_id}"
  headers = {
    "Authorization": access_token, 
    "ST-App-Key": servicetitan_api_key
  }

  response = requests.request("DELETE", url, headers=headers)

  ## delete event in file
  with open('personal_events_by_user.json', 'r') as file:
    personal_events_by_user = json.load(file)
    personal_events = personal_events_by_user[user_email]
    personal_events = [event for event in personal_events if event['servicetitan_id'] != st_event_id]
    personal_events_by_user[user_email] = personal_events
    
  with open('personal_events_by_user.json', 'w') as file:
    json.dump(personal_events_by_user, file, indent=2)

  print(f'ST event deleted: {st_event_id}')
  # print(json.dumps(response.json(), indent=2))

## Matches Google users and ServiceTitan technicians
def match_users_and_techs(google_users, technicians):
  print('Matching Google users with ServiceTitan technicians')
  matches = {}

  for user_email in google_users.keys():
    g_email = user_email.lower().strip()
    g_name = google_users[user_email]['name'].lower().strip()
    alias_email = g_email.split('@')
    alias_email = alias_email[0] + '+1@' + alias_email[1]

    for tech_id in technicians.keys():
      t_email = technicians[tech_id]['email']
      if t_email:
        t_email = t_email.lower().strip()
      t_name = technicians[tech_id]['name'].lower().strip()

      if g_email == t_email or alias_email == t_email or g_name in t_name:
        matches[g_email] = {
          'google_id': google_users[user_email]['id'],
          'google_email': g_email,
          'google_name': g_name,
          'servicetitan_id': tech_id,
          'servicetitan_name': t_name,
          'servicetitan_email': t_email,
          'active': google_users[user_email]['active'] and technicians[tech_id]['active']
        }
        break  # stop at first match for each Google user

  return matches

## Debug function to find which users don't match between Google and ServiceTitan
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

## --- MAIN SCRIPT ---
if __name__ == "__main__":
  access_token = login_to_st()
  setup(access_token)

  ## Login and find new or updated events every 15 minutes
  while True:
    ## Get and print the current time
    current_time = datetime.now()
    print("Current time:", current_time.strftime("%Y-%m-%d %H:%M:%S"))

    access_token = login_to_st()
    google_users = update_google_users()
    st_techs = update_st_technicians(access_token)
    match_users_and_techs(google_users, st_techs)
    find_and_add_or_update_events(access_token)
  
    if TESTING:
      sleep_time = 10 ## 30 seconds, for testing purposes
    else:
      sleep_time = 15 * 60 ## 15 minutes, for production
    print(f'Sleeping for {sleep_time} seconds')
    print('-------------------------')
    time.sleep(sleep_time)


  ## TEST Create new non-job event in ST
  # print(f'Creating event in ServiceTitan and saving to file')
  # user_email = 'colton@amstillroofing.com'
  # new_event = {
  #   "google_id": "_88q38c9k64r48ba388s44b9k6533cba270r30ba26gqk6dq384sj0cpm8o",
  #   "google_email": user_email,
  #   "created": "2025-06-17T03:59:35.000Z",
  #   "updated": "2025-06-17T03:59:35.693Z",
  #   "creator_email": user_email,
  #   "organizer_email": user_email,
  #   "summary": "TEST",
  #   "description": "this is a test",
  #   "start_dateTime": "2025-06-01T13:00:00-06:00",
  #   "end_dateTime": "2025-06-01T14:00:00-06:00",
  #   "time_zone": "UTC"
  # }
  # st_event_id = create_new_non_job_event(new_event, access_token)
  # ## Save event to saved_personal_events with st_id
  # new_event['servicetitan_id'] = st_event_id
  # ## DEBUG
  # print('ServiceTitan ID')
  # print(st_event_id)
  # print('saving personal events to file')
  # with open('personal_events_by_user.json', 'r') as file:
  #   saved_personal_events_by_user = json.load(file)
  # saved_personal_events_by_user[user_email].append(new_event)
  # with open('personal_events_by_user.json', 'w') as file:
  #   json.dump(saved_personal_events_by_user, file, indent=2)