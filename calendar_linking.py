import requests
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# --- CONFIGURATION ---

# Google
GOOGLE_SCOPES = ['https://www.googleapis.com/auth/admin.directory.user.readonly']
GOOGLE_CREDENTIALS_FILE = 'credentials.json'

# ServiceTitan
SERVICETITAN_API_URL = 'https://api.servicetitan.io/settings/v2/tenant/4160781343/technicians'

# --- FUNCTIONS ---

def get_google_users():
  google_admin_user = os.getenv("GOOGLE_ADMIN_USER")

  credentials = service_account.Credentials.from_service_account_file(
    GOOGLE_CREDENTIALS_FILE,
    scopes=GOOGLE_SCOPES,
    subject=google_admin_user
  )
  service = build('admin', 'directory_v1', credentials=credentials)
  results = service.users().list(customer='my_customer', maxResults=200, orderBy='email').execute()
  return results.get('users', [])

def get_calendars():
  google_admin_user = os.getenv("GOOGLE_ADMIN_USER")

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

def get_calendar_events(calendar_id):
  scopes = ['https://www.googleapis.com/auth/calendar.readonly']
  credentials = service_account.Credentials.from_service_account_file(
    GOOGLE_CREDENTIALS_FILE,
    scopes=scopes,
    # subject=google_admin_user
  )
  service = build('calendar', 'v3', credentials=credentials)

  events = []
  page_token = None

  while True:
    response = service.events().list(
      calendarId=calendar_id,
      pageToken=page_token,
      maxResults=2500,  # max allowed
      singleEvents=True,
      orderBy='startTime'
    ).execute()

    events.extend(response.get('items', []))
    page_token = response.get('nextPageToken')
    if not page_token:
      break

  return events

def get_servicetitan_technicians():
  # Access variables
  client_id = os.getenv("CLIENT_ID")
  client_secret = os.getenv("CLIENT_SECRET")
  servicetitan_api_key = os.getenv("SERVICETITAN_API_KEY")

  token_url = 'https://auth.servicetitan.io/connect/token'

  data = {
    'grant_type': 'client_credentials',
    'client_id': client_id,
    'client_secret': client_secret,
    'scope': 'api'
  }

  response = requests.post(token_url, data=data)
  ## DEBUG
  print('-------------------------')
  print(json.dumps(response.json(), indent=2))
  print('-------------------------')

  access_token = response.json()['access_token']

  url = f"{SERVICETITAN_API_URL}/technicians"
  headers = {
    'Authorization': f'Bearer {access_token}',
    'ST-App-Key': servicetitan_api_key,
    'Content-Type': 'application/json'
  }
  technicians = []
  offset = 0
  limit = 100

  while True:
    response = requests.get(f"{url}?offset={offset}&limit={limit}", headers=headers)
    if response.status_code != 200:
      raise Exception(f"ServiceTitan API error: {response.status_code} - {response.text}")

    data = response.json()
    technicians.extend(data.get('data', []))

    if len(data.get('data', [])) < limit:
      break
    offset += limit

  return technicians

def match_users_and_techs(google_users, technicians):
  matches = []

  for user in google_users:
    g_email = user.get('primaryEmail', '').lower()
    g_name = f"{user.get('name', {}).get('givenName', '')} {user.get('name', {}).get('familyName', '')}".strip().lower()

    for tech in technicians:
      t_email = tech.get('email', '').lower()
      t_name = tech.get('name', '').lower()

      if g_email == t_email or g_name in t_name:
        matches.append({
          'google_email': g_email,
          'google_name': g_name,
          'servicetitan_id': tech.get('id'),
          'servicetitan_name': t_name,
          'servicetitan_email': t_email,
        })
        break  # stop at first match for each Google user

  return matches

# --- MAIN SCRIPT ---

if __name__ == "__main__":
  ## Get list of Users from Google Workspace
  print("Fetching Google Workspace users...")
  google_users = get_google_users()
  n = 1
  for user in google_users:
    # print(f'Technician {n}: {user.google_name}')
    print(f'User {n}')
    print(user['name']['fullName'])
    print(user['primaryEmail'])
    print(user['id'])
    print('--------------------------')
    n += 1

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
  user_email = 'will@amstillroofing.com'
  calendar_service = get_user_calendar_service(user_email)
  events = calendar_service.events().list(calendarId=user_email).execute()
  n = 1
  for event in events['items']:
    # print(f'Technician {n}: {user.google_name}')
    print(f'Event {n}')
    print(event)
    # print(user['primaryEmail'])
    # print(user['id'])
    print('--------------------------')
    n += 1

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
  print("Fetching ServiceTitan technicians...")
  servicetitan_techs = get_servicetitan_technicians()
  n = 1
  for tech in servicetitan_techs:
    print(f'Technician {n}: {tech}')
    n += 1

  # print("Matching users...")
  # matches = match_users_and_techs(google_users, servicetitan_techs)

  # for match in matches:
  #   print(match)

  # print(f"\nTotal matches found: {len(matches)}")
