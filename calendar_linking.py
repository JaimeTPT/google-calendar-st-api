import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build

# --- CONFIGURATION ---

# Google
GOOGLE_SCOPES = ['https://www.googleapis.com/auth/admin.directory.user.readonly']
GOOGLE_CREDENTIALS_FILE = 'credentials.json'
GOOGLE_ADMIN_USER = 'sam@amstillroofing.com'  # delegated admin

# ServiceTitan
SERVICETITAN_API_URL = 'https://api.servicetitan.io/v1'
SERVICETITAN_API_KEY = 'your_servicetitan_api_key'

# --- FUNCTIONS ---

def get_google_users():
  credentials = service_account.Credentials.from_service_account_file(
    GOOGLE_CREDENTIALS_FILE,
    scopes=GOOGLE_SCOPES,
    subject=GOOGLE_ADMIN_USER
  )
  service = build('admin', 'directory_v1', credentials=credentials)
  results = service.users().list(customer='my_customer', maxResults=200, orderBy='email').execute()
  return results.get('users', [])

def get_servicetitan_technicians():
  url = f"{SERVICETITAN_API_URL}/technicians"
  headers = {
    'Authorization': f'Bearer {SERVICETITAN_API_KEY}',
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
  print("Fetching Google Workspace users...")
  google_users = get_google_users()

  print("Fetching ServiceTitan technicians...")
  servicetitan_techs = get_servicetitan_technicians()

  print("Matching users...")
  matches = match_users_and_techs(google_users, servicetitan_techs)

  for match in matches:
    print(match)

  print(f"\nTotal matches found: {len(matches)}")
