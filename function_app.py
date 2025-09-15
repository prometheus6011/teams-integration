import json
import os
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import jwt
import requests
from azure.functions import FunctionApp, HttpRequest, HttpResponse
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from msal import ConfidentialClientApplication

app = FunctionApp()

# Configuration
CLIENT_ID = os.environ.get('AZURE_CLIENT_ID')
CLIENT_SECRET = os.environ.get('AZURE_CLIENT_SECRET')
TENANT_ID = os.environ.get('AZURE_TENANT_ID')
REDIRECT_URI = os.environ.get('REDIRECT_URI', 'https://your-function-app.azurewebsites.net/api/oauth/callback')
STORAGE_ACCOUNT_URL = os.environ.get('STORAGE_ACCOUNT_URL')
JWT_SECRET = os.environ.get('JWT_SECRET', 'your-jwt-secret')

# Microsoft Graph scopes for Teams and SharePoint
SCOPES = [
    'https://graph.microsoft.com/Sites.Read.All',
    'https://graph.microsoft.com/Files.Read.All',
    'https://graph.microsoft.com/Chat.Read',
    'https://graph.microsoft.com/ChannelMessage.Read.All',
    'https://graph.microsoft.com/Team.ReadBasic.All'
]

# Initialize MSAL app
msal_app = ConfidentialClientApplication(
    CLIENT_ID,
    authority=f"https://login.microsoftonline.com/{TENANT_ID}",
    client_credential=CLIENT_SECRET,
)

# Initialize blob service client
credential = DefaultAzureCredential()
blob_service_client = BlobServiceClient(account_url=STORAGE_ACCOUNT_URL, credential=credential)


class ClientManager:
    @staticmethod
    def create_client_token(client_id: str, client_name: str) -> str:
        payload = {
            'client_id': client_id,
            'client_name': client_name,
            'created_at': datetime.utcnow().isoformat(),
            'exp': datetime.utcnow() + timedelta(days=365)
        }
        return jwt.encode(payload, JWT_SECRET, algorithm='HS256')

    @staticmethod
    def verify_client_token(token: str) -> Optional[Dict[str, Any]]:
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
            return payload
        except jwt.ExpiredSignatureError:
            return None
        except jwt.InvalidTokenError:
            return None


@app.route(route="health", methods=["GET"])
def health_check(req: HttpRequest) -> HttpResponse:
    return HttpResponse("OK", status_code=200)


@app.route(route="onboard/initiate", methods=["POST"])
def initiate_onboarding(req: HttpRequest) -> HttpResponse:
    """Initiate client onboarding by generating OAuth authorization URL"""
    try:
        req_json = req.get_json()
        client_name = req_json.get('client_name')
        client_email = req_json.get('client_email')

        if not client_name or not client_email:
            return HttpResponse(
                json.dumps({"error": "client_name and client_email are required"}),
                status_code=400,
                mimetype="application/json"
            )

        # Generate state parameter with client info
        state = {
            'client_name': client_name,
            'client_email': client_email,
            'timestamp': datetime.utcnow().isoformat()
        }
        state_token = jwt.encode(state, JWT_SECRET, algorithm='HS256')

        # Generate authorization URL
        auth_url = msal_app.get_authorization_request_url(
            scopes=SCOPES,
            state=state_token,
            redirect_uri=REDIRECT_URI
        )

        return HttpResponse(
            json.dumps({
                "authorization_url": auth_url,
                "state": state_token,
                "message": "Please visit the authorization URL to grant permissions"
            }),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"Error initiating onboarding: {str(e)}")
        return HttpResponse(
            json.dumps({"error": "Failed to initiate onboarding"}),
            status_code=500,
            mimetype="application/json"
        )


@app.route(route="oauth/callback", methods=["GET", "POST"])
def oauth_callback(req: HttpRequest) -> HttpResponse:
    """Handle OAuth callback and complete client onboarding"""
    try:
        code = req.params.get('code')
        state = req.params.get('state')
        error = req.params.get('error')

        if error:
            return HttpResponse(
                json.dumps({"error": f"OAuth error: {error}"}),
                status_code=400,
                mimetype="application/json"
            )

        if not code or not state:
            return HttpResponse(
                json.dumps({"error": "Missing authorization code or state"}),
                status_code=400,
                mimetype="application/json"
            )

        # Verify state token
        try:
            state_data = jwt.decode(state, JWT_SECRET, algorithms=['HS256'])
        except jwt.InvalidTokenError:
            return HttpResponse(
                json.dumps({"error": "Invalid state token"}),
                status_code=400,
                mimetype="application/json"
            )

        # Exchange code for tokens
        result = msal_app.acquire_token_by_authorization_code(
            code,
            scopes=SCOPES,
            redirect_uri=REDIRECT_URI
        )

        if "error" in result:
            return HttpResponse(
                json.dumps({"error": f"Token acquisition failed: {result.get('error_description')}"}),
                status_code=400,
                mimetype="application/json"
            )

        # Create client record
        client_id = f"client_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        client_token = ClientManager.create_client_token(client_id, state_data['client_name'])

        # Store tokens securely (in production, use Azure Key Vault)
        client_data = {
            'client_id': client_id,
            'client_name': state_data['client_name'],
            'client_email': state_data['client_email'],
            'access_token': result['access_token'],
            'refresh_token': result.get('refresh_token'),
            'token_expires_at': (datetime.utcnow() + timedelta(seconds=result['expires_in'])).isoformat(),
            'onboarded_at': datetime.utcnow().isoformat(),
            'status': 'active'
        }

        # Store client data in blob storage
        try:
            blob_client = blob_service_client.get_blob_client(
                container=f"mcp-{client_id}",
                blob="secrets.json"
            )
            blob_client.upload_blob(json.dumps(client_data), overwrite=True)
        except Exception as e:
            logging.error(f"Failed to store client data: {str(e)}")

        return HttpResponse(
            json.dumps({
                "message": "Client onboarded successfully",
                "client_id": client_id,
                "client_token": client_token,
                "status": "active"
            }),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"Error in OAuth callback: {str(e)}")
        return HttpResponse(
            json.dumps({"error": "Failed to complete onboarding"}),
            status_code=500,
            mimetype="application/json"
        )


@app.route(route="client/{client_id}/teams/scrape", methods=["POST"])
def scrape_teams_data(req: HttpRequest) -> HttpResponse:
    """Scrape Microsoft Teams data for a client"""
    try:
        client_id = req.route_params.get('client_id')
        auth_header = req.headers.get('Authorization')

        if not auth_header or not auth_header.startswith('Bearer '):
            return HttpResponse(
                json.dumps({"error": "Missing or invalid authorization header"}),
                status_code=401,
                mimetype="application/json"
            )

        token = auth_header[7:]
        client_info = ClientManager.verify_client_token(token)

        if not client_info or client_info.get('client_id') != client_id:
            return HttpResponse(
                json.dumps({"error": "Invalid or expired token"}),
                status_code=401,
                mimetype="application/json"
            )

        # Get client data
        try:
            blob_client = blob_service_client.get_blob_client(
                container=f"mcp-{client_id}",
                blob="secrets.json"
            )
            client_data = json.loads(blob_client.download_blob().readall().decode())
        except Exception:
            return HttpResponse(
                json.dumps({"error": "Client not found"}),
                status_code=404,
                mimetype="application/json"
            )

        access_token = client_data['access_token']

        # Scrape Teams data
        teams_data = []

        # Get all teams
        teams_response = requests.get(
            'https://graph.microsoft.com/v1.0/me/joinedTeams',
            headers={'Authorization': f'Bearer {access_token}'}
        )

        if teams_response.status_code == 200:
            teams = teams_response.json().get('value', [])

            for team in teams:
                team_id = team['id']
                team_name = team['displayName']

                # Get channels for this team
                channels_response = requests.get(
                    f'https://graph.microsoft.com/v1.0/teams/{team_id}/channels',
                    headers={'Authorization': f'Bearer {access_token}'}
                )

                if channels_response.status_code == 200:
                    channels = channels_response.json().get('value', [])

                    for channel in channels:
                        channel_id = channel['id']
                        channel_name = channel['displayName']

                        # Get messages from this channel
                        messages_response = requests.get(
                            f'https://graph.microsoft.com/v1.0/teams/{team_id}/channels/{channel_id}/messages',
                            headers={'Authorization': f'Bearer {access_token}'}
                        )

                        if messages_response.status_code == 200:
                            messages = messages_response.json().get('value', [])

                            for message in messages:
                                teams_data.append({
                                    'type': 'teams_message',
                                    'team_name': team_name,
                                    'channel_name': channel_name,
                                    'message_id': message['id'],
                                    'content': message.get('body', {}).get('content', ''),
                                    'created_at': message.get('createdDateTime'),
                                    'author': message.get('from', {}).get('user', {}).get('displayName', 'Unknown')
                                })

        # Store scraped data as events
        current_date = datetime.utcnow()
        events_data = {
            'tenant_id': client_id,
            'event_type': 'teams_scrape',
            'scraped_at': current_date.isoformat(),
            'events': teams_data
        }

        # Store in blob storage organized by date
        date_folder = f"dt={current_date.strftime('%Y-%m-%d')}"
        kb_blob_client = blob_service_client.get_blob_client(
            container=f"mcp-{client_id}",
            blob=f"{date_folder}/teams_events_{current_date.strftime('%H%M%S')}.json"
        )
        kb_blob_client.upload_blob(json.dumps(events_data), overwrite=True)

        return HttpResponse(
            json.dumps({
                "message": "Teams data scraped successfully",
                "records_count": len(teams_data),
                "storage_path": f"{date_folder}/teams_events_{current_date.strftime('%H%M%S')}.json"
            }),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"Error scraping Teams data: {str(e)}")
        return HttpResponse(
            json.dumps({"error": "Failed to scrape Teams data"}),
            status_code=500,
            mimetype="application/json"
        )


@app.route(route="client/{client_id}/sharepoint/scrape", methods=["POST"])
def scrape_sharepoint_data(req: HttpRequest) -> HttpResponse:
    """Scrape Microsoft SharePoint data for a client"""
    try:
        client_id = req.route_params.get('client_id')
        auth_header = req.headers.get('Authorization')

        if not auth_header or not auth_header.startswith('Bearer '):
            return HttpResponse(
                json.dumps({"error": "Missing or invalid authorization header"}),
                status_code=401,
                mimetype="application/json"
            )

        token = auth_header[7:]
        client_info = ClientManager.verify_client_token(token)

        if not client_info or client_info.get('client_id') != client_id:
            return HttpResponse(
                json.dumps({"error": "Invalid or expired token"}),
                status_code=401,
                mimetype="application/json"
            )

        # Get client data
        try:
            blob_client = blob_service_client.get_blob_client(
                container=f"mcp-{client_id}",
                blob="secrets.json"
            )
            client_data = json.loads(blob_client.download_blob().readall().decode())
        except Exception:
            return HttpResponse(
                json.dumps({"error": "Client not found"}),
                status_code=404,
                mimetype="application/json"
            )

        access_token = client_data['access_token']

        # Scrape SharePoint data
        sharepoint_data = []

        # Get all sites
        sites_response = requests.get(
            'https://graph.microsoft.com/v1.0/sites?search=*',
            headers={'Authorization': f'Bearer {access_token}'}
        )

        if sites_response.status_code == 200:
            sites = sites_response.json().get('value', [])

            for site in sites:
                site_id = site['id']
                site_name = site.get('displayName', site.get('name', 'Unknown'))

                # Get document libraries
                libraries_response = requests.get(
                    f'https://graph.microsoft.com/v1.0/sites/{site_id}/drives',
                    headers={'Authorization': f'Bearer {access_token}'}
                )

                if libraries_response.status_code == 200:
                    libraries = libraries_response.json().get('value', [])

                    for library in libraries:
                        drive_id = library['id']
                        library_name = library.get('name', 'Unknown')

                        # Get files from this library
                        files_response = requests.get(
                            f'https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children',
                            headers={'Authorization': f'Bearer {access_token}'}
                        )

                        if files_response.status_code == 200:
                            files = files_response.json().get('value', [])

                            for file in files:
                                if file.get('file'):  # Only process files, not folders
                                    sharepoint_data.append({
                                        'type': 'sharepoint_file',
                                        'site_name': site_name,
                                        'library_name': library_name,
                                        'file_id': file['id'],
                                        'file_name': file['name'],
                                        'file_size': file.get('size', 0),
                                        'created_at': file.get('createdDateTime'),
                                        'modified_at': file.get('lastModifiedDateTime'),
                                        'download_url': file.get('@microsoft.graph.downloadUrl')
                                    })

        # Store scraped data as events
        current_date = datetime.utcnow()
        events_data = {
            'tenant_id': client_id,
            'event_type': 'sharepoint_scrape',
            'scraped_at': current_date.isoformat(),
            'events': sharepoint_data
        }

        # Store in blob storage organized by date
        date_folder = f"dt={current_date.strftime('%Y-%m-%d')}"
        kb_blob_client = blob_service_client.get_blob_client(
            container=f"mcp-{client_id}",
            blob=f"{date_folder}/sharepoint_events_{current_date.strftime('%H%M%S')}.json"
        )
        kb_blob_client.upload_blob(json.dumps(events_data), overwrite=True)

        return HttpResponse(
            json.dumps({
                "message": "SharePoint data scraped successfully",
                "records_count": len(sharepoint_data),
                "storage_path": f"{date_folder}/sharepoint_events_{current_date.strftime('%H%M%S')}.json"
            }),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"Error scraping SharePoint data: {str(e)}")
        return HttpResponse(
            json.dumps({"error": "Failed to scrape SharePoint data"}),
            status_code=500,
            mimetype="application/json"
        )


@app.route(route="client/{client_id}/status", methods=["GET"])
def get_client_status(req: HttpRequest) -> HttpResponse:
    """Get client onboarding and data scraping status"""
    try:
        client_id = req.route_params.get('client_id')
        auth_header = req.headers.get('Authorization')

        if not auth_header or not auth_header.startswith('Bearer '):
            return HttpResponse(
                json.dumps({"error": "Missing or invalid authorization header"}),
                status_code=401,
                mimetype="application/json"
            )

        token = auth_header[7:]
        client_info = ClientManager.verify_client_token(token)

        if not client_info or client_info.get('client_id') != client_id:
            return HttpResponse(
                json.dumps({"error": "Invalid or expired token"}),
                status_code=401,
                mimetype="application/json"
            )

        # Get client data
        try:
            blob_client = blob_service_client.get_blob_client(
                container=f"mcp-{client_id}",
                blob="secrets.json"
            )
            client_data = json.loads(blob_client.download_blob().readall().decode())
        except Exception:
            return HttpResponse(
                json.dumps({"error": "Client not found"}),
                status_code=404,
                mimetype="application/json"
            )

        # Get tenant events data count
        event_blobs = blob_service_client.get_container_client(f"mcp-{client_id}").list_blobs()

        event_files = list(event_blobs)
        teams_files = [f for f in event_files if 'teams_events_' in f.name]
        sharepoint_files = [f for f in event_files if 'sharepoint_events_' in f.name]

        return HttpResponse(
            json.dumps({
                "client_id": client_id,
                "client_name": client_data.get('client_name'),
                "status": client_data.get('status'),
                "onboarded_at": client_data.get('onboarded_at'),
                "events": {
                    "total_files": len(event_files),
                    "teams_events": len(teams_files),
                    "sharepoint_events": len(sharepoint_files)
                },
                "last_scrape": {
                    "teams": teams_files[-1].last_modified.isoformat() if teams_files else None,
                    "sharepoint": sharepoint_files[-1].last_modified.isoformat() if sharepoint_files else None
                }
            }),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"Error getting client status: {str(e)}")
        return HttpResponse(
            json.dumps({"error": "Failed to get client status"}),
            status_code=500,
            mimetype="application/json"
        )


@app.route(route="clients", methods=["GET"])
def list_clients(req: HttpRequest) -> HttpResponse:
    """List all onboarded clients (admin endpoint)"""
    try:
        # In production, add admin authentication here

        clients = []

        # List all containers that start with 'mcp-'
        containers = blob_service_client.list_containers(name_starts_with="mcp-")

        for container in containers:
            try:
                blob_client = blob_service_client.get_blob_client(
                    container=container.name,
                    blob="secrets.json"
                )
                client_data = json.loads(blob_client.download_blob().readall().decode())

                clients.append({
                    "client_id": client_data.get('client_id'),
                    "client_name": client_data.get('client_name'),
                    "client_email": client_data.get('client_email'),
                    "status": client_data.get('status'),
                    "onboarded_at": client_data.get('onboarded_at')
                })
            except Exception:
                # Skip containers without secrets.json
                continue

        return HttpResponse(
            json.dumps({
                "clients": clients,
                "total_count": len(clients)
            }),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"Error listing clients: {str(e)}")
        return HttpResponse(
            json.dumps({"error": "Failed to list clients"}),
            status_code=500,
            mimetype="application/json"
        )