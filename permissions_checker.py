import os
import requests
import logging
import xml.etree.ElementTree as ET
from typing import Dict
from dotenv import load_dotenv
from dbt_tableau.tableau import authenticate_tableau

def check_user_role_and_permissions(
    tableau_server: str,
    site_id: str,
    token: str,
    api_version: str = "3.17"
) -> Dict:
    """
    Checks the current user's role and permissions using the users/current endpoint.
    """
    logger = logging.getLogger(__name__)
    tableau_server = tableau_server.rstrip('/')
    
    try:
        headers = {
            'X-Tableau-Auth': token,
            'Accept': 'application/xml'
        }
        
        # Use current user endpoint instead of whoami
        user_url = f"{tableau_server}/api/{api_version}/sites/{site_id}/users/current"
        logger.info(f"Checking user info at: {user_url}")
        
        response = requests.get(user_url, headers=headers)
        response.raise_for_status()
        
        # Log full response for debugging
        logger.debug(f"Response status: {response.status_code}")
        logger.debug(f"Response text: {response.text}")
        
        root = ET.fromstring(response.text)
        user = root.find(".//user")
        
        if user is not None:
            return {
                'name': user.get('name'),
                'site_role': user.get('siteRole'),
                'last_login': user.get('lastLogin'),
                'raw_response': response.text
            }
        else:
            return {
                'error': 'No user information found in response',
                'raw_response': response.text
            }
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Error response: {e.response.text}")
        return {'error': str(e)}
        
    except ET.ParseError as e:
        logger.error(f"XML parsing error: {str(e)}")
        return {'error': str(e)}
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return {'error': str(e)}

def check_metadata_api_access(
    tableau_server: str,
    site_id: str,
    token: str,
    api_version: str = "3.17"
) -> Dict:
    """
    Checks access to metadata API endpoints.
    """
    logger = logging.getLogger(__name__)
    tableau_server = tableau_server.rstrip('/')
    results = {}
    
    try:
        headers = {
            'X-Tableau-Auth': token,
            'Accept': 'application/xml'
        }
        
        # Test different metadata endpoints
        endpoints = {
            'tables': f"{tableau_server}/api/{api_version}/sites/{site_id}/tables",
            'databases': f"{tableau_server}/api/{api_version}/sites/{site_id}/databases",
            'metadata': f"{tableau_server}/api/{api_version}/sites/{site_id}/metadata"
        }
        
        for name, url in endpoints.items():
            try:
                logger.info(f"Checking {name} endpoint at: {url}")
                response = requests.get(url, headers=headers)
                results[name] = {
                    'status_code': response.status_code,
                    'has_access': response.status_code == 200
                }
            except Exception as e:
                results[name] = {'error': str(e)}
        
        return results
        
    except Exception as e:
        logger.error(f"Error checking metadata API access: {str(e)}")
        return {'error': str(e)}

def check_site_status(
    tableau_server: str,
    site_id: str,
    token: str,
    api_version: str = "3.17"
) -> Dict:
    """
    Checks site status and settings.
    """
    logger = logging.getLogger(__name__)
    tableau_server = tableau_server.rstrip('/')
    
    try:
        headers = {
            'X-Tableau-Auth': token,
            'Accept': 'application/xml'
        }
        
        site_url = f"{tableau_server}/api/{api_version}/sites/{site_id}"
        logger.info(f"Checking site status at: {site_url}")
        
        response = requests.get(site_url, headers=headers)
        response.raise_for_status()
        
        # Log full response for debugging
        logger.debug(f"Response status: {response.status_code}")
        logger.debug(f"Response text: {response.text}")
        
        root = ET.fromstring(response.text)
        site = root.find(".//site")
        
        if site is not None:
            return {
                'name': site.get('name'),
                'content_url': site.get('contentUrl'),
                'status': response.status_code,
                'raw_response': response.text
            }
        else:
            return {
                'error': 'No site information found in response',
                'raw_response': response.text
            }
            
    except Exception as e:
        logger.error(f"Error checking site status: {str(e)}")
        return {'error': str(e)}

def check_all_permissions():
    """
    Runs all permission checks using environment variables.
    """
    # Load environment variables and authenticate
    load_dotenv()
    
    tableau_server = os.getenv("TABLEAU_SERVER")
    tableau_site = os.getenv("TABLEAU_SITE")
    tableau_pat_name = os.getenv("TABLEAU_PAT_NAME")
    tableau_pat = os.getenv("TABLEAU_PAT")
    
    if not all([tableau_server, tableau_site, tableau_pat_name, tableau_pat]):
        print("Error: Missing required environment variables")
        return
    
    try:
        auth = authenticate_tableau(tableau_server, tableau_site, tableau_pat_name, tableau_pat)
    except Exception as e:
        print(f"Error authenticating: {str(e)}")
        return
    
    # Run checks
    results = {
        'user_info': check_user_role_and_permissions(
            tableau_server, 
            auth['site']['id'], 
            auth['token']
        ),
        'metadata_access': check_metadata_api_access(
            tableau_server, 
            auth['site']['id'], 
            auth['token']
        ),
        'site_status': check_site_status(
            tableau_server, 
            auth['site']['id'], 
            auth['token']
        )
    }
    
    # Print results
    print("\n=== Permission Check Results ===\n")
    
    # User Info
    print("User Information:")
    user_info = results['user_info']
    if 'error' not in user_info:
        print(f"  Name: {user_info.get('name')}")
        print(f"  Site Role: {user_info.get('site_role')}")
        print(f"  Last Login: {user_info.get('last_login')}")
    else:
        print(f"  Error: {user_info.get('error')}")
    
    # Metadata Access
    print("\nMetadata API Access:")
    metadata = results['metadata_access']
    if isinstance(metadata, dict) and 'error' not in metadata:
        for endpoint, info in metadata.items():
            print(f"  {endpoint.title()}:")
            print(f"    Status Code: {info.get('status_code')}")
            print(f"    Has Access: {info.get('has_access')}")
    else:
        print(f"  Error: {metadata.get('error')}")
    
    # Site Status
    print("\nSite Status:")
    site = results['site_status']
    if 'error' not in site:
        print(f"  Name: {site.get('name')}")
        print(f"  Content URL: {site.get('content_url')}")
        print(f"  Status: {site.get('status')}")
    else:
        print(f"  Error: {site.get('error')}")
    
    return results

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    results = check_all_permissions()
