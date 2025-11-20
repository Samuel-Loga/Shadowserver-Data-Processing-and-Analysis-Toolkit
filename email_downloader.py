import imaplib
import email
from email.header import decode_header
import os
import zipfile
import io
import re
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
IMAP_SERVER = "imap.gmail.com"
EMAIL_USER = os.getenv("SHADOWSERVER_EMAIL_USER", "")
EMAIL_PASS = os.getenv("SHADOWSERVER_EMAIL_PASS", "")

DOWNLOAD_FOLDER = r'd:\PD\shadow_intel_processor\src'

# CRITERIA: fetch unread emails from Shadowserver
SEARCH_CRITERIA = '(UNSEEN FROM "enter the email address here")'

def clean_filename(filename):
    return re.sub(r'[\\/*?:"<>|]', "", filename)

def download_file_from_url(url, target_dir, log_callback):
    """
    Downloads a file from a Shadowserver URL, handling filenames and zips.
    """
    try:
        # Stream the request to check headers first
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            
            # Try to get the real filename from the Content-Disposition header
            filename = ""
            if "Content-Disposition" in r.headers:
                cd = r.headers["Content-Disposition"]
                filenames = re.findall(r'filename="?([^"]+)"?', cd)
                if filenames:
                    filename = filenames[0]
            
            # Fallback if no filename in headers
            if not filename:
                filename = url.split("/")[-1]
                if not "." in filename: 
                    filename += ".zip" # Safe guess for Shadowserver
            
            filename = clean_filename(filename)
            filepath = os.path.join(target_dir, filename)

            # Skip if already downloaded
            if os.path.exists(filepath):
                return 0

            # Download the content
            file_content = r.content

            # Handle Zip files
            if filename.endswith(".zip"):
                try:
                    with zipfile.ZipFile(io.BytesIO(file_content)) as z:
                        z.extractall(target_dir)
                        log_callback(f"  Link Processed: Extracted {filename}")
                        return 1
                except zipfile.BadZipFile:
                    log_callback(f"  Bad Zip from link: {filename}", "error")
                    return 0
            else:
                # Save regular file
                with open(filepath, "wb") as f:
                    f.write(file_content)
                log_callback(f"  Link Downloaded: {filename}")
                return 1

    except Exception as e:
        log_callback(f"  ❌ Failed to download link {url}: {e}", "error")
        return 0

def download_shadowserver_reports(username, password, server=IMAP_SERVER, target_dir=DOWNLOAD_FOLDER, log_callback=print):
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    try:
        log_callback(f"Connecting to {server}...")
        mail = imaplib.IMAP4_SSL(server)
        mail.login(username, password)
        mail.select("inbox")

        log_callback("Searching for NEW (unopened) emails...")
        status, messages = mail.search(None, SEARCH_CRITERIA)
        
        email_ids = messages[0].split()
        if not email_ids:
            log_callback("No new unread Shadowserver emails found.")
            return

        log_callback(f"Found {len(email_ids)} unread emails. Scanning for files & links...")
        
        count = 0
        processed_urls = set()

        # Process latest 20 emails to avoid taking too long (Adjust as needed)
        for email_id in reversed(email_ids[-20:]):
            res, msg = mail.fetch(email_id, "(RFC822)")
            for response in msg:
                if isinstance(response, tuple):
                    msg = email.message_from_bytes(response[1])
                    subject, encoding = decode_header(msg["Subject"])[0]
                    if isinstance(subject, bytes):
                        subject = subject.decode(encoding if encoding else "utf-8")
                    
                    # 1. Walk through parts to find Attachments OR Text Body
                    for part in msg.walk():
                        content_type = part.get_content_type()
                        content_disposition = str(part.get("Content-Disposition"))
                        
                        # A) Handle Direct Attachments
                        if "attachment" in content_disposition:
                            filename = part.get_filename()
                            if filename:
                                filename = clean_filename(filename)
                                if filename.endswith(".csv") or filename.endswith(".zip"):
                                    filepath = os.path.join(target_dir, filename)
                                    if not os.path.exists(filepath):
                                        payload = part.get_payload(decode=True)
                                        if filename.endswith(".zip"):
                                            try:
                                                with zipfile.ZipFile(io.BytesIO(payload)) as z:
                                                    z.extractall(target_dir)
                                                    log_callback(f"  Attachment Extracted: {filename}")
                                                    count += 1
                                            except: pass
                                        else:
                                            with open(filepath, "wb") as f:
                                                f.write(payload)
                                            log_callback(f"  Attachment Saved: {filename}")
                                            count += 1

                        # B) Handle Links in Text Body
                        elif content_type == "text/plain":
                            try:
                                body = part.get_payload(decode=True).decode('utf-8', errors='ignore')
                                # Regex to find Shadowserver DL links
                                found_urls = re.findall(r'(https://dl\.shadowserver\.org/\S+)', body)
                                
                                for url in found_urls:
                                    # Clean trailing punctuation often found in emails
                                    url = url.rstrip('.,)>]')
                                    
                                    if url not in processed_urls:
                                        processed_urls.add(url)
                                        count += download_file_from_url(url, target_dir, log_callback)
                            except Exception as e:
                                log_callback(f"Error parsing text body: {e}", "error")

        mail.close()
        mail.logout()
        
        if count > 0:
            log_callback(f"Success! {count} new report files added to src.", "success")
        else:
            log_callback("Scan complete. No new reports found.", "info")

    except imaplib.IMAP4.error as e:
        log_callback(f"IMAP Error: {e}. Check credentials.", "error")
    except Exception as e:
        log_callback(f"Unexpected error: {e}", "error")

if __name__ == "__main__":
    import getpass
    u = EMAIL_USER if EMAIL_USER else input("Email: ")
    p = EMAIL_PASS if EMAIL_PASS else getpass.getpass("Password: ")
    download_shadowserver_reports(u, p)