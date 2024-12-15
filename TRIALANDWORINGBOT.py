import os
import pickle
import telepot
from telepot.loop import MessageLoop
import logging
import time
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import threading  # Importing threading module
from collections import deque
from threading import Lock
from filelock import FileLock  # For file locking

# Google Drive API Scopes
SCOPES = ['https://www.googleapis.com']

# Replace with your actual bot token
TOKEN = ''  # Replace with your actual bot token

# Enable logging for debugging
logging.basicConfig(level=logging.WARNING)

# Global variable for Google Drive service
creds = None

# Caching for search results
cache = {}
cache_lock = Lock()
CACHE_EXPIRY = 300  # Cache expiry in seconds

# Rate limiter for Telegram messages
RATE_LIMIT = 10  # Max messages per second
message_queue = deque()
message_lock = Lock()

# Function to process queued messages
def process_message_queue(bot):
    while True:
        with message_lock:
            if message_queue:
                chat_id, message = message_queue.popleft()
                bot.sendMessage(chat_id, message)
        time.sleep(1 / RATE_LIMIT)

# Function to send messages with rate limiting
def send_message(chat_id, message):
    with message_lock:
        message_queue.append((chat_id, message))

# Authenticate Google Drive API
def authenticate():
    global creds
    creds_file_path = r'n'  # Path to your credentials file

    if not os.path.exists(creds_file_path):
        logging.error(f"Error: {creds_file_path} not found!")
        return None

    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(creds_file_path, SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return build('drive', 'v3', credentials=creds)

# Function to search for PDF in Google Drive
def search_pdf_on_drive(service, pdf_name):
    with cache_lock:
        if pdf_name in cache and time.time() - cache[pdf_name]['timestamp'] < CACHE_EXPIRY:
            return cache[pdf_name]['file_id']

    query = f"name = '{pdf_name}.pdf'"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])

    if not files:
        logging.info(f"No file found for: {pdf_name}.pdf")
        return None
    else:
        file_id = files[0]['id']
        with cache_lock:
            cache[pdf_name] = {'file_id': file_id, 'timestamp': time.time()}
        return file_id

# Function to download PDF from Google Drive with retry mechanism
def download_pdf_from_drive(service, file_id, chat_id, pdf_name):
    send_message(chat_id, f"Downloading PDF: {pdf_name}. Please wait...")
    
    file_path = f"temp_{file_id}_{int(time.time())}.pdf"
    lock_path = f"{file_path}.lock"  # Lock file for the temp file
    
    # Retry logic
    max_retries = 3
    retry_delay = 5  # Initial delay in seconds

    for attempt in range(max_retries):
        try:
            # Ensure no other process is using the file by acquiring a lock
            with FileLock(lock_path):
                request = service.files().get_media(fileId=file_id)
                timeout = 180  # 3 minutes timeout
                request.uri = request.uri + f"&timeout={timeout}"

                with open(file_path, 'wb') as f:
                    downloader = MediaIoBaseDownload(f, request)
                    done = False
                    last_reported_progress = 0

                    while not done:
                        status, done = downloader.next_chunk()
                        if status:
                            progress = int(status.progress() * 100)
                            if progress - last_reported_progress >= 25:
                                send_message(chat_id, f"Download progress: {progress}%")
                                last_reported_progress = progress

            if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                return file_path
            else:
                send_message(chat_id, "Failed to download the PDF.")
                return None
        except Exception as e:
            send_message(chat_id, f"Error occurred: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                send_message(chat_id, "Max retries reached. Please try again later.")
                return None

# Function to handle PDF requests
def handle_pdf_request(chat_id, pdf_name):
    service = authenticate()
    if not service:
        send_message(chat_id, "Authentication failed! Please try again later.")
        return

    file_id = search_pdf_on_drive(service, pdf_name)
    if file_id:
        # Use a direct thread to handle the download and send task
        download_thread = threading.Thread(target=download_and_send_pdf, args=(chat_id, service, file_id, pdf_name))
        download_thread.start()
    else:
        send_message(chat_id, f"Sorry, no PDF found for '{pdf_name}'.")

# Function to download and send PDF
def download_and_send_pdf(chat_id, service, file_id, pdf_name):
    try:
        file_path = download_pdf_from_drive(service, file_id, chat_id, pdf_name)

        if file_path:
            try:
                with open(file_path, 'rb') as f:
                    bot.sendDocument(chat_id, f, caption=f"Here is your PDF: {pdf_name}")
                send_message(chat_id, "PDF sent successfully!")
            except Exception as e:
                send_message(chat_id, f"Error sending PDF: {e}")
            finally:
                if os.path.exists(file_path):
                    os.remove(file_path)  # Ensure the file is removed after sending
        else:
            send_message(chat_id, "Failed to download the PDF.")
    except Exception as e:
        logging.error(f"Error in thread while downloading and sending PDF: {e}")
        send_message(chat_id, "An error occurred while processing your request.")

# Handle messages from users
def handle_message(msg):
    chat_id = msg['chat']['id']
    command = msg['text']

    if command == '/start':
        send_message(chat_id, "Hello! I'm your PDF bot. Use /help to see available commands.")
    elif command == '/info':
        send_message(chat_id, "I am a simple Telegram bot built using the telepot library. Created by Himanshu Kabariya.")
    elif command == '/help':
        help_text = (
            "/start - Start the bot\n"
            "/help - Show this help message\n"
            "/info - Get information about the bot\n"
            "/pdf_<Room_No-Bed_No> - Get a specific PDF file (e.g., 101-A)"
        )
        send_message(chat_id, help_text)
    elif command.startswith('/pdf_'):
        pdf_name = command[5:]
        handle_pdf_request(chat_id, pdf_name)
    else:
        send_message(chat_id, "Sorry, I didn't understand that command. Type /help to see available commands.")

# Main function to start the bot
def main():
    global bot
    bot = telepot.Bot(TOKEN)

    # Set up the message handler
    MessageLoop(bot, handle_message).run_as_thread()

    # Start the message processing thread
    threading.Thread(target=process_message_queue, args=(bot,), daemon=True).start()

    print("Bot is running...")

    # Keep the bot running
    while True:
        time.sleep(10)

if __name__ == "__main__":
    main()
