import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, simpledialog
import threading
import subprocess
import requests # For Gemini API
import json
import os

# Attempt to import functions from your other scripts
try:
    import shadowserver_files_processor as processor
    import email_downloader # Import the new script
except ImportError as e:
    messagebox.showerror("Error", f"Could not find required scripts. Missing: {e.name}\nMake sure all .py files are in the same directory.")
    exit()

# --- Configuration for other scripts ---
# These paths are dynamically generated based on the script location
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.join(BASE_DIR, 'src')
DST_DIR = os.path.join(BASE_DIR, 'dst')
DESTINATION_FILE = os.path.join(DST_DIR, 'destination.csv')
DEDUPLICATED_FILE = os.path.join(DST_DIR, 'destination_deduplicated.csv')
SPLIT_DIR = os.path.join(BASE_DIR, 'splitted')
REPORTS_DIR = os.path.join(BASE_DIR, 'reports')

class VulnerabilityApp(tk.Tk):
    def __init__(self):
        super().__init__()
        for directory in [SRC_DIR, DST_DIR, SPLIT_DIR, REPORTS_DIR]:
            os.makedirs(directory, exist_ok=True)

        self.title("Vulnerability Data Toolkit")
        self.geometry("900x750") # Increased height slightly

        self.style = ttk.Style(self)
        self.style.theme_use('clam') 
        self.configure(bg='#2E2E2E')
        self.style.configure('.', background='#2E2E2E', foreground='#EAEAEA', fieldbackground='#4E4E4E')
        self.style.configure('TButton', padding=6, relief="flat", background='#4A4A4A', foreground='#EAEAEA')
        self.style.map('TButton', background=[('active', '#5A5A5A')])
        self.style.configure('TFrame', background='#2E2E2E')
        self.style.configure('TLabel', background='#2E2E2E', foreground='#EAEAEA', font=('Arial', 10))
        
        # Store credentials temporarily in memory
        # Try to load from environment variables via the downloader script first
        self.email_user = email_downloader.EMAIL_USER
        self.email_pass = email_downloader.EMAIL_PASS
        
        self.create_widgets()

    def create_widgets(self):
        main_frame = ttk.Frame(self, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # --- Control Frame ---
        control_frame = ttk.Frame(main_frame, padding="10", relief="groove", borderwidth=1)
        control_frame.pack(fill=tk.X, side=tk.TOP, pady=5)
        
        ttk.Label(control_frame, text="1. Ingest & Process:", font=('Arial', 12, 'bold')).pack(anchor='w', pady=5)
        
        # Email Button
        btn_email = ttk.Button(control_frame, text="📥 Download from Email", command=self.run_email_download)
        btn_email.pack(side=tk.LEFT, padx=5)
        
        ttk.Separator(control_frame, orient='vertical').pack(side=tk.LEFT, fill='y', padx=10, pady=5)

        btn_discover = ttk.Button(control_frame, text="Discover New Files", command=self.run_discover)
        btn_discover.pack(side=tk.LEFT, padx=5)
        btn_process = ttk.Button(control_frame, text="Process New Files", command=self.run_process)
        btn_process.pack(side=tk.LEFT, padx=5)
        
        # --- Utilities Frame ---
        utils_frame = ttk.Frame(main_frame, padding="10", relief="groove", borderwidth=1)
        utils_frame.pack(fill=tk.X, side=tk.TOP, pady=10)

        ttk.Label(utils_frame, text="2. Utilities:", font=('Arial', 12, 'bold')).pack(anchor='w', pady=5)
        btn_dedup = ttk.Button(utils_frame, text="De-duplicate Records", command=self.run_deduplicate)
        btn_dedup.pack(side=tk.LEFT, padx=5)
        btn_split = ttk.Button(utils_frame, text="Split by IP", command=self.run_split)
        btn_split.pack(side=tk.LEFT, padx=5)
        
        # --- Analysis Frame ---
        analysis_frame = ttk.Frame(main_frame, padding="10", relief="groove", borderwidth=1)
        analysis_frame.pack(fill=tk.X, side=tk.TOP, pady=10)

        ttk.Label(analysis_frame, text="3. Analyze & Remediate:", font=('Arial', 12, 'bold')).pack(anchor='w', pady=5)
        btn_analyze = ttk.Button(analysis_frame, text="Run Full Analysis", command=self.run_analysis)
        btn_analyze.pack(side=tk.LEFT, padx=5)
        
        # AI Remediation
        ttk.Separator(analysis_frame, orient='vertical').pack(side=tk.LEFT, fill='y', padx=10, pady=5)
        self.ai_issue_entry = ttk.Entry(analysis_frame, width=30, font=('Arial', 10))
        self.ai_issue_entry.pack(side=tk.LEFT, padx=(0, 5))
        self.ai_issue_entry.insert(0, "Enter issue (e.g., rdp service)...")
        btn_ai = ttk.Button(analysis_frame, text="Ask AI for Remediation", command=self.run_ai_remediation)
        btn_ai.pack(side=tk.LEFT, padx=5)

        # --- Log Frame ---
        log_frame = ttk.Frame(main_frame)
        log_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        ttk.Label(log_frame, text="Activity Console / Analysis Report:", font=('Arial', 12, 'bold')).pack(anchor='w', pady=5)
        self.log_text = scrolledtext.ScrolledText(log_frame, height=20, width=100, wrap=tk.WORD, bg='#1C1C1C', fg='#E0E0E0', insertbackground='white')
        self.log_text.pack(fill=tk.BOTH, expand=True)

    def log_message(self, message, level="info"):
        """Thread-safe logging to the GUI text box."""
        def _log():
            self.log_text.configure(state='normal')
            self.log_text.insert(tk.END, f"{message}\n")
            self.log_text.configure(state='disabled')
            self.log_text.see(tk.END)
        self.after(0, _log)

    def run_in_thread(self, target_function, *args):
        """Wrapper to run a function in a separate thread."""
        def threaded_function():
            try:
                target_function(self.log_message, *args)
            except Exception as e:
                self.log_message(f"--- Thread Error ---\n{e}\n---", "error")
        threading.Thread(target=threaded_function, daemon=True).start()

    # --- Script Runners ---

    def run_email_download(self):
        # Prompt for credentials only if we don't have them from env vars
        if not self.email_user or not self.email_pass:
            self.email_user = simpledialog.askstring("Email Login", "Enter your email address:")
            if not self.email_user: return
            self.email_pass = simpledialog.askstring("Email Login", "Enter your password (App Password recommended):", show='*')
            if not self.email_pass: return

        self.log_message("--- Starting Email Download ---")
        
        # Default IMAP server (can be made configurable)
        imap_server = "imap.gmail.com" 
        
        self.run_in_thread(
            email_downloader.download_shadowserver_reports, 
            self.email_user, 
            self.email_pass, 
            imap_server
        )

    def run_discover(self):
        self.log_text.configure(state='normal')
        self.log_text.delete(1.0, tk.END)
        self.log_text.configure(state='disabled')
        self.run_in_thread(processor.run_discover)

    def run_process(self):
        self.log_text.configure(state='normal')
        self.log_text.delete(1.0, tk.END)
        self.log_text.configure(state='disabled')
        self.run_in_thread(processor.run_process)

    def run_script_in_subprocess(self, script_name, args, log_callback):
        try:
            command = ['python', script_name] + args
            log_callback(f"--- Running: {script_name} ---")
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            if result.stdout: log_callback(result.stdout)
            if result.stderr: log_callback(f"Errors:\n{result.stderr}", "warning")
            log_callback(f"--- {script_name} Finished ---", "success")
        except Exception as e:
            log_callback(f"Error running script: {e}", "error")

    def run_deduplicate(self):
        self.run_in_thread(self.run_script_in_subprocess, 'deduplicate_records.py', [DESTINATION_FILE, DEDUPLICATED_FILE])

    def run_split(self):
        self.run_in_thread(self.run_script_in_subprocess, 'split_csv_by_ip.py', [DEDUPLICATED_FILE, SPLIT_DIR])

    def run_analysis(self):
        self.run_in_thread(self.run_script_in_subprocess, 'analyzer.py', [DEDUPLICATED_FILE])

    def run_ai_remediation(self):
        issue = self.ai_issue_entry.get()
        if not issue or "Enter issue" in issue:
            messagebox.showwarning("Warning", "Please enter an issue name first.")
            return
        self.log_message(f"--- 🤖 Asking AI about '{issue}'... ---")
        self.run_in_thread(self.get_gemini_remediation, issue)

    def get_gemini_remediation(self, log_callback, issue):
        apiKey = "" # PROVIDED BY ENVIRONMENT
        apiUrl = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-09-2025:generateContent?key={apiKey}"
        payload = {
            "contents": [{"parts": [{"text": f"Provide a concise, step-by-step remediation guide for an open vulnerability: '{issue}'."}]}]
        }
        try:
            response = requests.post(apiUrl, headers={'Content-Type': 'application/json'}, data=json.dumps(payload), timeout=30)
            if response.status_code == 200:
                text = response.json().get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', 'No content.')
                log_callback(f"--- AI Guide for '{issue}' ---\n{text}\n-----------------------------", "success")
            else:
                log_callback(f"AI Error: {response.status_code}", "error")
        except Exception as e:
            log_callback(f"AI Connection Error: {e}", "error")

if __name__ == "__main__":
    app = VulnerabilityApp()
    app.mainloop()