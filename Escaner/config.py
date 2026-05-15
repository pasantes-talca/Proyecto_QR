import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "10.242.4.13")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME", "stock")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_SCHEMA = os.getenv("DB_SCHEMA", "produccion")
MAX_AUTOFILL = int(os.getenv("MAX_AUTOFILL", 5000))

# ── Email ─────────────────────────────────────────────────────────────────────
EMAIL_SMTP_HOST     = os.getenv("EMAIL_SMTP_HOST", "mail.talca.com.ar")
EMAIL_SMTP_PORT     = int(os.getenv("EMAIL_SMTP_PORT", 587))
EMAIL_SMTP_USER     = os.getenv("EMAIL_SMTP_USER", "pasantes@talca.com.ar")
EMAIL_SMTP_PASSWORD = os.getenv("EMAIL_SMTP_PASSWORD", "")
EMAIL_FROM          = os.getenv("EMAIL_FROM", "pasantes@talca.com.ar")
EMAIL_TO            = os.getenv("EMAIL_TO", "pasantes@talca.com.ar,logistica@talca.com.ar")
EMAIL_USE_TLS       = os.getenv("EMAIL_USE_TLS", "true").lower() == "true"