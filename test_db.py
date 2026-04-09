import django, os
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'edms_pervy_kluch.settings')
django.setup()
from django.db import connection
cursor = connection.cursor()
cursor.execute("SELECT version()")
print("OK:", cursor.fetchone()[0][:60])
cursor.execute("SELECT COUNT(*) FROM auth_user")
print("Пользователей в Railway DB:", cursor.fetchone()[0])
cursor.execute("SELECT COUNT(*) FROM documents_document")
print("Документов в Railway DB:", cursor.fetchone()[0])
