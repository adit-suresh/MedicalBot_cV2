import re

# Regular expressions for validation
EMIRATES_ID_PATTERN = re.compile(r'^\d{3}-\d{4}-\d{7}-\d{1}$')
PASSPORT_NUMBER_PATTERN = re.compile(r'^[A-Z0-9]{6,9}$')

# Email filtering
SUBJECT_KEYWORDS = ["addition", "adding", "add", "endorse", "endorsement"]

# File naming patterns
FILE_NAME_PATTERN = re.compile(r'^[a-zA-Z0-9\s\-_.,()\']+\.[a-zA-Z0-9]+$')