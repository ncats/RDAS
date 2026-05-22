
import os
import sys

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))
import re
import csv
import time
import json
import random
import datetime
import hashlib
import unicodedata
import pandas as pd
from pathlib import Path 
from datetime import datetime as DT
from charset_normalizer import detect
from typing import Dict, List, Any, Optional
 
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize

# Setup NLTK for english word parsing for synonym filtering
from nltk.corpus import words as nltk_words
wordset = set(nltk_words.words())

from colorama import init, Fore, Style
# Initialize colorama for Windows compatibility
init()

def read_csv(file_path: Any) -> pd.DataFrame:
    """
    Read a CSV file into a pandas DataFrame.

    This helper keeps CSV loading behavior consistent across the project:
    - index_col=False prevents pandas from treating the first column as an index.
    - Path normalizes string/path-like inputs before passing them to pandas.
    - Specific file and parser errors are re-raised with clearer file context
      while preserving the original exception as the cause.
    """
    path = Path(file_path)

    try:
        return pd.read_csv(path, index_col=False)

    except FileNotFoundError as exc:
        raise FileNotFoundError(f"CSV file does not exist: {path}") from exc

    except pd.errors.EmptyDataError as exc:
        raise pd.errors.EmptyDataError(f"CSV file is empty: {path}") from exc

    except pd.errors.ParserError as exc:
        raise pd.errors.ParserError(f"Unable to parse CSV file: {path}") from exc


def read_csv_as_dict(file_path: Any) -> List[Dict[str, str]]:
    """
    Read a CSV file as a list of dictionaries.

    csv.DictReader uses the first row as column names and returns each later row
    as a dict keyed by those names. The utf-8-sig encoding intentionally accepts
    CSV files that include a UTF-8 byte-order mark, which is common in files
    exported from spreadsheet tools.
    """
    path = Path(file_path)

    try:
        # newline="" lets the csv module handle platform-specific line endings
        # correctly instead of letting Python translate them before parsing.
        with path.open("r", encoding="utf-8-sig", newline="") as csv_file:
            reader = csv.DictReader(csv_file)
            return [row for row in reader]

    except FileNotFoundError as exc:
        raise FileNotFoundError(f"CSV file does not exist: {path}") from exc

    except csv.Error as exc:
        raise csv.Error(f"Unable to parse CSV file as dictionaries: {path}") from exc
    
    
def write_json_to_file(data: Any, file_path: Any) -> None:
    """
    Serialize a Python object to a JSON file.

    The helper uses the same readable JSON format everywhere:
    - UTF-8 output for consistent cross-platform file handling.
    - ensure_ascii=False so non-ASCII text is written as normal characters.
    - indent=2 so generated files are easy to inspect in reviews and logs.
    """
    path = Path(file_path)

    try:
        # Use Path.open so callers can pass either a string path or a Path-like
        # object without each call site having to normalize the value first.
        with path.open("w", encoding="utf-8") as json_file:
            json.dump(
                data,
                json_file,
                ensure_ascii=False,
                indent=2,
            )

    except TypeError as exc:
        raise TypeError(f"Data is not JSON serializable for file: {path}") from exc

    except ValueError as exc:
        raise ValueError(f"Unable to encode JSON data for file: {path}") from exc

    except OSError as exc:
        raise OSError(f"Unable to write JSON file: {path}") from exc


def _load_json_file(file_path: Any) -> Any:
    """
    Load and parse a JSON file.

    This helper centralizes JSON reads so callers get consistent UTF-8 handling
    and useful file-specific errors. It returns the decoded Python value exactly
    as json.load produces it, usually a dict or list.
    """
    path = Path(file_path)

    try:
        # Keep reads explicit as UTF-8 because project-generated JSON files are
        # written that way by write_json_to_file().
        with path.open("r", encoding="utf-8") as json_file:
            return json.load(json_file)

    except FileNotFoundError as exc:
        raise FileNotFoundError(f"JSON file does not exist: {path}") from exc

    except json.JSONDecodeError as exc:
        raise json.JSONDecodeError(
            f"Unable to parse JSON file {path}: {exc.msg}",
            exc.doc,
            exc.pos,
        ) from exc

    except OSError as exc:
        raise OSError(f"Unable to read JSON file: {path}") from exc


def _format_recipients(recipients: Any) -> Optional[str]:
    """
    Format one or more email recipients for an email header.

    Email headers expect one comma-separated string, but callers may pass a
    single email, a comma-separated string, or an iterable of email values.
    Return None only when the caller provided no recipient value at all; that
    lets the email-sending code skip optional headers such as Cc.
    """
    if recipients is None:
        return None

    # Reuse the list-normalization helper so header formatting and SMTP
    # recipient-envelope formatting stay consistent with each other.
    normalized_recipients = _recipient_list(recipients)

    return ", ".join(normalized_recipients)


def _recipient_list(*recipient_values: Any) -> List[str]:
    """
    Flatten email recipient inputs into a clean list.

    Callers may pass recipients in several practical forms:
    - one comma-separated string from an environment variable,
    - a list/tuple/set from configuration,
    - multiple values that combine To and Cc recipients.

    Every non-empty recipient is stripped of surrounding whitespace. Empty
    values are ignored so the returned list is ready for SMTP sendmail().
    """
    recipients: List[str] = []

    def append_recipient_value(value: Any) -> None:
        """Normalize one recipient value and append any concrete emails."""
        if value is None:
            return

        if isinstance(value, (list, tuple, set)):
            # Recurse through nested collections so callers do not need to
            # flatten config values before passing them into this helper.
            for nested_value in value:
                append_recipient_value(nested_value)
            return

        # A single string can still contain multiple comma-separated emails,
        # especially when it came from an environment variable.
        for email in str(value).split(","):
            cleaned_email = email.strip()
            if cleaned_email:
                recipients.append(cleaned_email)

    for value in recipient_values:
        append_recipient_value(value)

    return recipients


def insert_params_to_template(template: str, params: Dict[str, Any]) -> str:
    """
    Render a simple ``$name`` placeholder template with concrete values.

    This helper is useful for generating readable query text or script output
    from small templates. It is intentionally simple and should not replace
    parameterized database APIs for user-provided values in executable SQL.

    Values are rendered through _escape_sql_literal() so SQL-style literal
    formatting is implemented in one place instead of being duplicated here.
    """
    rendered_template = template

    for key, value in params.items():
        placeholder = f"${key}"
        rendered_template = rendered_template.replace(
            placeholder,
            _escape_sql_literal(value),
        )

    return rendered_template

        
def _is_english(syn):
    """
    Checks if a string is a single English word.
    True if the string is a single word found in the wordset, False otherwise
    """
    tokens = syn.lower().split()
    return len(tokens) == 1 and tokens[0] in wordset


def _gard_text_normalize(text):
    """
    Normalizes text by converting to lowercase, replacing non-word characters
    with spaces, collapsing multiple spaces, and stripping whitespace.
    """
    # Convert to lowercase and split by non-word characters
    words = re.split(r'\W+', text.lower())
    # Filter out empty strings resulting from the split and join with single spaces
    return ' '.join(filter(None, words))

    
def _is_under_char_threshold(syn):
    """
    Checks if a string is a single word with fewer than 4 characters.
    True if the string is a single word under 4 characters, False otherwise
    """
    return len(syn.split()) == 1 and len(syn) < 4


def _len_greater_than_threshold(syn, threshold):
    return len(syn) > threshold

    
def _date_of_days_ago(num_days):
    today = datetime.date.today()
    days_ago = today - datetime.timedelta(days=num_days)

    return days_ago.strftime("%m/%d/%Y")


def _curr_timestamp(format_string="%Y-%m-%d %H:%M:%S"):
    """Returns the current date and time as a timestamp string with a custom format."""
    now = datetime.datetime.now()
    timestamp = now.strftime(format_string)
    return timestamp


def _date_string(format_string="%Y%m%d"):    
    now = datetime.datetime.now()
    date_str = now.strftime(format_string)
    return date_str


def _compare_json_by_properties(obj1, obj2, properties):

    for prop in properties:
        value1 = obj1.get(prop)
        value2 = obj2.get(prop)
        
        # Handle array comparison
        if isinstance(value1, list) and isinstance(value2, list):
            # Unordered match (ignore element order)
            if sorted(value1) != sorted(value2):
                return False
        else:
            # Regular comparison
            if value1 != value2:
                return False

    return True


def _clean(s: Any) -> str:
    """
    Sanitize input by removing non-alphanumeric characters except specific allowed characters.    
    Args:
        s: Input value to clean        
    Returns:
        Cleaned string or empty string if input is None/empty
    """
    if not s:
        return ''
    if not isinstance(s, str):
        s = str(s)
    return re.sub(r'[^\w\s\-\/@.+]+', '', s)


def _clean_data_extract(data):
    """
    Cleans and sanitizes dictionary data by removing special characters from strings
    and replacing empty values with empty string literals.    
    Args:
        data: Dictionary containing values to be cleaned        
    Returns:
        New dictionary with cleaned values (original dict is not modified)
    """
    cleaned = {}
    
    for k, v in data.items():
        # Check if value is an empty string, list, or dict
        if v == '' or v == [] or v == {}:
            cleaned[k] = '""'
        
        # Check if value is a non-empty string
        elif isinstance(v, str):
            # Reuse the same string sanitizer as _clean() so dictionary-level
            # extraction and scalar cleaning cannot drift apart over time.
            cleaned[k] = f'"{_clean(v)}"'
        
        # For all other data types (numbers, booleans, None, etc.)
        else:
            cleaned[k] = v
    
    return cleaned


def _split_string_with_brackets(text):
    # Match everything inside (), {}, [] OR any sequence of non-semicolon/bracket characters OR a semicolon itself

    # Example:
    # "AML (p13q22) or t(16;16)(p13;q22);abnormal bone marrow eosinophils;{a;b;c};Another [x;y;z]"
    # ["AML (p13q22) or t(16;16)(p13;q22)","abnormal bone marrow eosinophils","{a;b;c}","Another [x;y;z]"]

    pattern = r'(\([^()]*\)|\{[^{}]*\}|\[[^\[\]]*\]|[^;{}()\[\]]+|;)'
    matches = re.findall(pattern, text)

    result = []
    current = []
    
    for match in matches:
        if match == ';':  # Semicolon marks the split point
            if current:
                result.append(''.join(current))
                current = []
        else:
            current.append(match)

    if current:
        result.append(''.join(current))

    return result


def _try_parse_int(s):
    """Backward-compatible wrapper around the shared nullable int parser."""
    return _to_int(s)


def _to_int(value: Any) -> Optional[int]:
    """Convert nullable database/API values to ints."""

    if value is None or value == "":
        return None

    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> Optional[float]:
    """Convert nullable database/API values to floats."""

    if value is None or value == "":
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_json_list(value: Any) -> List[Any]:
    """Parse a JSON list value, passing through existing lists."""

    if value is None:
        return []

    try:
        parsed = json.loads(value) if isinstance(value, str) else value
    except json.JSONDecodeError:
        return []

    if isinstance(parsed, list):
        return parsed

    return [parsed]
    

def _na(s):
    """Convert common string NA/null markers to None."""
    if s is None:
        return None

    if _to_stripped_string(s).lower() in {'na', 'n/a', 'null', 'none'}:
        return None
    
    return s


def _id_range_generator(min_id, max_id, step, batch_size):

    if min_id > max_id:
        raise ValueError("Invalid value provided: min_id > max_id")
    if step <= 0:
        raise ValueError("Invalid value provided: step must be positive")
    if batch_size <= 0:
        raise ValueError("Invalid value provided: batch_size must be positive")

    # Number of IDs per batch (batch_size * ct_uniq_step)
    id_range_per_batch = batch_size * step  # How many of ID units per batch

    # For loop over ID ranges
    for start_id in range(min_id, max_id + 1, id_range_per_batch):

        end_id = min(start_id + id_range_per_batch - 1,max_id)

        yield start_id, end_id


def ask_to_continue(message):

    while True:
        response = input(f"{message} (yes/y): \n").lower().strip()
        if response in ('yes', 'y'):
            return True
        elif response in ('no', 'n'):
            return False
        else:
            print(Fore.RED +"Please enter 'yes' or 'y' to CONTINUE, 'no' or 'n' to STOP\n"+ Style.RESET_ALL)

    

# Function to parse date or return None
def parse_date(date_str):
    if date_str:
        try:
            # Handle datetime format (e.g., '2001-01-03T00:00:00')
            if 'T' in date_str:
                return DT.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
            # Handle date-only format (e.g., '2001-09-15')
            return DT.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return None
    return None


def detect_file_encoding(filename):
    try:
        with open(filename, 'rb') as file:  # Open in binary mode
            raw_data = file.read(10000)  # Read first 10,000 bytes (adjust if needed)
            result = detect(raw_data)
            encoding = result.get('encoding', None)
            confidence = result.get('confidence', 0.0)
            if encoding:
                return encoding, confidence
            else:
                raise ValueError(f"Could not detect encoding for '{filename}'")
    except FileNotFoundError:
        raise FileNotFoundError(f"File '{filename}' not found")
    


def convert_csv_files_to_utf8(dir_path):

    print(f'dir_path = {dir_path}')
     # Get all CSV files (case-insensitive)
    csv_files = Path(dir_path).glob('*.[Cc][Ss][Vv]')  

    for csv_file in csv_files: 

        filename = csv_file.name
        print(f'\n{filename}')

        encoding, confidence = detect_file_encoding(dir_path+'/'+filename)
        print(f"{filename}: Detected encoding: {encoding} (Confidence: {confidence:.2%})")
        
        convert_file_to_utf8(dir_path+'/'+filename, backup=True)


def convert_file_to_utf8(filepath, backup=False):
    """Convert a file to UTF-8 encoding."""
    try:
        # Detect the file's current encoding
        original_encoding, confidence = detect_file_encoding(filepath)
        #print(f"Detected encoding for '{filepath}': {original_encoding}")

        # Read the file with its original encoding
        with open(filepath, 'r', encoding=original_encoding, errors='replace') as file:
            content = file.read()

        # Optionally create a backup of the original file
        if backup:
            backup_filepath = filepath + '.bak'
            os.rename(filepath, backup_filepath)
            #print(f"Created backup: '{backup_filepath}'")

        # Write the file back with UTF-8 encoding
        with open(filepath, 'w', encoding='utf-8') as file:
            file.write(content)
        #print(f"Converted '{filepath}' to UTF-8")

    except Exception as e:
        print(Fore.RED + f"Error processing '{filepath}'{Style.RESET_ALL}: {e}")



def check_column_max_length(dir_path, column_names_list): 
 
    MAX_MAP = { col: 0 for col in column_names_list}
    STR_MAP = { col: '' for col in column_names_list}

    # Get all CSV files (case-insensitive)
    csv_files = Path(dir_path).glob('*.[Cc][Ss][Vv]')
   
    for csv_file in csv_files: 

        filename = csv_file.name
        encoding, confidence = detect_file_encoding(dir_path+'/'+filename)
        print(f"{filename}: Detected encoding: {encoding} (Confidence: {confidence:.2%})")
          
        with open(csv_file, 'r', newline='', encoding=encoding) as file:

            reader = csv.DictReader(file)  

            for row in list(reader)[1:]: # Skip the first row

                for col in column_names_list:
                    target_column_val = (row[col] if row[col] else None)

                    if target_column_val and len(target_column_val) > MAX_MAP[col]:

                        STR_MAP[col] = target_column_val
                        MAX_MAP[col] = len(target_column_val)
                     

    for key in MAX_MAP.keys():
        print(Fore.BLUE + f'\nMax length of {key} = {MAX_MAP[key]}'+ Style.RESET_ALL)
        print(f'{STR_MAP[key]}')


def _utf8(text):
    if isinstance(text, str):
        return text.encode('utf-8', errors='ignore').decode('utf-8')
    return text  # Leave non-strings untouched)


def _normalize_ascii_text(text: Any, default: Any = '') -> Any:
    """
    Normalize Unicode text to plain ASCII.

    Several older helpers need the same normalization but disagree on what to
    return for non-string inputs. Keeping the normalization in this helper makes
    that shared behavior explicit while allowing each wrapper to keep its legacy
    default.
    """
    if isinstance(text, str):
        return unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')

    return default


def _normalize_txt(text):
    """Normalize strings to ASCII and leave non-string values untouched."""
    return _normalize_ascii_text(text, default=text)


def _to_txt(text):
    """Normalize strings to ASCII and return an empty string for non-strings."""
    return _normalize_ascii_text(text, default='')


def _normalize_tuple(rowTuple):
    return tuple(_normalize_txt(item) for item in rowTuple)
 
 
# ntlk.stem
def _stem_text(text):
    stemmer = PorterStemmer()
    text_without_punctuation = re.sub(r'[^\w\s]', '', text) # Remove punctuation    
    words = word_tokenize(text_without_punctuation) # Tokenize the text into words
    stemmed_words = [stemmer.stem(word) for word in words]# Perform stemming on each word  
    stemmed_text = ' '.join(stemmed_words) # Join the stemmed words back into a single string
    return stemmed_text

def _remove_stop_words(text):
    stop_words = set(stopwords.words('english'))
    words = word_tokenize(text)
    filtered_words = [word for word in words if word.lower() not in stop_words]
    return ' '.join(filtered_words)


def _append_to_file(filename, text):
    with open(filename, 'a') as file:
        file.write(text + '\n')
 

def _append_to_file_and_print(filename, text):
    _append_to_file(filename, text)
    print(text)


def _format_dollars(amount):
    """Converts a number to a string representing dollars in M or K format.
    Args:
        amount: The numerical amount in dollars.
    Returns:
        A string representing the amount in dollars with "M" for millions
        or "K" for thousands, rounded to one decimal place.
    """
    if amount >= 1_000_000:
        return f"${amount / 1_000_000:.1f}M"
    elif amount >= 1_000:
        return f"${amount / 1_000:.1f}K"
    else:
        return f"${amount}"
    

def _val(obj, default=''):
    return obj if obj is not None else default


def _arr(obj, delimiter=';', default=None):
    """
    Split a delimited string into a list.

    The previous implementation used [] as a default argument, which creates one
    shared list object at function-definition time. Use None internally instead
    so each call gets a fresh empty list unless the caller passes an explicit
    default value.
    """
    if obj is None:
        return list(default) if isinstance(default, list) else (default or [])

    return str(obj).split(delimiter)


def _split_str(item):
    """Split a bracket-wrapped comma-separated string into a list."""
    if not item:
        return []

    return [part.strip() for part in str(item).strip('[]').split(',') if part.strip()]


def _time_hms(elapsed_time):
    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    return int(hours), int(minutes), int(seconds)

def _time_day_hms(elapsed_time):
    hours, minutes, seconds = _time_hms(elapsed_time)
    days, hours = divmod(hours, 24)
    return int(days), int(hours), int(minutes), int(seconds)


def _curr_time_diff(start_time):
    end_time = time.time()
    return _time_hms(end_time - start_time)   


def _hash(input_string):
    """
    Converts string to lowercase, and generates MD5 hash.        
    Returns:
        str: 32-character hexadecimal representation of the MD5 hash
    """
    if not input_string:
        input_string = 'not-a-string'

    # Convert to lowercase and strip
    lower_string = input_string.lower().strip()
         
    # Generate MD5 hash
    hash_object = hashlib.md5(lower_string.encode('utf-8'))
    hash_code = hash_object.hexdigest()
    
    return hash_code


def _hash_az(input_string):
    """
    Converts string to lowercase, removes non a-z characters, and generates MD5 hash.   
    """ 
    # Remove non a-z characters using regex
    cleaned_string = re.sub(r'[^a-z]', '', input_string)
     
    return _hash(cleaned_string)


def _elapsed_time(start_time, end_time):
    """Backward-compatible wrapper around elapsed_time()."""
    return elapsed_time(start_time, end_time)


def elapsed_time(start_time, end_time):
    """Return elapsed wall-clock time as ``(hours, minutes, seconds)``."""
    return _time_hms(end_time - start_time)

# for MySQL query result rows only
def _set_value_for_none(row):
    for key, value in row.items():
        if value is None:
            if isinstance(value, str): # type is string
                row[key] = ''
            elif isinstance(value, (int, float)): # type is number
                row[key] = -999

    return row


def _safe_get(data: Dict, *keys: str, default: Any = '') -> Any:
    """
    Safely navigate nested dictionaries with multiple keys.    
    Args:
        data: Dictionary to navigate
        *keys: Variable number of keys to traverse
        default: Default value if key path doesn't exist        
    Returns:
        Value at the key path or default value
    """
    result = data
    for key in keys:
        if not isinstance(result, dict):
            return default
        result = result.get(key, default)
        if result == default:
            return default
    return result


def _empty_if_none(value: Any) -> Any:
    """Return an empty string for None, otherwise keep the original value."""
    return "" if value is None else value


def _to_string(value: Any) -> str:
    """Return an empty string for None, otherwise convert the value to string."""
    return str(_empty_if_none(value))


def _to_stripped_string(value: Any) -> str:
    """Return an empty string for None, otherwise convert to a stripped string."""
    return _to_string(value).strip()


def _na_if_empty(value: Any) -> str:
    """Return 'N/A' for None or blank values, otherwise a stripped string."""
    value = _to_stripped_string(value)
    return value if value else "N/A"


def _as_bool(value: Any) -> bool:
    """Convert common database/string truthy values to a boolean."""
    if isinstance(value, bool):
        return value

    if isinstance(value, int):
        return value == 1

    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "t", "yes", "y"}

    return False


def _as_list(value: Any) -> List[str]:
    """Normalize a scalar/list value to a list of strings."""
    if value is None:
        return []

    if isinstance(value, list):
        return [str(item) for item in value if item is not None]

    return [str(value)]


def _normalize_keywords(value: Any) -> List[str]:
    """Normalize keyword data to sorted, unique, lowercase strings."""
    if value is None:
        return []

    if not isinstance(value, list):
        value = [value]

    return sorted({
        str(keyword).strip().lower()
        for keyword in value
        if keyword is not None and str(keyword).strip()
    })


def _make_hash_key(input_str: str = None) -> str:
    """
    Create a SHA256 hash key from input string.    
    Args: input_str: Input string to hash. If None/empty, generates random string.        
    Returns: SHA256 hexadecimal hash string
    """
    # Handle empty input once at the start
    if not input_str:
        input_str = _random_str()
    
    # 1. Remove non-printable characters, 
    # 2. Replace whitespace with '_', 
    # 3. Lowercase
    cleaned = re.sub(r'[^\x20-\x7E]+', '', str(input_str)) # Remove non-printable
    normalized = re.sub(r'\s+', '_', cleaned).lower()      # Replace whitespace & lowercase
    
    # If cleaning resulted in empty string, use random
    if not normalized:
        normalized = _random_str().lower()
    
    # Hash and return
    return hashlib.sha256(normalized.encode('utf-8')).hexdigest()


def _random_str():
    return _curr_timestamp() + str(random.random())


def _remove_parentheses(text):
    """
    Remove parentheses and their contents from a string.
    Also removes any trailing whitespace.    
    Args:
        text: Input string        
    Returns:
        String with parentheses and contents removed
    """
    # "National Eye Institute (NEI)" -> "National Eye Institute"
    # "National Heart, Lung, and Blood Institute (NHLBI)" -> "National Heart, Lung, and Blood Institute"

    # Remove parentheses and everything inside them
    result = re.sub(r'\s*\([^)]*\)', '', text)
    
    # Remove any trailing whitespace
    return result.strip()
 

def _escape_sql_literal(value: Any) -> str:
    """
    Minimal ANSI SQL literal serializer.

    WARNING:
        - For string literals only.
        - NOT safe for identifiers.
        - Prefer parameterized queries.
    """
    if value is None:
        return "NULL"

    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"

    if isinstance(value, (int, float)):
        return str(value)

    # Treat everything else as string
    s = str(value).replace("'", "''")
    return f"'{s}'"
