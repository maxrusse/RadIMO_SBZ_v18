from flask import (
    Flask, request, jsonify,
    render_template, redirect,
    url_for, send_from_directory, flash
)
import pandas as pd
from datetime import datetime, time
import pytz
import os
import re
import shutil
from threading import Lock


import yaml
from flask import session, redirect, url_for, request, render_template
from functools import wraps

import logging
from logging.handlers import RotatingFileHandler
import os

os.makedirs('logs', exist_ok=True)

selection_logger = logging.getLogger('selection')
selection_logger.setLevel(logging.INFO)

handler = RotatingFileHandler('logs/selection.log', maxBytes=10_000_000, backupCount=3)
handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
selection_logger.addHandler(handler)


app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.static_folder = 'static'
# Set a secret key for sessions (make sure to set a secure key in production)
app.secret_key = 'your-maxsecret-key'


if not os.path.exists(app.config['UPLOAD_FOLDER']):
    os.makedirs(app.config['UPLOAD_FOLDER'])

lock = Lock()

# -----------------------------------------------------------
# Global constants & modality-specific factors
# -----------------------------------------------------------
SKILL_COLUMNS = ["Normal", "Notfall", "Privat", "Herz", "Msk", "Gyn"]

# Base weight per role
skill_weights = {
    "Normal":  1.0,
    "Notfall": 1.1,
    "Herz":    1.2,
    "Privat":  1.2,
    "Msk":     0.8,
    "Gyn":   0.8
}

# Modality-specific multiplier: CT=1, MR=1.2, X-Ray=0.33
modality_factors = {
    'ct': 1.0,
    'mr': 1.2,
    'xray': 0.33
}

# Allowed modalities (all lower-case)
allowed_modalities = ['ct', 'mr', 'xray']

# -----------------------------------------------------------
# NEW: Global worker data structure for cross-modality tracking
# -----------------------------------------------------------
global_worker_data = {
    'worker_ids': {},  # Map of worker name variations to canonical ID
    # Modality-specific weighted counts and assignments:
    'weighted_counts_per_mod': {mod: {} for mod in allowed_modalities},
    'assignments_per_mod': {mod: {} for mod in allowed_modalities},
    'last_reset_date': None  # Global reset date tracker
}

# -----------------------------------------------------------
# Global state: one "data bucket" per modality.
# -----------------------------------------------------------
modality_data = {}
for mod in allowed_modalities:
    modality_data[mod] = {
        'working_hours_df': None,
        'info_texts': [],
        'total_work_hours': {},
        'worker_modifiers': {},
        'draw_counts': {},
        'skill_counts': {skill: {} for skill in SKILL_COLUMNS},
        'WeightedCounts': {},
        'last_uploaded_filename': f"SBZ_{mod.upper()}.xlsx",  # e.g. SBZ_CT.xlsx
        'default_file_path': os.path.join(app.config['UPLOAD_FOLDER'], f"SBZ_{mod.upper()}.xlsx"),
        'scheduled_file_path': os.path.join(app.config['UPLOAD_FOLDER'], f"SBZ_{mod.upper()}_scheduled.xlsx"),
        'last_reset_date': None
    }

# -----------------------------------------------------------
# TIME / DATE HELPERS (unchanged)
# -----------------------------------------------------------
def get_local_berlin_now() -> datetime:
    tz = pytz.timezone("Europe/Berlin")
    aware_now = datetime.now(tz)
    naive_now = aware_now.replace(tzinfo=None)
    return naive_now

def parse_time_range(time_range: str):
    start_str, end_str = time_range.split('-')
    start_time = datetime.strptime(start_str.strip(), '%H:%M').time()
    end_time   = datetime.strptime(end_str.strip(), '%H:%M').time()
    return start_time, end_time

# -----------------------------------------------------------
# Worker identification helper functions (NEW)
# -----------------------------------------------------------
def get_canonical_worker_id(worker_name):
    """
    Get the canonical worker ID from any name variation.
    If not found, create a new canonical ID.
    """
    if worker_name in global_worker_data['worker_ids']:
        return global_worker_data['worker_ids'][worker_name]
    
    canonical_id = worker_name
    abk_match = worker_name.strip().split('(')
    if len(abk_match) > 1 and ')' in abk_match[1]:
        abbreviation = abk_match[1].split(')')[0].strip()
        canonical_id = abbreviation  # Use abbreviation as canonical ID
    
    global_worker_data['worker_ids'][worker_name] = canonical_id
    return canonical_id

def get_all_workers_by_canonical_id():
    """
    Get a mapping of canonical worker IDs to all their name variations.
    """
    canonical_to_variations = {}
    for name, canonical in global_worker_data['worker_ids'].items():
        if canonical not in canonical_to_variations:
            canonical_to_variations[canonical] = []
        canonical_to_variations[canonical].append(name)
    return canonical_to_variations

def validate_time_string(time_val, context_label="") -> (bool, str):
    """
    Validate a single TIME cell/string. Returns (is_valid, error_message).
    context_label is appended to error messages for clarity (e.g., 'Zeile 2').
    """
    label = f" in {context_label}" if context_label else ""

    if pd.isna(time_val) or str(time_val).strip() == "":
        return False, f"Spalte 'TIME' enthält leere Zelle{label}"

    time_str = str(time_val).strip()

    if '.' in time_str and ':' not in time_str:
        return False, (
            f"Falsches Zeitformat{label}: '{time_str}' - Verwenden Sie ':' statt '.'"
            " (z.B. '11:15' statt '11.15')"
        )

    if '-' not in time_str:
        return False, (
            f"Falsches Zeitformat{label}: '{time_str}' - Format muss 'HH:MM-HH:MM' sein"
            " (z.B. '08:00-16:00')"
        )

    try:
        parts = time_str.split('-')
        if len(parts) != 2:
            return False, (
                f"Falsches Zeitformat{label}: '{time_str}' - Genau ein '-' Zeichen erwartet"
            )

        start_str, end_str = parts[0].strip(), parts[1].strip()

        for time_part, time_label in [(start_str, "Start"), (end_str, "Ende")]:
            if ':' not in time_part:
                return False, (
                    f"Falsches Zeitformat{label}: {time_label}zeit '{time_part}' muss Format 'HH:MM' haben"
                )

            time_components = time_part.split(':')
            if len(time_components) != 2:
                return False, (
                    f"Falsches Zeitformat{label}: {time_label}zeit '{time_part}' muss Format 'HH:MM' haben"
                )

            hour_str, minute_str = time_components
            if not hour_str.isdigit() or not minute_str.isdigit():
                return False, (
                    f"Falsches Zeitformat{label}: {time_label}zeit '{time_part}' enthält nicht-numerische Zeichen"
                )

            hour, minute = int(hour_str), int(minute_str)
            if not (0 <= hour <= 23):
                return False, (
                    f"Falsches Zeitformat{label}: {time_label}zeit Stunde '{hour}' muss zwischen 0-23 sein"
                )
            if not (0 <= minute <= 59):
                return False, (
                    f"Falsches Zeitformat{label}: {time_label}zeit Minute '{minute}' muss zwischen 0-59 sein"
                )

        # Final parsing check
        parse_time_range(time_str)
        return True, ""
    except Exception as e:
        return False, f"Falsches Zeitformat{label}: '{time_str}' - {str(e)}"

def validate_excel_structure(df: pd.DataFrame, required_columns) -> (bool, str):
    """
    Comprehensive validation of Excel file structure and data formats.
    Returns (is_valid, error_message) tuple.
    """
    # Rename column "PP" to "Privat" if it exists
    if "PP" in df.columns:
        df.rename(columns={"PP": "Privat"}, inplace=True)

    # Check for missing required columns
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        return False, f"Fehlende Spalten: {', '.join(missing_columns)}"

    # Validate PPL column (must not be empty)
    if 'PPL' in df.columns:
        if df['PPL'].isna().any():
            empty_rows = df[df['PPL'].isna()].index.tolist()
            return False, f"Spalte 'PPL' enthält leere Zellen in Zeilen: {empty_rows}"

    # Validate TIME format - must be HH:MM-HH:MM
    if 'TIME' in df.columns:
        for idx, time_val in enumerate(df['TIME']):
            is_valid_time, time_error = validate_time_string(time_val, f"Zeile {idx + 2}")
            if not is_valid_time:
                return False, time_error

    # Validate Modifier column format
    if 'Modifier' in df.columns:
        for idx, mod_val in enumerate(df['Modifier']):
            if pd.isna(mod_val):
                continue  # NaN is OK, will be filled with 1.0

            try:
                # Try to convert to float
                mod_str = str(mod_val).strip().replace(',', '.')
                mod_float = float(mod_str)

                # Check reasonable range
                if mod_float <= 0:
                    return False, f"Modifier in Zeile {idx + 2} muss größer als 0 sein (Wert: {mod_float})"
                if mod_float > 10:
                    return False, f"Modifier in Zeile {idx + 2} ist ungewöhnlich hoch (Wert: {mod_float}). Bitte überprüfen."

            except Exception as e:
                return False, f"Modifier-Spalte ungültiges Format in Zeile {idx + 2}: {str(e)}"

    # Check skill columns - must be numeric and valid values (0, 1, or 2)
    for skill in SKILL_COLUMNS:
        if skill in df.columns:
            for idx, skill_val in enumerate(df[skill]):
                if pd.isna(skill_val):
                    continue  # NaN is OK, will be filled with 0

                try:
                    skill_int = int(float(skill_val))
                    if skill_int not in [0, 1, 2]:
                        return False, f"Spalte '{skill}' in Zeile {idx + 2}: Wert muss 0, 1 oder 2 sein (Wert: {skill_int})"
                except Exception as e:
                    return False, f"Spalte '{skill}' in Zeile {idx + 2}: Ungültiger Wert '{skill_val}' - muss numerisch sein"

    return True, ""


def validate_uploaded_file(file_path: str) -> (bool, str):
    """
    Validate an uploaded Excel file before it's processed.
    Returns (is_valid, error_message) tuple.
    """
    try:
        # Try to open the Excel file
        excel_file = pd.ExcelFile(file_path)

        # Check for required sheet
        if 'Tabelle1' not in excel_file.sheet_names:
            return False, "Blatt 'Tabelle1' nicht gefunden. Die Excel-Datei muss ein Arbeitsblatt namens 'Tabelle1' enthalten."

        # Read the sheet
        df = pd.read_excel(excel_file, sheet_name='Tabelle1')

        # Check if dataframe is empty
        if df.empty:
            return False, "Blatt 'Tabelle1' ist leer. Bitte fügen Sie Daten hinzu."

        # Define required columns
        required_columns = ['PPL', 'TIME']

        # Validate structure
        valid, error_msg = validate_excel_structure(df, required_columns)
        if not valid:
            return False, error_msg

        return True, ""

    except Exception as e:
        return False, f"Fehler beim Lesen der Excel-Datei: {str(e)}"


def validate_manual_entry(person: str, time_str: str, modifier_str: str, form_data) -> (bool, str, dict):
    """
    Validate fields submitted via the manual edit/add form.
    Returns (is_valid, error_message, normalized_values).
    """
    normalized = {}

    if not person or '(' not in person or ')' not in person:
        return False, "Name muss ein Kürzel in Klammern enthalten (z.B. 'Max Mustermann (MM)').", {}

    initials_match = re.search(r"\(([A-Za-zÄÖÜäöüß]{2,})\)\s*$", person)
    if not initials_match:
        return False, "Kürzel in Klammern muss aus mindestens 2 Buchstaben bestehen (z.B. 'MM' oder 'RADIMO').", {}

    normalized['person'] = person.strip()

    valid_time, time_error = validate_time_string(time_str, "der Eingabe")
    if not valid_time:
        return False, time_error, {}
    normalized['time'] = str(time_str).strip()

    modifier_clean = modifier_str.strip().replace(',', '.') if modifier_str else "1.0"
    try:
        modifier_val = float(modifier_clean)
    except ValueError:
        return False, "Modifier muss eine Zahl sein.", {}

    if modifier_val <= 0:
        return False, "Modifier muss größer als 0 sein.", {}
    if modifier_val > 10:
        return False, "Modifier ist ungewöhnlich hoch (>10). Bitte prüfen.", {}
    normalized['modifier'] = modifier_val

    skill_values = {}
    for skill in SKILL_COLUMNS:
        val_str = form_data.get(skill.lower(), '0').strip()
        try:
            val_int = int(val_str) if val_str != '' else 0
        except ValueError:
            return False, f"Wert für {skill} muss numerisch sein.", {}

        if val_int not in (0, 1, 2):
            return False, f"Wert für {skill} muss 0, 1 oder 2 sein.", {}
        skill_values[skill] = val_int

    normalized['skills'] = skill_values
    return True, "", normalized


# -----------------------------------------------------------
# Helper functions to compute global totals across modalities
# -----------------------------------------------------------
def get_global_weighted_count(canonical_id):
    total = 0.0
    for mod in allowed_modalities:
        total += global_worker_data['weighted_counts_per_mod'][mod].get(canonical_id, 0.0)
    return total

def get_global_assignments(canonical_id):
    totals = {skill: 0 for skill in SKILL_COLUMNS}
    totals['total'] = 0
    for mod in allowed_modalities:
        mod_assignments = global_worker_data['assignments_per_mod'][mod].get(canonical_id, {})
        for skill in SKILL_COLUMNS:
            totals[skill] += mod_assignments.get(skill, 0)
        totals['total'] += mod_assignments.get('total', 0)
    return totals

# -----------------------------------------------------------
# Modality-specific work hours & weighted calculations
# -----------------------------------------------------------
def calculate_work_hours_now(current_dt: datetime, modality: str) -> dict:
    d = modality_data[modality]
    if d['working_hours_df'] is None:
        return {}
    df_copy = d['working_hours_df'].copy()
    
    def _calc(row):
        start_dt = datetime.combine(current_dt.date(), row['start_time'])
        end_dt   = datetime.combine(current_dt.date(), row['end_time'])
        if current_dt.time() < row['start_time']:
            return 0.0
        elif current_dt.time() >= row['end_time']:
            return (end_dt - start_dt).total_seconds() / 3600.0
        else:
            return (current_dt - start_dt).total_seconds() / 3600.0

    df_copy['work_hours_now'] = df_copy.apply(_calc, axis=1)
    
    hours_by_canonical = {}
    hours_by_worker = df_copy.groupby('PPL')['work_hours_now'].sum().to_dict()
    
    for worker, hours in hours_by_worker.items():
        canonical_id = get_canonical_worker_id(worker)
        hours_by_canonical[canonical_id] = hours_by_canonical.get(canonical_id, 0) + hours
        
    return hours_by_canonical


# -----------------------------------------------------------
# Data Initialization per modality (based on uploaded Excel)
# -----------------------------------------------------------
def initialize_data(file_path: str, modality: str):
    d = modality_data[modality]
    # Reset all counters for this modality - complete reset
    d['draw_counts'] = {}
    d['skill_counts'] = {skill: {} for skill in SKILL_COLUMNS}
    d['WeightedCounts'] = {}

    # Also reset global counters specific to this modality
    global_worker_data['weighted_counts_per_mod'][modality] = {}
    global_worker_data['assignments_per_mod'][modality] = {}

    with lock:
        try:
            excel_file = pd.ExcelFile(file_path)
            if 'Tabelle1' not in excel_file.sheet_names:
                raise ValueError("Blatt 'Tabelle1' nicht gefunden")

            df = pd.read_excel(excel_file, sheet_name='Tabelle1')

            # Define required columns
            required_columns = ['PPL', 'TIME']
            # Validate Excel structure
            valid, error_msg = validate_excel_structure(df, required_columns)
            if not valid:
                raise ValueError(error_msg)

            # Handle Modifier column
            if 'Modifier' not in df.columns:
                df['Modifier'] = 1.0
            else:
                df['Modifier'] = (
                    df['Modifier']
                    .fillna(1.0)
                    .astype(str)
                    .str.replace(',', '.')
                    .astype(float)
                )

            # Parse TIME into start and end times
            df['start_time'], df['end_time'] = zip(*df['TIME'].map(parse_time_range))

            # Process core skills
            core_skills = ["Normal", "Notfall", "Privat", "Herz", "Msk", "Gyn"]
            for skill in core_skills:
                if skill not in df.columns:
                    df[skill] = 0
                else:
                    df[skill] = df[skill].fillna(0).astype(int)

            # Process optional skills (Herz, Msk, Gyn)
            optional_skills = [s for s in SKILL_COLUMNS if s not in core_skills]
            for skill in optional_skills:
                if skill in df.columns:
                    df[skill] = df[skill].fillna(0).astype(int)

            # Compute shift_duration using the working logic:
            df['shift_duration'] = df.apply(
                lambda row: (
                    datetime.combine(datetime.min, row['end_time']) -
                    datetime.combine(datetime.min, row['start_time'])
                ).total_seconds() / 3600.0,
                axis=1
            )

            # Compute canonical ID for each worker
            df['canonical_id'] = df['PPL'].apply(get_canonical_worker_id)

            # Set column order as desired
            col_order = ['PPL', 'canonical_id', 'Modifier', 'TIME', 'start_time', 'end_time', 'shift_duration']
            desired_order = ["Normal", "Notfall", "Privat", "Herz", "Msk", "Gyn"]
            skill_cols = [skill for skill in desired_order if skill in df.columns]
            col_order = col_order[:4] + skill_cols + col_order[4:]
            df = df[[col for col in col_order if col in df.columns]]

            # Save the DataFrame and compute auxiliary data
            d['working_hours_df'] = df
            d['worker_modifiers'] = df.groupby('PPL')['Modifier'].first().to_dict()
            d['total_work_hours'] = df.groupby('PPL')['shift_duration'].sum().to_dict()
            unique_workers = df['PPL'].unique()
            d['draw_counts'] = {w: 0 for w in unique_workers}

            # Initialize skill counts for all workers
            d['skill_counts'] = {}
            for skill in SKILL_COLUMNS:
                if skill in df.columns:
                    d['skill_counts'][skill] = {w: 0 for w in unique_workers}
                else:
                    d['skill_counts'][skill] = {}

            d['WeightedCounts'] = {w: 0.0 for w in unique_workers}

            # Load info texts from Tabelle2 (if available)
            if 'Tabelle2' in excel_file.sheet_names:
                d['info_texts'] = pd.read_excel(excel_file, sheet_name='Tabelle2')['Info'].tolist()
            else:
                d['info_texts'] = []

        except Exception as e:
            error_message = f"Fehler beim Laden der Excel-Datei für Modality '{modality}': {str(e)}"
            selection_logger.error(error_message)
            selection_logger.exception("Stack trace:")
            raise ValueError(error_message)



# -----------------------------------------------------------
# Active Data Filtering and Weighted-Selection Logic
# -----------------------------------------------------------
def get_active_df_for_role(active_df: pd.DataFrame, role: str):
    role_map = {
        'normal':  'Normal',
        'notfall': 'Notfall',
        'herz':    'Herz',
        'privat':  'Privat',
        'msk':     'Msk',
        'gyn':   'Gyn'
    }
    role_lower = role.lower()
    if role_lower not in role_map:
        role_lower = 'normal'
    primary_column = role_map[role_lower]
    
    fallback_chain = {
        'Normal': [],
        'Notfall': ['Normal'],
        'Herz': ['Notfall', 'Normal'],
        'Privat': ['Notfall', 'Normal'],
        'Msk': ['Notfall', 'Normal'],
        'Gyn': ['Notfall', 'Normal']
    }
    
    if primary_column not in active_df.columns:
        primary_column = 'Normal'
        
    if primary_column in active_df.columns:
        filtered_df = active_df[active_df[primary_column] > 0]
        if not filtered_df.empty:
            return filtered_df, primary_column
    
    if primary_column in fallback_chain:
        for fallback in fallback_chain[primary_column]:
            if fallback in active_df.columns:
                filtered_df = active_df[active_df[fallback] > 0]
                if not filtered_df.empty:
                    return filtered_df, fallback
    
    if 'Normal' in active_df.columns:
        filtered_df = active_df[active_df['Normal'] > 0]
        if not filtered_df.empty:
            return filtered_df, 'Normal'
    
    return active_df.iloc[0:0], primary_column

def get_next_available_worker(current_dt: datetime, role='normal', modality='ct'):
    d = modality_data[modality]
    if d['working_hours_df'] is None:
        selection_logger.info(f"No working hours data for modality {modality}")
        return None

    tnow = current_dt.time()
    active_df = d['working_hours_df'][
        (d['working_hours_df']['start_time'] <= tnow) &
        (d['working_hours_df']['end_time']   >= tnow)
    ]
    
    if active_df.empty:
        selection_logger.info(f"No active workers at time {tnow} for modality {modality}")
        return None

    filtered_df, used_column = get_active_df_for_role(active_df, role)
    
    if filtered_df.empty:
        selection_logger.info(f"No workers found for role {role} (using column {used_column}) at time {tnow}")
        return None
    
    worker_count = len(filtered_df['PPL'].unique())
    selection_logger.info(f"Found {worker_count} workers available for role {role} (using column {used_column})")

    def weighted_ratio(person):
        canonical_id = get_canonical_worker_id(person)
        hours_map = calculate_work_hours_now(current_dt, modality)
        h = hours_map.get(canonical_id, 0)
        w = get_global_weighted_count(canonical_id)
        return w / h if h > 0 else w

    skill1_df = filtered_df[filtered_df[used_column] == 1]
    skill2_df = filtered_df[filtered_df[used_column] == 2]

    if skill1_df.empty and skill2_df.empty:
        selection_logger.info(f"No workers with skill level 1 or 2 found for {used_column}")
        return None

    if not skill1_df.empty and not skill2_df.empty:
        skill1_candidates = skill1_df['PPL'].unique()
        min_ratio_skill1 = min(weighted_ratio(p) for p in skill1_candidates)
        skill2_candidates = skill2_df['PPL'].unique()
        best_person_skill2 = sorted(skill2_candidates, key=lambda p: weighted_ratio(p))[0]
        ratio_skill2 = weighted_ratio(best_person_skill2)
        
        selection_logger.info(f"Skill level 1 candidates: {skill1_candidates}")
        selection_logger.info(f"Skill level 2 candidates: {skill2_candidates}")
        selection_logger.info(f"Best skill 2 person: {best_person_skill2}, ratio: {ratio_skill2}")
        selection_logger.info(f"Min ratio for skill 1: {min_ratio_skill1}")
        
        if ratio_skill2 <= min_ratio_skill1 * 1.25:
            candidate = skill2_df[skill2_df['PPL'] == best_person_skill2].iloc[0]
            selection_logger.info(f"Selected skill 2 candidate: {best_person_skill2}")
            return candidate, used_column
        else:
            best_person_skill1 = sorted(skill1_candidates, key=lambda p: weighted_ratio(p))[0]
            candidate = skill1_df[skill1_df['PPL'] == best_person_skill1].iloc[0]
            selection_logger.info(f"Selected skill 1 candidate: {best_person_skill1}")
            return candidate, used_column

    if not skill1_df.empty:
        best_person = sorted(skill1_df['PPL'].unique(), key=lambda p: weighted_ratio(p))[0]
        candidate = skill1_df[skill1_df['PPL'] == best_person].iloc[0]
        selection_logger.info(f"Selected only skill 1 candidate: {best_person}")
        return candidate, used_column
        
    if not skill2_df.empty:
        best_person = sorted(skill2_df['PPL'].unique(), key=lambda p: weighted_ratio(p))[0]
        candidate = skill2_df[skill2_df['PPL'] == best_person].iloc[0]
        selection_logger.info(f"Selected only skill 2 candidate: {best_person}")
        return candidate, used_column

    selection_logger.info("Logic error: No candidate selected despite having candidates")
    return None

# -----------------------------------------------------------
# Daily Reset: check (for every modality) at >= 07:30
# -----------------------------------------------------------
def check_and_perform_daily_reset():
    now = get_local_berlin_now()
    today = now.date()
    
    if global_worker_data['last_reset_date'] != today and now.time() >= time(7, 30):
        should_reset_global = any(
            os.path.exists(modality_data[mod]['scheduled_file_path']) 
            for mod in allowed_modalities
        )
        if should_reset_global:
            global_worker_data['last_reset_date'] = today
            selection_logger.info("Performed global reset based on modality scheduled uploads.")
        
    for mod, d in modality_data.items():
        if d['last_reset_date'] == today:
            continue
        if now.time() >= time(7, 30):
            if os.path.exists(d['scheduled_file_path']):
                # Validate the scheduled file before loading
                selection_logger.info(f"Validating scheduled file for modality {mod}")
                is_valid, error_msg = validate_uploaded_file(d['scheduled_file_path'])

                if not is_valid:
                    selection_logger.error(f"Scheduled file validation failed for modality {mod}: {error_msg}")
                    # Move invalid file to error folder
                    error_dir = os.path.join(app.config['UPLOAD_FOLDER'], "errors")
                    if not os.path.exists(error_dir):
                        os.makedirs(error_dir)
                    error_file = os.path.join(error_dir, f"{os.path.basename(d['scheduled_file_path'])}_INVALID_{today.strftime('%Y%m%d')}.xlsx")
                    os.rename(d['scheduled_file_path'], error_file)
                    selection_logger.error(f"Invalid scheduled file moved to {error_file}. Keeping old data for modality {mod}.")
                else:
                    # Reset all counters for this modality before initializing new data
                    d['draw_counts'] = {}
                    d['skill_counts'] = {skill: {} for skill in SKILL_COLUMNS}
                    d['WeightedCounts'] = {}

                    initialize_data(d['scheduled_file_path'], mod)
                    # Instead of deleting, move scheduled file to backup folder
                    backup_dir = os.path.join(app.config['UPLOAD_FOLDER'], "backups")
                    if not os.path.exists(backup_dir):
                        os.makedirs(backup_dir)
                    backup_file = os.path.join(backup_dir, os.path.basename(d['scheduled_file_path']))
                    os.rename(d['scheduled_file_path'], backup_file)
                    selection_logger.info(f"Scheduled daily file loaded and moved to backup for modality {mod}.")
                    # 3) Live-Backup sofort aktualisieren
                    backup_dataframe(mod)
                    selection_logger.info(f"Live-backup updated for modality {mod} after daily reset.")

            else:
                selection_logger.info(f"No scheduled file found for modality {mod}. Keeping old data.")
            d['last_reset_date'] = today
            global_worker_data['weighted_counts_per_mod'][mod] = {}
            global_worker_data['assignments_per_mod'][mod] = {}
            
@app.before_request
def before_request():
    check_and_perform_daily_reset()

# -----------------------------------------------------------
# Helper for low-duplication global update
# -----------------------------------------------------------
def _get_or_create_assignments(modality: str, canonical_id: str) -> dict:
    assignments = global_worker_data['assignments_per_mod'][modality]
    if canonical_id not in assignments:
        assignments[canonical_id] = {skill: 0 for skill in SKILL_COLUMNS}
        assignments[canonical_id]['total'] = 0
    return assignments[canonical_id]

def update_global_assignment(person: str, role: str, modality: str) -> str:
    canonical_id = get_canonical_worker_id(person)
    # Get the modifier (default 1.0); now a modifier > 1 means more work, < 1 means less work.
    modifier = modality_data[modality]['worker_modifiers'].get(person, 1.0)
    weight = skill_weights.get(role, 1.0) * (1.0 / modifier) * modality_factors[modality]
  #  weight = skill_weights.get(role, 1.0) * modifier * modality_factors[modality]

    global_worker_data['weighted_counts_per_mod'][modality][canonical_id] = \
        global_worker_data['weighted_counts_per_mod'][modality].get(canonical_id, 0.0) + weight

    assignments = _get_or_create_assignments(modality, canonical_id)
    assignments[role] += 1
    assignments['total'] += 1

    return canonical_id

# -----------------------------------------------------------
# Helper: Live Backup of DataFrame
# -----------------------------------------------------------
def backup_dataframe(modality: str):
    """
    Writes the current working_hours_df for the given modality to a live backup Excel file.
    The backup file will include:
      - "Tabelle1": containing the working_hours_df data without extra columns.
      - "Tabelle2": containing the info_texts (if available).
      
    This version removes the columns 'start_time', 'end_time', and 'shift_duration'.
    """
    d = modality_data[modality]
    if d['working_hours_df'] is not None:
        backup_dir = os.path.join(app.config['UPLOAD_FOLDER'], "backups")
        os.makedirs(backup_dir, exist_ok=True)
        backup_file = os.path.join(backup_dir, f"SBZ_{modality.upper()}_live.xlsx")
        try:
            # Remove unwanted columns from backup
            cols_to_backup = [col for col in d['working_hours_df'].columns
                              if col not in ['start_time', 'end_time', 'shift_duration']]
            df_backup = d['working_hours_df'][cols_to_backup].copy()
            
            with pd.ExcelWriter(backup_file, engine='openpyxl') as writer:
                # Write the filtered DataFrame into sheet "Tabelle1"
                df_backup.to_excel(writer, sheet_name='Tabelle1', index=False)
                # If info_texts are available, write them into sheet "Tabelle2"
                if d.get('info_texts'):
                    df_info = pd.DataFrame({'Info': d['info_texts']})
                    df_info.to_excel(writer, sheet_name='Tabelle2', index=False)
            selection_logger.info(f"Live backup updated for modality {modality} at {backup_file}")
        except Exception as e:
            selection_logger.info(f"Error backing up DataFrame for modality {modality}: {e}")


# -----------------------------------------------------------
# Startup: initialize each modality – zuerst das aktuelle Live-Backup, dann das Default-File
# -----------------------------------------------------------
for mod, d in modality_data.items():
    backup_dir  = os.path.join(app.config['UPLOAD_FOLDER'], "backups")
    backup_path = os.path.join(backup_dir, f"SBZ_{mod.upper()}_live.xlsx")

    # 1. Live-Backup lädt Vorrang
    if os.path.exists(backup_path):
        try:
            initialize_data(backup_path, mod)
            selection_logger.info(f"Initialized {mod.upper()} modality from live-backup: {backup_path}")
            continue
        except Exception as e:
            selection_logger.info(f"Fehler beim Laden des Live-Backups für {mod.upper()}: {e}")
            # Backup defekt --> weiter zum Default-File

    # 2. Default-File als Fallback
    if os.path.exists(d['default_file_path']):
        try:
            initialize_data(d['default_file_path'], mod)
            selection_logger.info(f"Initialized {mod.upper()} modality from default file: {d['default_file_path']}")
        except Exception as e:
            selection_logger.info(f"Fehler beim Laden des Default-Files für {mod.upper()}: {e}")
    else:
        selection_logger.info(f"Kein Default-File und kein Live-Backup gefunden für {mod.upper()}.")

        # Ensure the modality's data structures are in a clean, empty state
        d['working_hours_df'] = None
        d['info_texts'] = []
        d['total_work_hours'] = {}
        d['worker_modifiers'] = {}
        d['draw_counts'] = {}
        d['skill_counts'] = {skill: {} for skill in SKILL_COLUMNS}
        d['WeightedCounts'] = {}
        # Ensure last_reset_date is not set to today if no data loaded
        d['last_reset_date'] = None

# -----------------------------------------------------------
# Routes
# -----------------------------------------------------------
@app.route('/')
def index():
    modality = request.args.get('modality', 'ct').lower()
    if modality not in modality_data:
        modality = 'ct'
    d = modality_data[modality]

    # Determine available specialties based on currently active working hours
    if d['working_hours_df'] is not None:
        tnow = get_local_berlin_now().time()
        active_df = d['working_hours_df'][
            (d['working_hours_df']['start_time'] <= tnow) &
            (d['working_hours_df']['end_time'] >= tnow)
        ]
        available_specialties = {
            skill: (skill in active_df.columns and active_df[skill].sum() > 0)
            for skill in SKILL_COLUMNS
        }
    else:
        available_specialties = {skill: False for skill in SKILL_COLUMNS}

    # Force core skills to always be visible
    for core in ["Normal", "Notfall", "Privat"]:
        available_specialties[core] = True

    return render_template(
        'index.html',
        available_specialties=available_specialties,
        info_texts=d.get('info_texts', []),
        modality=modality
    )

def get_admin_password():
    try:
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)
        return config.get("admin_password", "")
    except Exception as e:
        selection_logger.info("Error loading config.yaml:", e)
        return ""

# --- Create a decorator to protect admin routes:
def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('admin_logged_in'):
            # redirect to login page and pass current modality if needed
            modality = request.args.get('modality', 'ct')
            return redirect(url_for('login', modality=modality))
        return f(*args, **kwargs)
    return decorated

# --- Add a login route:
@app.route('/login', methods=['GET', 'POST'])
def login():
    modality = request.args.get('modality', 'ct')
    error = None
    if request.method == 'POST':
        pw = request.form.get('password', '')
        if pw == get_admin_password():
            session['admin_logged_in'] = True
            return redirect(url_for('upload_file', modality=modality))
        else:
            error = "Falsches Passwort"
    return render_template("login.html", error=error, modality=modality)


@app.route('/logout')
def logout():
    session.pop('admin_logged_in', None)
    modality = request.args.get('modality', 'ct')
    return redirect(url_for('index', modality=modality))


@app.route('/upload', methods=['GET', 'POST'])
@admin_required
def upload_file():
    modality = request.args.get('modality', 'ct').lower()
    if modality not in modality_data:
        modality = 'ct'
    d = modality_data[modality]

    if request.method == 'POST':
        if 'file' not in request.files:
            return jsonify({"error": "Keine Datei ausgewählt"}), 400
        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "Keine Datei ausgewählt"}), 400
        if not file.filename.endswith('.xlsx'):
            return jsonify({"error": "Ungültiger Dateityp. Nur .xlsx Dateien sind erlaubt."}), 400

        scheduled = request.form.get('scheduled_upload', '0')

        try:
            # Save to temporary file first for validation
            import tempfile
            temp_fd, temp_path = tempfile.mkstemp(suffix='.xlsx')
            os.close(temp_fd)

            try:
                file.save(temp_path)

                # Validate the uploaded file
                selection_logger.info(f"Validating uploaded file for modality {modality}")
                is_valid, error_msg = validate_uploaded_file(temp_path)

                if not is_valid:
                    selection_logger.error(f"File validation failed: {error_msg}")
                    os.remove(temp_path)
                    return jsonify({"error": f"Datei-Validierung fehlgeschlagen:\n\n{error_msg}"}), 400

                selection_logger.info(f"File validation successful for modality {modality}")

                # File is valid, move it to the proper location
                if scheduled == '1':
                    # For scheduled uploads
                    if os.path.exists(d['scheduled_file_path']):
                        os.remove(d['scheduled_file_path'])
                    shutil.move(temp_path, d['scheduled_file_path'])
                    selection_logger.info(f"Scheduled file uploaded and validated for modality {modality}")
                    return redirect(url_for('upload_file', modality=modality))
                else:
                    # For immediate uploads, reset all counters BEFORE loading the file
                    d['draw_counts'] = {}
                    d['skill_counts'] = {skill: {} for skill in SKILL_COLUMNS}
                    d['WeightedCounts'] = {}
                    global_worker_data['weighted_counts_per_mod'][modality] = {}
                    global_worker_data['assignments_per_mod'][modality] = {}

                    # Move validated file to default location
                    file_path = d['default_file_path']
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    shutil.move(temp_path, file_path)

                    d['last_uploaded_filename'] = os.path.basename(file_path)
                    initialize_data(file_path, modality)

                    # Update live backup on new upload
                    backup_dataframe(modality)
                    selection_logger.info(f"Immediate file uploaded, validated and loaded for modality {modality}")
                    return redirect(url_for('upload_file', modality=modality))

            except Exception as e:
                # Clean up temp file on any error
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                raise e

        except Exception as e:
            selection_logger.error(f"Error during file upload: {str(e)}")
            return jsonify({"error": f"Fehler beim Hochladen der Datei:\n\n{str(e)}"}), 500

    # GET method: Prepare data for the upload page
    # 1. Debug info table from working_hours_df.
    debug_info = (
        d['working_hours_df'].to_html(index=True)
        if d['working_hours_df'] is not None else "Keine Daten verfügbar"
    )

    # 2. Prepare JSON for timeline usage.
    if d['working_hours_df'] is not None:
        df_for_json = d['working_hours_df'].copy()
        df_for_json['start_time'] = df_for_json['start_time'].apply(
            lambda t: t.strftime('%H:%M:%S') if pd.notnull(t) else ""
        )
        df_for_json['end_time'] = df_for_json['end_time'].apply(
            lambda t: t.strftime('%H:%M:%S') if pd.notnull(t) else ""
        )
        debug_data = df_for_json.to_json(orient='records')
    else:
        debug_data = "[]"

    # 3. Compute per‑skill counts and summed counts per worker.
    skill_counts = {skill: d['skill_counts'].get(skill, {}) for skill in SKILL_COLUMNS}
    sum_counts = {}
    for worker in skill_counts.get("Normal", {}):
        total = sum(skill_counts[skill].get(worker, 0) for skill in SKILL_COLUMNS)
        sum_counts[worker] = total

    # 4. Compute global assignments and weighted counts.
    global_counts = {}
    global_weighted_counts = {}
    for worker in sum_counts.keys():
        canonical = get_canonical_worker_id(worker)
        global_counts[worker] = get_global_assignments(canonical)
        global_weighted_counts[worker] = get_global_weighted_count(canonical)

    # 5. Build the combined stats for a unified table.
    combined_workers = sorted(set(sum_counts.keys()) | set(global_counts.keys()))
    modality_stats = {}
    for worker in combined_workers:
        modality_stats[worker] = {
            skill: skill_counts.get(skill, {}).get(worker, 0)
            for skill in ['Normal', 'Notfall', 'Privat', 'Herz', 'Msk', 'Gyn']
        }
        modality_stats[worker]['total'] = sum(
            modality_stats[worker][skill] for skill in ['Normal', 'Notfall', 'Privat', 'Herz', 'Msk', 'Gyn']
        )

    # 6. Get info texts.
    info_texts = d.get('info_texts', [])

    return render_template(
        'upload.html',
        debug_info=debug_info,
        debug_data=debug_data,
        modality=modality,
        skill_counts=skill_counts, 
        sum_counts=sum_counts,     
        global_counts=global_counts,
        global_weighted_counts=global_weighted_counts,
        combined_workers=combined_workers,
        modality_stats=modality_stats,
        info_texts=info_texts
    )


@app.route('/api/<modality>/<role>', methods=['GET'])
def assign_worker_api(modality, role):
    modality = modality.lower()
    if modality not in modality_data:
        return jsonify({"error": "Invalid modality"}), 400
    return _assign_worker(modality, role)

def _assign_worker(modality: str, role: str):
    try:
        d = modality_data[modality]
        now = get_local_berlin_now()
        selection_logger.info(f"Assignment request: modality={modality}, role={role}, time={now.strftime('%H:%M:%S')}")
        
        with lock:
            result = get_next_available_worker(now, role=role, modality=modality)
            if result is not None:
                candidate, used_column = result
                selection_logger.info(f"Selected worker: {candidate['PPL']} using column: {used_column}")
                
                candidate = candidate.to_dict() if hasattr(candidate, "to_dict") else dict(candidate)
                if "PPL" not in candidate:
                    raise ValueError("Candidate row is missing the 'PPL' field")
                person = candidate['PPL']
                
                d['draw_counts'][person] += 1
                if role in SKILL_COLUMNS:
                    if role not in d['skill_counts']:
                        d['skill_counts'][role] = {}
                    if person not in d['skill_counts'][role]:
                        d['skill_counts'][role][person] = 0
                    d['skill_counts'][role][person] += 1
                    modifier = candidate.get('Modifier', 1.0)
                    d['WeightedCounts'][person] += skill_weights.get(role, 1.0) * modifier * modality_factors[modality]
                
                canonical_id = update_global_assignment(person, role, modality)
                
                skill_counts = {}
                for skill in SKILL_COLUMNS:
                    if skill in d['skill_counts']:
                        skill_counts[skill] = {w: int(v) for w, v in d['skill_counts'][skill].items()}
                    else:
                        skill_counts[skill] = {}

                sum_counts = {}
                for w in skill_counts["Normal"].keys():
                    total = 0
                    for skill in SKILL_COLUMNS:
                        total += skill_counts[skill].get(w, 0)
                    sum_counts[w] = total
                
                global_stats = {}
                for worker in sum_counts.keys():
                    global_stats[worker] = get_global_assignments(get_canonical_worker_id(worker))
                
                result_data = {
                    "Draw Time": now.strftime('%H:%M:%S'),
                    "Assigned Person": person,
                    "Normal": skill_counts["Normal"],
                    "Notfall": skill_counts["Notfall"],
                    "Herz": skill_counts["Herz"],
                    "Privat": skill_counts["Privat"],
                    "Msk": skill_counts["Msk"],
                    "Gyn": skill_counts["Gyn"],
                    "Summe": sum_counts,
                    "Global": global_stats
                }
            else:
                empty_counts = {w: 0 for w in d['draw_counts']}
                skill_counts = {skill: empty_counts.copy() for skill in SKILL_COLUMNS}
                sum_counts = {w: 0 for w in d['draw_counts']}
                
                result_data = {
                    "Draw Time": now.strftime('%H:%M:%S'),
                    "Assigned Person": "Bitte nochmal klicken",
                    "Normal": skill_counts["Normal"],
                    "Notfall": skill_counts["Notfall"],
                    "Herz": skill_counts["Herz"],
                    "Privat": skill_counts["Privat"],
                    "Msk": skill_counts["Msk"],
                    "Gyn": skill_counts["Gyn"],
                    "Summe": sum_counts,
                    "Global": {}
                }
        return jsonify(result_data)
    except Exception as e:
        app.logger.exception("Error in _assign_worker")
        return jsonify({"error": str(e)}), 500

@app.route('/edit_info', methods=['POST'])
def edit_info():
    modality = request.form.get('modality', 'ct').lower()
    if modality not in modality_data:
        modality = 'ct'
    d = modality_data[modality]
    new_info = request.form.get('info_text', '')
    d['info_texts'] = [line.strip() for line in new_info.splitlines() if line.strip()]
    selection_logger.info(f"Updated info_texts for {modality}: {d['info_texts']}")
    return redirect(url_for('upload_file', modality=modality))

@app.route('/download')
def download_file():
    modality = request.args.get('modality', 'ct').lower()
    if modality not in modality_data:
        modality = 'ct'
    d = modality_data[modality]
    return send_from_directory(app.config['UPLOAD_FOLDER'], d['last_uploaded_filename'], as_attachment=True)

@app.route('/download_latest')
def download_latest():
    modality = request.args.get('modality', 'ct').lower()
    if modality not in modality_data:
        return jsonify({"error": "Invalid modality"}), 400
    backup_dataframe(modality)  # always ensure the latest backup is current
    backup_file = os.path.join(app.config['UPLOAD_FOLDER'], "backups", f"SBZ_{modality.upper()}_live.xlsx")
    if os.path.exists(backup_file):
        return send_from_directory(
            os.path.join(app.config['UPLOAD_FOLDER'], "backups"),
            os.path.basename(backup_file),
            as_attachment=True
        )
    else:
        return jsonify({"error": "Backup file unavailable."}), 404

@app.route('/edit', methods=['POST'])
def edit_entry():
    modality = request.form.get('modality', 'ct').lower()
    if modality not in modality_data:
        modality = 'ct'
    d = modality_data[modality]
    idx_str = request.form.get('index')
    person  = request.form['person']
    time_str= request.form['time']
    modifier_str = request.form.get('modifier', '1.0')

    is_valid, error_msg, normalized = validate_manual_entry(person, time_str, modifier_str, request.form)
    if not is_valid:
        flash(error_msg, 'error')
        return redirect(url_for('upload_file', modality=modality))

    new_modifier = normalized['modifier']
    normalized_skills = normalized['skills']
    time_str = normalized['time']
    person = normalized['person']

    with lock:
        if d['working_hours_df'] is None:
            flash("Keine Daten geladen. Bitte zuerst eine Excel-Datei hochladen.", 'error')
            return redirect(url_for('upload_file', modality=modality))

        if idx_str:
            idx = int(idx_str)
            if 0 <= idx < len(d['working_hours_df']):
                old_person = d['working_hours_df'].at[idx, 'PPL']
                old_canonical = get_canonical_worker_id(old_person)
                new_canonical = get_canonical_worker_id(person)
                
                d['working_hours_df'].at[idx, 'PPL'] = person
                d['working_hours_df'].at[idx, 'canonical_id'] = new_canonical
                d['working_hours_df'].at[idx, 'TIME'] = time_str
                d['working_hours_df'].at[idx, 'Modifier'] = new_modifier
                d['worker_modifiers'][person] = new_modifier

                # Ensure all SKILL_COLUMNS exist in the dataframe
                for skill in SKILL_COLUMNS:
                    # Add column if it doesn't exist
                    if skill not in d['working_hours_df'].columns:
                        d['working_hours_df'][skill] = 0

                    d['working_hours_df'].at[idx, skill] = normalized_skills[skill]

                if person != old_person:
                    d['draw_counts'][person] = d['draw_counts'].get(person, 0) + d['draw_counts'].pop(old_person, 0)
                    for skill in SKILL_COLUMNS:
                        # Ensure both old and new persons exist in all skill dictionaries
                        if skill not in d['skill_counts']:
                            d['skill_counts'][skill] = {}
                        if old_person not in d['skill_counts'][skill]:
                            d['skill_counts'][skill][old_person] = 0
                        if person not in d['skill_counts'][skill]:
                            d['skill_counts'][skill][person] = 0

                        d['skill_counts'][skill][person] = d['skill_counts'][skill].get(person, 0) + d['skill_counts'][skill].pop(old_person, 0)
                    d['WeightedCounts'][person] = d['WeightedCounts'].get(person, 0) + d['WeightedCounts'].pop(old_person, 0)
            else:
                flash("Index ist ungültig.", 'error')
                return redirect(url_for('upload_file', modality=modality))
                
        else:
            # This is for adding a new row - similar fixes needed here
            canonical_id = get_canonical_worker_id(person)
            data_dict = {
                'PPL': person,
                'canonical_id': canonical_id,
                'TIME': time_str,
                'Modifier': new_modifier,
            }
            for skill in SKILL_COLUMNS:
                data_dict[skill] = normalized_skills[skill]
            new_row = pd.DataFrame([data_dict])
            
            # Add missing columns to working_hours_df if needed
            for skill in SKILL_COLUMNS:
                if skill not in d['working_hours_df'].columns:
                    d['working_hours_df'][skill] = 0
                    
            d['working_hours_df'] = pd.concat([d['working_hours_df'], new_row], ignore_index=True)
            if person not in d['draw_counts']:
                d['draw_counts'][person] = 0
            for skill in SKILL_COLUMNS:
                if skill not in d['skill_counts']:
                    d['skill_counts'][skill] = {}
                if person not in d['skill_counts'][skill]:
                    d['skill_counts'][skill][person] = 0
            if person not in d['WeightedCounts']:
                d['WeightedCounts'][person] = 0.0
            d['worker_modifiers'][person] = new_modifier

        d['working_hours_df']['start_time'], d['working_hours_df']['end_time'] = zip(*d['working_hours_df']['TIME'].map(parse_time_range))
        d['working_hours_df']['shift_duration'] = d['working_hours_df'].apply(
            lambda row: (datetime.combine(datetime.min, row['end_time']) - datetime.combine(datetime.min, row['start_time'])).total_seconds() / 3600.0,
            axis=1
        )
        d['total_work_hours'] = d['working_hours_df'].groupby('PPL')['shift_duration'].sum().to_dict()

        # Update live backup after editing
        backup_dataframe(modality)

    flash("Eintrag wurde aktualisiert.", 'success')
    return redirect(url_for('upload_file', modality=modality))
@app.route('/delete', methods=['POST'])
def delete_entry():
    modality = request.form.get('modality', 'ct').lower()
    if modality not in modality_data:
        modality = 'ct'
    d = modality_data[modality]
    idx = int(request.form['index'])
    with lock:
        if d['working_hours_df'] is not None and 0 <= idx < len(d['working_hours_df']):
            d['working_hours_df'].at[idx, 'TIME'] = '00:00-00:00'
            d['working_hours_df'].at[idx, 'start_time'], d['working_hours_df'].at[idx, 'end_time'] = parse_time_range('00:00-00:00')
            d['working_hours_df']['shift_duration'] = d['working_hours_df'].apply(
                lambda row: (datetime.combine(datetime.min, row['end_time']) - datetime.combine(datetime.min, row['start_time'])).total_seconds() / 3600.0,
                axis=1
            )
            d['total_work_hours'] = d['working_hours_df'].groupby('PPL')['shift_duration'].sum().to_dict()
            # Update live backup after deletion
            backup_dataframe(modality)
    return redirect(url_for('upload_file', modality=modality))

@app.route('/get_entry', methods=['GET'])
def get_entry():
    modality = request.args.get('modality', 'ct').lower()
    if modality not in modality_data:
        modality = 'ct'
    d = modality_data[modality]

    idx = request.args.get('index', type=int)
    if d['working_hours_df'] is not None and idx is not None and 0 <= idx < len(d['working_hours_df']):
        entry = d['working_hours_df'].iloc[idx]

        # Start building the response with core fields:
        resp = {
            'person':   entry.get('PPL', ''),       # or entry['PPL'] if guaranteed to exist
            'time':     entry.get('TIME', '00:00-00:00'),
            'modifier': entry.get('Modifier', 1.0)
        }

        # Convert skill columns to int safely:
        for skill in SKILL_COLUMNS:
            if skill in entry:
                val = entry[skill]
                # If it's NaN or non-numeric, default to 0
                if pd.isna(val):
                    val = 0
                try:
                    resp[skill.lower()] = int(val)
                except (ValueError, TypeError):
                    resp[skill.lower()] = 0
            else:
                # If the skill column isn't in this row, default 0
                resp[skill.lower()] = 0

        return jsonify(resp)

    # If index is out of range or DataFrame is empty:
    return jsonify({"error": "Ungültiger Index"}), 400



@app.route('/api/quick_reload', methods=['GET'])
def quick_reload():
    modality = request.args.get('modality', 'ct').lower()
    if modality not in modality_data:
        modality = 'ct'
    d = modality_data[modality]
    now = get_local_berlin_now()
    
    # Determine available buttons based on currently active working hours
    available_buttons = {}
    if d['working_hours_df'] is not None:
        tnow = now.time()
        active_df = d['working_hours_df'][
            (d['working_hours_df']['start_time'] <= tnow) &
            (d['working_hours_df']['end_time'] >= tnow)
        ]
        for skill in SKILL_COLUMNS:
            available_buttons[skill.lower()] = bool((skill in active_df.columns) and (active_df[skill].sum() > 0))
    else:
        for skill in SKILL_COLUMNS:
            available_buttons[skill.lower()] = False
            
    # Force these buttons to be available:
    available_buttons["normal"] = True
    available_buttons["notfall"] = True
    available_buttons["privat"] = True
            
    # Rebuild per-skill counts:
    skill_counts = {}
    for skill in SKILL_COLUMNS:
        skill_counts[skill] = d['skill_counts'].get(skill, {})

    # Summation per worker
    sum_counts = {}
    if "Normal" in skill_counts:
        for worker in skill_counts["Normal"].keys():
            total = 0
            for s in SKILL_COLUMNS:
                total += int(skill_counts[s].get(worker, 0))
            sum_counts[worker] = total

    # Global assignments per worker:
    global_stats = {}
    for worker in sum_counts.keys():
        cid = get_canonical_worker_id(worker)
        global_stats[worker] = get_global_assignments(cid)
        
    # Also compute global weighted counts:
    global_weighted_counts = {}
    for worker in sum_counts.keys():
        canonical = get_canonical_worker_id(worker)
        global_weighted_counts[worker] = get_global_weighted_count(canonical)

    return jsonify({
        "Draw Time": now.strftime("%H:%M:%S"),
        "Assigned Person": None,  # quick_reload doesn't assign a new person
        "Normal": skill_counts.get("Normal", {}),
        "Notfall": skill_counts.get("Notfall", {}),
        "Herz": skill_counts.get("Herz", {}),
        "Privat": skill_counts.get("Privat", {}),
        "Msk": skill_counts.get("Msk", {}),
        "Gyn": skill_counts.get("Gyn", {}),
        "Summe": sum_counts,
        "Global": global_stats,
        "GlobalWeighted": global_weighted_counts,
        "available_buttons": available_buttons
    })


@app.route('/timetable')
def timetable():
    modality = request.args.get('modality', 'ct').lower()
    if modality not in modality_data:
        modality = 'ct'
    d = modality_data[modality]
    if d['working_hours_df'] is not None:
        df_for_json = d['working_hours_df'].copy()
        df_for_json['start_time'] = df_for_json['start_time'].apply(lambda t: t.strftime('%H:%M:%S') if pd.notnull(t) else "")
        df_for_json['end_time'] = df_for_json['end_time'].apply(lambda t: t.strftime('%H:%M:%S') if pd.notnull(t) else "")
        debug_data = df_for_json.to_json(orient='records')
    else:
        debug_data = "[]"
    return render_template('timetable.html', debug_data=debug_data, modality=modality)







app.config['DEBUG'] = True

if __name__ == '__main__':
    app.run()

    
    
