# Excel File Validation Implementation

## Overview
This document describes the comprehensive Excel file validation system implemented to prevent errors during file uploads and scheduled data loading.

## Problem Statement
Previously, errors in Excel files (e.g., incorrect time formats like "11.15" instead of "11:15", missing columns) were only detected when the files were loaded, causing application crashes and data processing failures.

## Solution
Implemented a multi-layer validation system that validates Excel files **before** they are saved or processed.

## Features Implemented

### 1. Enhanced Validation Function (`validate_excel_structure`)
Location: `app.py:144-251`

Validates the following aspects of Excel files:

#### Required Columns
- **PPL**: Worker names (cannot be empty)
- **TIME**: Time ranges (required)

#### Time Format Validation
- **Format**: Must be `HH:MM-HH:MM` (e.g., "08:00-16:00")
- **Common Error Detection**:
  - Detects dots instead of colons (e.g., "11.15" → error with helpful message)
  - Validates hour range (0-23)
  - Validates minute range (0-59)
  - Checks for proper dash separator
  - Provides row-specific error messages (e.g., "Zeile 5")

#### Modifier Column Validation
- Must be numeric
- Accepts comma or dot as decimal separator
- Must be > 0 and <= 10
- Empty cells are allowed (default to 1.0)

#### Skill Column Validation
- Columns: Normal, Notfall, Privat, Herz, Msk, Gyn
- Values must be 0, 1, or 2
- Must be numeric
- Empty cells are allowed (default to 0)

### 2. File Upload Validation Function (`validate_uploaded_file`)
Location: `app.py:254-285`

- Checks if file can be opened as Excel
- Verifies "Tabelle1" sheet exists
- Checks if sheet is not empty
- Calls `validate_excel_structure` for detailed validation
- Returns clear error messages with specific row numbers

### 3. Upload Endpoint Enhancement
Location: `app.py:794-871`

**Immediate Uploads**:
1. Saves file to temporary location
2. Validates file completely
3. If validation fails:
   - Deletes temporary file
   - Returns HTTP 400 with detailed error message
   - No changes to system state
4. If validation passes:
   - Moves file to proper location
   - Initializes data
   - Updates live backup

**Scheduled Uploads**:
1. Saves file to temporary location
2. Validates file completely
3. If validation fails:
   - Deletes temporary file
   - Returns HTTP 400 with detailed error message
4. If validation passes:
   - Moves file to scheduled location
   - File will be validated again before loading at 07:30

### 4. Scheduled File Loading Enhancement
Location: `app.py:590-626`

When loading scheduled files at 07:30:
1. **Validates file first** before any processing
2. If validation fails:
   - Logs detailed error message
   - Moves invalid file to `uploads/errors/` folder
   - Filename: `SBZ_{MODALITY}_scheduled_INVALID_{DATE}.xlsx`
   - Keeps old data (system continues with previous valid data)
   - Prevents application crash
3. If validation passes:
   - Loads file normally
   - Moves to backup folder
   - Updates live backup

## Error Messages

All error messages are in German and include:
- Specific row numbers (Excel row = validation row + 1 for header)
- Clear description of the problem
- Examples of correct format
- Specific values that caused the error

### Example Error Messages

```
Falsches Zeitformat in Zeile 5: '11.15-16.00' - Verwenden Sie ':' statt '.' (z.B. '11:15' statt '11.15')

Fehlende Spalten: TIME, PPL

Spalte 'Normal' in Zeile 3: Wert muss 0, 1 oder 2 sein (Wert: 5)

Spalte 'PPL' enthält leere Zellen in Zeilen: [2, 5, 8]

Modifier in Zeile 4 muss größer als 0 sein (Wert: 0.0)
```

## File Management

### Directories Created
- `uploads/` - Main upload directory
- `uploads/backups/` - Valid files after processing
- `uploads/errors/` - Invalid scheduled files

### Invalid File Handling
Invalid scheduled files are moved to `uploads/errors/` with timestamp:
- Example: `SBZ_CT_scheduled_INVALID_20251219.xlsx`
- Allows administrators to review and fix errors
- System continues with previous valid data

## Benefits

1. **No More Crashes**: Invalid files are rejected before they can cause errors
2. **Clear Error Messages**: Users know exactly what to fix and in which row
3. **Data Integrity**: Only valid data is loaded into the system
4. **Audit Trail**: Invalid files are preserved in error folder for review
5. **Graceful Degradation**: Scheduled file errors don't crash the system
6. **User-Friendly**: Error messages include examples and suggestions

## Testing

The validation can be tested using `test_validation.py` which includes test cases for:
- Valid files
- Invalid time formats (dots, missing dash, invalid hours/minutes)
- Missing columns
- Empty cells
- Invalid skill values
- Invalid modifier values

Run tests:
```bash
python3 test_validation.py
```

## Logging

All validation events are logged to `logs/selection.log`:
- Validation start
- Validation success
- Validation failures with full error messages
- File movements (to backups or errors folder)

## Usage Notes

1. **File Format**: Excel files must be `.xlsx` format
2. **Sheet Name**: Must contain a sheet named "Tabelle1"
3. **Time Format**: Always use `HH:MM-HH:MM` (colon, not dot)
4. **Required Columns**: PPL and TIME must be present
5. **Row Numbers**: Error messages show Excel row numbers (accounting for header)

## Future Enhancements

Potential improvements:
- Email notifications for invalid scheduled files
- Web interface to view error folder contents
- Automatic correction suggestions
- Bulk validation tool for multiple files
- Export validation report as PDF
