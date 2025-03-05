import csv
from datetime import date, datetime
import os
import chardet
import hl7_utilities
import patientinfo
from logger import AppLogger

INPUT_FOLDER = "input"
HL7_OUTPUT_FOLDER = "output_hl7"
PAS_RECORD_SEPARATOR = chr(30)

logger = AppLogger()


def calculate_age(birth_date):
    """Calculate the current age from a date of birth.

    Args:
        birth_date (datetime | str): The date of birth used to calculate age.

    Returns:
        int: The current age derived from the date of birth.
    """
    today = date.today()
    if type(birth_date) == str:
        birth_date = datetime.strptime(birth_date, "%Y%m%d")
    age = (
        today.year
        - birth_date.year
        - ((today.month, today.day) < (birth_date.month, birth_date.day))
    )
    return age


def detect_encoding(file_path):
    """Detect the encoding of a file."""
    with open(file_path, 'rb') as file:
        raw_data = file.read()
    result = chardet.detect(raw_data)
    return result['encoding']


def process_file(input_file, file_type):
    """Processes a single file based on its type (CSV or PAS)."""
    encoding = 'utf-8'
    try:
        logger.log(f"Attempting to read {input_file} with utf-8 encoding", "INFO")
        if file_type == "csv":
            with open(input_file, newline='', encoding=encoding) as file:
                reader = csv.reader(file)
                headers = next(reader)  # Read the header row
                records = list(reader)  # Read the entire content into memory
        elif file_type == "PAS":
            with open(input_file, newline='', encoding=encoding) as file:
                content = file.read()
                records = [record.split(PAS_RECORD_SEPARATOR) for record in content.split('\r\n') if record.strip()]
    except UnicodeDecodeError:
        logger.log(f"utf-8 encoding failed for {input_file}, attempting to detect encoding", "WARNING")
        try:
            encoding = detect_encoding(input_file)
            logger.log(f"Detected encoding for {input_file}: {encoding}", "INFO")
            if file_type == "csv":
                with open(input_file, newline='', encoding=encoding) as file:
                    reader = csv.reader(file)
                    headers = next(reader)  # Read the header row
                    records = list(reader)  # Read the entire content into memory
            elif file_type == "PAS":
                with open(input_file, newline='', encoding=encoding) as file:
                    content = file.read()
                    records = [record.split(PAS_RECORD_SEPARATOR) for record in content.split('\r\n') if record.strip()]
        except Exception as e:
            logger.log(f"Failed to read {input_file} with detected encoding. Error: {e}", "ERROR")
            print(f"Failed to read {input_file} with detected encoding. Error: {e}")
            return
    
    for record in records:
        
        patient_data = {
            'internal_patient_number': record[0],
            'assigning_authority': record[1],
            'hospital_case_number': record[2],
            'nhs_number': record[3],
            'nhs_verification_status': record[4],
            'surname': record[5],
            'forename': record[6],
            'date_of_birth': record[7],
            'sex': record[8],
            'patient_title': record[9],
            'address_line_1': record[10],
            'address_line_2': record[11],
            'address_line_3': record[12],
            'address_line_4': record[13],
            'address_line_5': record[14],
            'postcode': record[15],
            'death_indicator': record[16],
            'date_of_death': record[17],
            'registered_gp_code': record[18],
            'ethnic_code': record[19],
            'home_phone': record[20],
            'work_phone': record[21],
            'mobile_phone': record[22],
            'registered_gp': record[23],
            'registered_practice': record[24]
        }
        
        patient_info = patientinfo.Patient(**patient_data)
        
        # Minor exclusion checks
        if not patient_info.surname:
            logger.log("Skipping patient - missing required surname", "WARNING")
        elif patient_info.date_of_birth and (patient_info.date_of_death is None) \
                                    and (calculate_age(patient_info.date_of_birth) > 112):
                                        logger.log("Skipping patient - no DOD, and age > 112", "INFO")
        elif patient_info.death_indicator and patient_info.date_of_death \
                                    and (patient_info.death_indicator == 'Y') \
                                    and (calculate_age(patient_info.date_of_death) > 2):
                                        logger.log("Skipping patient - dod > 2 years ago", "INFO")
        else:
            # Create the A28 HL7 message, and save within the `HL7_OUTPUT_FOLDER`
            hl7_message = hl7_utilities.create_adt_message(patient_info=patient_info, event_type="A28")
            if hl7_message:
                hl7_utilities.save_hl7_message_to_file(hl7_message=hl7_message, hl7_folder_path=HL7_OUTPUT_FOLDER)


def process_files_in_folder():
    """Processes all files in the target folders based on their extensions."""
    for filename in os.listdir(INPUT_FOLDER):
        input_file = os.path.join(INPUT_FOLDER, filename)
        if filename.lower().endswith(".csv"):
            process_file(input_file, "csv")
        elif filename.lower().endswith(".txt"):
            process_file(input_file, "PAS")


if __name__ == "__main__":
    process_files_in_folder()
