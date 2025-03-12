import csv
from datetime import date, datetime
import os
import chardet
import hl7_utilities
import patientinfo
from logger import AppLogger
import multiprocessing
from concurrent.futures import ProcessPoolExecutor

INPUT_FOLDER = "input"
HL7_OUTPUT_FOLDER = "output_hl7"
PAS_RECORD_SEPARATOR = chr(30)
BATCH_SIZE = 1000
NUM_WORKERS = max(1, multiprocessing.cpu_count() - 2)

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


def process_record_batch(batch, batch_id):
    """Process a batch of patient records and generate HL7 messages."""
    valid_messages = []
    batch_log = []
    
    for record in batch:
        try:
            # Validate record length before accessing indices
            if len(record) < 25:
                batch_log.append((f"Skipping record - insufficient data fields (found {len(record)}, need 25)", "WARNING"))
                continue
                
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
            
            # Unpack into Patient object
            patient_info = patientinfo.Patient(**patient_data)
            
            # Minor exclusion checks
            if not patient_info.surname:
                batch_log.append((f"Skipping patient {patient_info.internal_patient_number} - missing required surname", "WARNING"))
            elif patient_info.date_of_birth and (patient_info.date_of_death is None) \
                                        and (calculate_age(patient_info.date_of_birth) > 112):
                batch_log.append((f"Skipping patient {patient_info.internal_patient_number} - no DOD, and age > 112", "INFO"))
            elif patient_info.death_indicator and patient_info.date_of_death \
                                        and (patient_info.death_indicator == 'Y') \
                                        and (calculate_age(patient_info.date_of_death) > 2):
                batch_log.append((f"Skipping patient {patient_info.internal_patient_number} - dod > 2 years ago", "INFO"))
            else:
                # Create the HL7 message and collect in batch
                hl7_message = hl7_utilities.create_adt_message(patient_info=patient_info, event_type="A28")
                if hl7_message:
                    valid_messages.append(hl7_message)
        except Exception as e:
            batch_log.append((f"Error processing record: {e}", "ERROR"))
            
    if valid_messages:
        try:
            hl7_utilities.save_hl7_messages_batch(
                hl7_messages=valid_messages, 
                hl7_folder_path=HL7_OUTPUT_FOLDER, 
                batch_id=batch_id
            )
        except Exception as e:
            batch_log.append((f"Error saving batch {batch_id}: {e}", "ERROR"))
    
    return batch_log


def process_file_streaming(input_file, file_type):
    """Process a file using streaming to avoid loading all records into memory."""
    encoding = 'utf-8'
    
    try:
        # Attempt to detect encoding first to avoid double file reading
        encoding = detect_encoding(input_file)
        logger.log(f"Using encoding for {input_file}: {encoding}", "INFO")
        
        batches = []
        batch_counter = 0
        
        if file_type == "csv":
            with open(input_file, newline='', encoding=encoding) as file:
                reader = csv.reader(file)
                headers = next(reader)  # Skip header row
                
                # Process in batches
                batch = []
                for i, record in enumerate(reader):
                    batch.append(record)
                    
                    if len(batch) >= BATCH_SIZE:
                        batch_counter += 1
                        batches.append((batch, batch_counter))
                        batch = []
                        
                if batch:
                    batch_counter += 1
                    batches.append((batch, batch_counter))
        
        elif file_type == "PAS":
            with open(input_file, newline='', encoding=encoding) as file:
                content = file.read()
                records = [record.split(PAS_RECORD_SEPARATOR) for record in content.split('\r\n') if record.strip()]
                
                # Process in batches
                for i in range(0, len(records), BATCH_SIZE):
                    batch = records[i:i+BATCH_SIZE]
                    batch_counter += 1
                    batches.append((batch, batch_counter))
        
        # Process batches in parallel
        logger.log(f"Starting parallel processing with {NUM_WORKERS} workers for {len(batches)} batches", "INFO")
        
        # Include filename in context for better logging
        file_basename = os.path.basename(input_file)
        
        with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
            futures = []
            for batch, batch_id in batches:
                futures.append(executor.submit(process_record_batch, batch, f"{file_basename}:{batch_id}"))
            
            # Process logs from completed tasks
            for future in futures:
                batch_logs = future.result()
                for log_message, log_level in batch_logs:
                    logger.log(log_message, log_level)
        
        logger.log(f"Completed processing {input_file}", "INFO")
    
    except Exception as e:
        logger.log(f"Failed to process {input_file}. Error: {e}", "ERROR")


def process_files_in_folder():
    """Processes all files in the target folders using the streaming approach."""
    for filename in os.listdir(INPUT_FOLDER):
        input_file = os.path.join(INPUT_FOLDER, filename)
        if filename.lower().endswith(".csv"):
            logger.log(f"Processing CSV file: {filename}", "INFO")
            process_file_streaming(input_file, "csv")
        elif filename.lower().endswith(".txt"):
            logger.log(f"Processing PAS file: {filename}", "INFO")
            process_file_streaming(input_file, "PAS")


if __name__ == "__main__":
    process_files_in_folder()
