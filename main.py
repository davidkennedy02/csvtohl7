import csv
from datetime import date, datetime
import os
import chardet
import hl7_utilities
import patientinfo
from logger import AppLogger
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
import queue
import time
import io
import math

INPUT_FOLDER = "input"
HL7_OUTPUT_FOLDER = "output_hl7"
PAS_RECORD_SEPARATOR = chr(30)
BATCH_SIZE = 1000
NUM_WORKERS = max(1, multiprocessing.cpu_count() - 2)
QUEUE_SIZE = 100  # Number of batches to keep in queue
SENTINEL = None   # Sentinel value to signal queue end
LARGE_FILE_THRESHOLD = 100 * 1024 * 1024  # 100MB threshold for large file detection

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


def count_lines(file_path):
    """Count the number of lines in a file efficiently."""
    line_count = 0
    buffer_size = 1024 * 1024  # 1MB buffer
    
    with open(file_path, 'rb') as f:
        buffer = f.read(buffer_size)
        while buffer:
            line_count += buffer.count(b'\n')
            buffer = f.read(buffer_size)
    
    return line_count

def get_file_chunks(file_path, num_chunks=None):
    """
    Divide a file into chunks by line numbers to ensure we never split in the middle of a line.
    
    Args:
        file_path (str): Path to the file
        num_chunks (int, optional): Number of chunks. Defaults to number of workers.
        
    Returns:
        list: List of (start_line, end_line) tuples for each chunk
    """
    if not num_chunks:
        num_chunks = NUM_WORKERS
    
    # Determine file type
    file_type = "csv" if file_path.lower().endswith(".csv") else "PAS"
    
    # Count lines in the file
    logger.log(f"Counting lines in {file_path} to determine chunk sizes...", "INFO")
    total_lines = count_lines(file_path)
    logger.log(f"File has {total_lines} lines", "INFO")
    
    if total_lines == 0:  # Empty file
        return []
    
    # Account for header line in CSV files only
    has_header = file_type == "csv"
    data_lines = total_lines - 1 if has_header else total_lines
    
    if data_lines <= 0:  # Only header or empty file
        return []
    
    # If very few lines, use fewer chunks
    if data_lines < num_chunks * 100:  # Need at least 100 lines per chunk
        num_chunks = max(1, data_lines // 100)
    
    # Calculate lines per chunk
    lines_per_chunk = data_lines // num_chunks
    remainder = data_lines % num_chunks
    
    chunks = []
    # Start after header line for CSV, at beginning for PAS
    start_line = 1 if has_header else 0
    
    for i in range(num_chunks):
        # Add one extra line to early chunks to distribute the remainder
        extra = 1 if i < remainder else 0
        chunk_size = lines_per_chunk + extra
        end_line = start_line + chunk_size
        
        chunks.append((start_line, end_line))
        start_line = end_line
    
    return chunks

def producer_function_chunk(input_file, task_queue, start_line, end_line, chunk_id):
    """Read a chunk of records from file and add to queue in batches."""
    try:
        encoding = detect_encoding(input_file)
        file_basename = os.path.basename(input_file)
        file_type = "csv" if input_file.lower().endswith(".csv") else "PAS"
        logger.log(f"Producer {chunk_id}: Processing lines {start_line} to {end_line} of {file_basename}", "INFO")
        
        batch = []
        batch_counter = 0
        line_number = 0
        
        if file_type == "csv":
            with open(input_file, newline='', encoding=encoding) as file:
                reader = csv.reader(file)
                
                # Skip header row for CSV files
                next(reader)
                line_number = 1
                
                # Skip lines until we reach our starting line
                while line_number < start_line:
                    next(reader)
                    line_number += 1
                
                # Read assigned lines
                while line_number < end_line:
                    try:
                        record = next(reader)
                        batch.append(record)
                        line_number += 1
                        
                        if len(batch) >= BATCH_SIZE:
                            batch_counter += 1
                            task_queue.put((batch, f"{file_basename}:{chunk_id}:{batch_counter}"))
                            batch = []
                    except StopIteration:
                        break
        
        elif file_type == "PAS":
            with open(input_file, newline='', encoding=encoding) as file:
                # For PAS files, read all content first
                content = file.read()
                # Parse the records
                all_records = [record.split(PAS_RECORD_SEPARATOR) 
                             for record in content.split('\r\n') if record.strip()]
                
                # Extract just the records for this chunk
                chunk_records = all_records[start_line:end_line]
                
                # Process them in batches
                for i in range(0, len(chunk_records), BATCH_SIZE):
                    batch_counter += 1
                    batch = chunk_records[i:i+BATCH_SIZE]
                    task_queue.put((batch, f"{file_basename}:{chunk_id}:{batch_counter}"))
        
        # Add any remaining records as a final batch (for CSV files)
        if batch:
            batch_counter += 1
            task_queue.put((batch, f"{file_basename}:{chunk_id}:{batch_counter}"))
            
        logger.log(f"Producer {chunk_id}: Completed chunk - {batch_counter} batches queued", "INFO")
            
    except Exception as e:
        logger.log(f"Producer {chunk_id}: Error processing chunk of {input_file}: {e}", "ERROR")

def process_large_file(file_path):
    """Process a single large file by dividing it into line-based chunks."""
    # Create multiprocessing queues
    task_queue = multiprocessing.Queue(QUEUE_SIZE)
    result_queue = multiprocessing.Queue()
    
    # Start consumer processes
    consumers = []
    logger.log(f"Starting {NUM_WORKERS} consumer processes", "INFO")
    for _ in range(NUM_WORKERS):
        consumer = multiprocessing.Process(
            target=consumer_function, 
            args=(task_queue, result_queue)
        )
        consumer.daemon = True
        consumer.start()
        consumers.append(consumer)
    
    # Calculate chunks for the large file
    chunks = get_file_chunks(file_path)
    num_chunks = len(chunks)
    logger.log(f"Divided file into {num_chunks} chunks for parallel processing", "INFO")
    
    # Start producer processes for each chunk
    producers = []
    for i, (start_line, end_line) in enumerate(chunks):
        producer = multiprocessing.Process(
            target=producer_function_chunk,
            args=(file_path, task_queue, start_line, end_line, i)
        )
        producer.daemon = True
        producer.start()
        producers.append(producer)
    
    # Wait for all producers to finish
    for producer in producers:
        producer.join()
    
    # Add sentinel to queue to signal consumers to stop
    logger.log("All producers finished, signaling consumers to stop", "INFO")
    task_queue.put(SENTINEL)
    
    # Process results from the result queue
    result_count = 0
    while any(consumer.is_alive() for consumer in consumers) or not result_queue.empty():
        try:
            batch_logs = result_queue.get(timeout=0.1)
            result_count += 1
            for log_message, log_level in batch_logs:
                logger.log(log_message, log_level)
        except queue.Empty:
            time.sleep(0.1)
    
    # Clean up and report results
    for consumer in consumers:
        consumer.join(1.0)
        if consumer.is_alive():
            consumer.terminate()
    
    logger.log(f"Processing complete: large file processed in {result_count} result batches", "INFO")

def producer_function(input_file, file_type, task_queue):
    """Read records from file and add to queue in batches."""
    try:
        encoding = detect_encoding(input_file)
        logger.log(f"Producer: Using encoding for {input_file}: {encoding}", "INFO")
        file_basename = os.path.basename(input_file)
        
        batch = []
        batch_counter = 0
        
        if file_type == "csv":
            with open(input_file, newline='', encoding=encoding) as file:
                reader = csv.reader(file)
                headers = next(reader)  # Skip header row
                
                for record in reader:
                    batch.append(record)
                    
                    if len(batch) >= BATCH_SIZE:
                        batch_counter += 1
                        # Put batch in queue with identifier
                        task_queue.put((batch, f"{file_basename}:{batch_counter}"))
                        batch = []
                        
        elif file_type == "PAS":
            with open(input_file, newline='', encoding=encoding) as file:
                content = file.read()
                records = [record.split(PAS_RECORD_SEPARATOR) 
                          for record in content.split('\r\n') if record.strip()]
                
                for i in range(0, len(records), BATCH_SIZE):
                    batch_counter += 1
                    batch = records[i:i+BATCH_SIZE]
                    task_queue.put((batch, f"{file_basename}:{batch_counter}"))
        
        # Add any remaining records as a final batch
        if batch:
            batch_counter += 1
            task_queue.put((batch, f"{file_basename}:{batch_counter}"))
            
        logger.log(f"Producer: Completed reading {file_basename} - {batch_counter} batches queued", "INFO")
            
    except Exception as e:
        logger.log(f"Producer: Error processing {input_file}: {e}", "ERROR")


def consumer_function(task_queue, result_queue):
    """Process records from queue and generate HL7 messages."""
    while True:
        try:
            # Get a batch from the queue with timeout
            item = task_queue.get(timeout=5)
            
            # Check for sentinel value indicating end of processing
            if item is SENTINEL:
                # Put sentinel back for other consumers and exit
                task_queue.put(SENTINEL)
                break
                
            batch, batch_id = item
            logger.log(f"Consumer: Processing batch {batch_id} with {len(batch)} records", "INFO")
            
            # Process the batch and collect logs
            batch_logs = process_record_batch(batch, batch_id)
            
            # Send logs to result queue for the main process to handle
            result_queue.put(batch_logs)
            
        except queue.Empty:
            # Queue is empty, check if we should continue
            continue
        except Exception as e:
            logger.log(f"Consumer: Unexpected error: {e}", "ERROR")


def process_files_in_folder():
    """Process all files using producer-consumer model."""
    # Look for files in input folder
    files = [os.path.join(INPUT_FOLDER, filename) for filename in os.listdir(INPUT_FOLDER)]
    
    # Check if there's just one large CSV file
    if len(files) == 1 and files[0].lower().endswith(".csv"):
        file_path = files[0]
        file_size = os.path.getsize(file_path)
        
        # If file is large (>100MB), use the large file processing method
        if file_size > LARGE_FILE_THRESHOLD:
            logger.log(f"Large CSV file detected ({file_size/1024/1024:.1f} MB). Using optimized processing.", "INFO")
            process_large_file(file_path)
            return
    
    # Otherwise, use the original multi-file processing approach
    # Create multiprocessing queues
    task_queue = multiprocessing.Queue(QUEUE_SIZE)
    result_queue = multiprocessing.Queue()
    
    # Start consumer processes
    consumers = []
    logger.log(f"Starting {NUM_WORKERS} consumer processes", "INFO")
    for _ in range(NUM_WORKERS):
        consumer = multiprocessing.Process(
            target=consumer_function, 
            args=(task_queue, result_queue)
        )
        consumer.daemon = True
        consumer.start()
        consumers.append(consumer)
    
    # Start producer processes (one per file)
    producers = []
    file_count = 0
    
    for filename in os.listdir(INPUT_FOLDER):
        input_file = os.path.join(INPUT_FOLDER, filename)
        file_type = None
        
        if filename.lower().endswith(".csv"):
            logger.log(f"Starting producer for CSV file: {filename}", "INFO")
            file_type = "csv"
        elif filename.lower().endswith(".txt"):
            logger.log(f"Starting producer for PAS file: {filename}", "INFO")
            file_type = "PAS"
        else:
            continue
            
        file_count += 1
        producer = multiprocessing.Process(
            target=producer_function,
            args=(input_file, file_type, task_queue)
        )
        producer.daemon = True
        producer.start()
        producers.append(producer)
    
    # Wait for all producers to finish
    for producer in producers:
        producer.join()
    
    # Add sentinel to queue to signal consumers to stop
    logger.log("All producers finished, signaling consumers to stop", "INFO")
    task_queue.put(SENTINEL)
    
    # Process results from the result queue
    result_count = 0
    while any(consumer.is_alive() for consumer in consumers) or not result_queue.empty():
        try:
            batch_logs = result_queue.get(timeout=0.1)
            result_count += 1
            for log_message, log_level in batch_logs:
                logger.log(log_message, log_level)
        except queue.Empty:
            time.sleep(0.1)
    
    # Clean up and report results
    for consumer in consumers:
        consumer.join(1.0)
        if consumer.is_alive():
            consumer.terminate()
    
    logger.log(f"Processing complete: {file_count} files processed, {result_count} result batches received", "INFO")

if __name__ == "__main__":
    process_files_in_folder()
