# This file contains the code to create the PID segment of the HL7 message
from logger import AppLogger
from patientinfo import Patient

logger = AppLogger()

def create_pid(patient_info:Patient, hl7):
    """Creates a PID segment for the HL7 message, 
    requires a patient_info object and the hl7 message"""
    try:
        hl7.pid.pid_1 = "1"
        if patient_info.internal_patient_number : hl7.pid.pid_3           = patient_info.internal_patient_number
        if patient_info.hospital_case_number    : hl7.pid.pid_3.pid_3_1   = patient_info.hospital_case_number
        if patient_info.nhs_number              : hl7.pid.pid_3.pid_3_2   = patient_info.nhs_number
        if patient_info.assigning_authority     : hl7.pid.pid_3.pid_3_4   = patient_info.assigning_authority
        if patient_info.nhs_verification_status : hl7.pid.pid_3.pid_3_5   = patient_info.nhs_verification_status
        if patient_info.registered_gp_code      : hl7.pid.pid_3.pid_3_6   = patient_info.registered_gp_code
        if patient_info.registered_gp           : hl7.pid.pid_3.pid_3_7   = patient_info.registered_gp
        if patient_info.registered_practice     : hl7.pid.pid_3.pid_3_8   = patient_info.registered_practice 
        if patient_info.surname                 : hl7.pid.pid_5.pid_5_1   = patient_info.surname
        if patient_info.forename                : hl7.pid.pid_5.pid_5_2   = patient_info.forename
        if patient_info.patient_title           : hl7.pid.pid_5.pid_5_3   = patient_info.patient_title
        if patient_info.date_of_birth           : hl7.pid.pid_7           = patient_info.date_of_birth
        if patient_info.sex                     : hl7.pid.pid_8           = patient_info.sex
        if patient_info.address[0]              : hl7.pid.pid_11.pid_11_1 = patient_info.address[0]
        if patient_info.address[1]              : hl7.pid.pid_11.pid_11_2 = patient_info.address[1]
        if patient_info.address[2]              : hl7.pid.pid_11.pid_11_3 = patient_info.address[2]
        if patient_info.address[3]              : hl7.pid.pid_11.pid_11_4 = patient_info.address[3]
        if patient_info.address[4]              : hl7.pid.pid_11.pid_11_5 = patient_info.address[4]
        if patient_info.postcode                : hl7.pid.pid_11.pid_11_6 = patient_info.postcode
        if patient_info.home_phone              : hl7.pid.pid_13.pid_13_1 = patient_info.home_phone
        if patient_info.work_phone              : hl7.pid.pid_13.pid_13_2 = patient_info.work_phone
        if patient_info.mobile_phone            : hl7.pid.pid_13.pid_13_3 = patient_info.mobile_phone
        if patient_info.ethnic_code             : hl7.pid.pid_22          = patient_info.ethnic_code
        if patient_info.date_of_death           : hl7.pid.pid_29          = patient_info.date_of_death
        if patient_info.death_indicator         : hl7.pid.pid_30          = patient_info.death_indicator
    
    except Exception as e:
        print("An AssertionError occurred:", e)
        print(f"Could not create MSH Segment: {e}")
        logger.log(
            f"An error of type {type(e).__name__} occurred. Arguments:\n{e.args}\n\nCould not create MSH segment", "CRITICAL")
        return None    
    else:
        return hl7