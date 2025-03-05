import datetime
from typing import List
from logger import AppLogger

logger = AppLogger()

class Patient:
    """A class used to read in, validate, modify, and store patient information from a CSV file. 
    """
    def __init__(self, internal_patient_number, assigning_authority, hospital_case_number, nhs_number,
                 nhs_verification_status, surname, forename, date_of_birth, sex, patient_title, 
                 address_line_1, address_line_2, address_line_3, address_line_4, address_line_5, 
                 postcode, death_indicator, date_of_death, registered_gp_code, ethnic_code, 
                 home_phone, work_phone, mobile_phone, registered_gp, registered_practice):
        """Initializes a Patient object with the provided information.

        Args:
            internal_patient_number (str): The internal patient number.
            assigning_authority (str): The assigning authority.
            hospital_case_number (str): The hospital case number.
            nhs_number (str): The NHS number.
            nhs_verification_status (str): The NHS verification status.
            surname (str): The patient's surname.
            forename (str): The patient's forename.
            date_of_birth (str): The patient's date of birth in YYYYMMDD format.
            sex (str): The patient's sex.
            patient_title (str): The patient's title.
            address_line_1 (str): The first line of the patient's address.
            address_line_2 (str): The second line of the patient's address.
            address_line_3 (str): The third line of the patient's address.
            address_line_4 (str): The fourth line of the patient's address.
            address_line_5 (str): The fifth line of the patient's address.
            postcode (str): The patient's postcode.
            death_indicator (str): The death indicator ('Y' or 'N').
            date_of_death (str): The patient's date of death in YYYYMMDD format.
            registered_gp_code (str): The registered GP code.
            ethnic_code (str): The ethnic code.
            home_phone (str): The home phone number.
            work_phone (str): The work phone number.
            mobile_phone (str): The mobile phone number.
            registered_gp (str): The registered GP.
            registered_practice (str): The registered practice.
        """
        
        logger.log(f"Initializing Patient with internal_patient_number: {internal_patient_number}", "DEBUG")
        self.internal_patient_number = self.validate_length(internal_patient_number, 12)
        self.assigning_authority = 'RX1'  # Hardcoded per guidance
        self.hospital_case_number = self.validate_hospital_case_number(hospital_case_number)
        self.nhs_number = self.validate_nhs_number(nhs_number)
        self.nhs_verification_status = self.validate_length(nhs_verification_status, 2)
        self.surname = self.validate_length(surname, 30)
        self.forename = self.validate_length(forename, 20)
        self.date_of_birth = self.parse_date(date_of_birth, "Date of birth")
        self.sex = self.map_sex(sex)
        
        # Note:
        '''Join to NUH.MBI.DIM_LOOKUPS as DILO on DILO.DIM_LOOKUP = 'DIM_LOOKUP_TITLE' and 
        NUH.MBI.DIM_PATIENT.DIM_LOOKUP_TITLE_ID  = DILO.DIM_LOOKUP_ID. Check against domain rather than any denormalised 
        column on dim_patient, as any updates to the denormalised domain values may not have pulled through. Use Medway full 
        description, but if the value is over 8 characters then use the value in the 10 character description field, unless that 
        is also > 8 characters, in which case use the 5 character abbreviation. **
        '''
        # Do not have access to these lookups - for now, have truncated to 8 characters max - NEEDS FIXING
        self.patient_title = self.validate_length(patient_title, 8)
        self.address = self.format_address([address_line_1, address_line_2, address_line_3, address_line_4, address_line_5], 50)
        self.postcode = self.validate_length(postcode, 10).upper() if postcode else None
        self.death_indicator = self.parse_death_indicator(death_indicator)
        self.date_of_death = self.parse_date(date_of_death, "Date of death")
        self.registered_gp_code = self.validate_length(registered_gp_code, 8)
        
        # Note:
        '''Join to NUH.MBI.DIM_LOOKUPS as DILO on DILO.DIM_LOOKUP = 'DIM_LOOKUP_ETHNIC_ORIGIN' and 
        NUH.MBI.DIM_PATIENT.DIM_LOOKUP_ETHNIC_ORIGIN_ID  = DILO.DIM_LOOKUP_ID. Please note that the supplier wants the NHS code, 
        I assume that this will end up feeding into the MAIN_CODE field in the DIM_LOOKUPS table.***
        '''
        # Do not have access to these lookups - for now, have truncated to 2 characters max - NEEDS FIXING
        self.ethnic_code = self.validate_length(ethnic_code, 2)
        self.home_phone = self.validate_phone(home_phone)
        
        # Leave blank
        self.work_phone = ""
        self.mobile_phone = self.validate_phone(mobile_phone)
        self.registered_gp = registered_gp[:50] if registered_gp else None  # Store description, not code
        
        # Note:
        '''Must exist on the WPE Source table (Source.Source_Code). If it does not exist, 
        load the patient record but do not load this code.
        '''
        # Do not have access to these lookups - for now, have truncated to 10 characters max - NEEDS FIXING
        self.registered_practice = registered_practice[:10] if registered_practice else None  # Must exist in external table

        # Perform additional validation
        if self.date_of_death: 
            self.validate_date_of_death()
        logger.log(f"Patient initialized: {self}", "DEBUG")

    @staticmethod
    def validate_length(value:str, max_length:int):
        """Ensure field does not exceed max_length.

        Args:
            value (str): The value to validate.
            max_length (int): The maximum allowed length.

        Returns:
            str: The validated value.
        """
        return value.strip()[:max_length] if value else None

    def validate_hospital_case_number(self, value:str):
        """Ensure hospital case number is less than or equal to 25 characters.
        Added logging to 'flag them up somehow as Data Quality will need to investigate'.

        Args:
            value (str): The hospital case number.

        Returns:
            str: The validated hospital case number.
        """
        if value:
            if len(value.strip()) > 25:
                logger.log(f"Patient internal number {self.internal_patient_number}: " \
                           f"Hospital number / case note number {value} over 25 chars - notify Data Quality team", "ERROR")
            return value.strip()[:25]
        else: return None    

    def validate_nhs_number(self, value:str):
        """Ensure NHS number is less than or equal to 10 characters.
        Added logging to 'flag them up somehow as Data Quality will need to investigate'.

        Args:
            value (str): The NHS number.

        Returns:
            str: The validated NHS number.
        """
        if value:
            value = value.strip() 
            if not value.isdigit():
                logger.log(f"Patient internal number {self.internal_patient_number}: " \
                           f"NHS number {value} contains non-numeric characters - notify Data Quality team", "ERROR")
            if len(value) > 10:
                logger.log(f"Patient internal number {self.internal_patient_number}: " \
                           f"NHS number {value} over 10 chars - notify Data Quality team", "ERROR")
            return value[:10]
        else: return None 

    def parse_date(self, date_str:str, field:str="unknown"):
        """Checks for correct date formatting: YYYYMMDD
        Added logging to 'flag them up somehow as Data Quality will need to investigate'.

        Args:
            date_str (str): The date string to parse.
            field (str): The field name for logging purposes.

        Returns:
            str: The parsed date string or None if invalid.
        """
        try:
            # NULL, so ignore 
            if ((date_str == "NULL") or (not date_str)):
                return None
            else:
                datetime.datetime.strptime(date_str, "%Y%m%d")
                return date_str
        except ValueError:
            if date_str:
                logger.log(f"Patient internal number {self.internal_patient_number} in field {field}: " \
                           f"Invalid date {date_str} - notify Data Quality team", "ERROR")
            return None
    
    @staticmethod
    def validate_postcode(postcode:str):
        """Ensure postcode is less than or equal to 10 digits, and capitalised.

        Args:
            postcode (str): The postcode to validate.

        Returns:
            str: The validated postcode.
        """
        return postcode.strip()[:10].upper() if postcode else None
        
    @staticmethod
    def parse_death_indicator(value:any):
        """Convert 'Y'/'N' string to a boolean.
        According to doc: 'Y' if DEATH_DTTM contains a value, otherwise 'N'.

        Args:
            value (any): The death indicator value.

        Returns:
            str: 'Y' if value is truthy, otherwise 'N'.
        """
        if value == 'N':
            return value 
        return 'Y' if value else 'N'

    @staticmethod
    def validate_phone(phone:str):
        """Ensure phone number contains only digits and length <= 20.

        Args:
            phone (str): The phone number to validate.

        Returns:
            str: The validated phone number or None if invalid.
        """
        return phone[:20] if phone and phone.isdigit() else None

    @staticmethod
    def format_address(address_list:List[str], max_length:int):
        """Format address fields, ensuring valid length.

        Args:
            address_list (list[str]): The list of address lines.
            max_length (int): The maximum allowed length for each address line.

        Returns:
            list[str]: The formatted address list.
        """
        for line in range(len(address_list)):
            if address_list[line] and address_list[line] != "NULL":
                address_list[line] = " ".join(address_list[line].split())  # Remove excess whitespace between words
                address_list[line] = address_list[line][:max_length]  # Truncate line to maximum length
        return address_list
    
    @staticmethod
    def map_sex(value:str):
        """Map '1' to 'M', '2' to 'F', any other value to 'U'.

        Args:
            value (str): The sex value to map.

        Returns:
            str: 'M', 'F', or 'U' based on the input value.
        """
        if value == '1' or value == 'M' or value == 'male' or value == 'Male':
            return 'M'
        elif value == '2' or value == 'F' or value == 'female' or value == 'Female':
            return 'F'
        return 'U'
    
    def validate_date_of_death(self):
        """Ensure the date of death does not occur before the date of birth, and if a date of death is recorded, ensure that
        the death indicator is set to 'Y'.
        """
        try:
            if datetime.datetime.strptime(self.date_of_death, "%Y%m%d") < datetime.datetime.strptime(self.date_of_birth, "%Y%m%d"):
                logger.log(f"Patient internal number {self.internal_patient_number}: " \
                           f"Date of death {self.date_of_death} is earlier than date of birth {self.date_of_birth}")
        except TypeError as e:
            logger.log(f"Patient internal number {self.internal_patient_number}: "\
                f"Date of death type error: {e}", "ERROR")

        if self.death_indicator.upper() == "N" and self.date_of_death:
            logger.log(f"Patient internal number {self.internal_patient_number}: " \
                  f"Death indicator is 'N' but a date of death {self.date_of_death} has been recorded")


    def __str__(self):
        """Returns a string representation of the Patient object.

        Returns:
            str: A string representation of the Patient object.
        """
        return f"Patient({self.forename} {self.surname}, DOB: {self.date_of_birth}, NHS: {self.nhs_number})"

    