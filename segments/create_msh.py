from logger import AppLogger
import traceback

logger = AppLogger()

def create_msh(messageType, control_id, hl7, current_date):

    # Add MSH Segment
    try:
        # convert the message type to a string replacing the underscore with ^
        messageTypeSegment = str(messageType)
        messageTypeSegment = messageTypeSegment.replace("_", "^")

        hl7.msh.msh_3 = "Sending Application"  # Sending Application
        hl7.msh.msh_4 = "Sending Facility"  # Sending Facility
        hl7.msh.msh_5 = "Receiving Application"  # Receiving Application
        hl7.msh.msh_6 = "Receiving Facility"  # Receiving Facility
        hl7.msh.msh_7 = current_date.strftime("%Y%m%d%H%M")  # Date/Time of Message
        hl7.msh.msh_9 = messageTypeSegment  # Message Type
        hl7.msh.msh_10 = control_id  # Message Control ID
        hl7.msh.msh_11 = "T"  # Processing ID
        hl7.msh.msh_12 = "2.4"  # Version ID
        hl7.msh.msh_15 = "AL"  # Accept Acknowledgment Type
        hl7.msh.msh_16 = "NE"  # Application Acknowledgment Type
        
    except Exception as e:
        print("An AssertionError occurred:", e)
        print(f"Could not create MSH Segment: {e}")
        logger.log(
            f"An error of type {type(e).__name__} occurred. Arguments:\n{e.args}\n\nCould not create MSH segment", "CRITICAL")
        return None
    else:
        return hl7