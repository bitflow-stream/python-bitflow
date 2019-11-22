# ################ Definition of message header keys #################

# Type of header filter matching
MATCH_KEY = "match"

# Identifies the type of the message.
TYPE_KEY = "type"

# Identifies the related system component for the message.
COMPONENT_KEY = "component"

# ################ Definition of constant values for respective message header keys #################
# ################ Message type value constants #################

# Matching if any rabbitmq header entry matches match the defined filter
MATCH_ALL = "all"

# Matching only if all rabbitmq header entries match the defined filter
MATCH_ANY = "any"

# Constant message type for RCA.
TYPE_RCA = "rca"

# Constant message type for anomaly detection.
TYPE_ANOMALY_DETECTION = "anomaly-detection"

# Constant message type for anomaly analysis.
TYPE_ANOMALY_ANALYSIS = "aa"

# Constant message type for anomaly analysis feedback.
TYPE_ANOMALY_ANALYSIS_FEEDBACK = "aa-feedback"

# ################ Component value constants #################

# Default value if component cannot be determined.
COMPONENT_UNKNOWN = "unknown"

# All possible feedback message types that can be received
ANOMALY_ANALYSIS_FEEDBACK_TYPES = {0: "NO_FEEDBACK", 1: "UNKNOWN_ANOMALY",
                                   2: "NOT_RUNNING", 3: "SUCCESSFUL", 4: "FAILED"}
