# ########### Anomaly Classification Tags ############
SIMILARITY_HEADER_PREFIX = "similarity-"
RESULT = "aa-result"
RESULT_UNKNOWN = "unknown"
STATUS = "status"
STATUS_TAG_NAME_SIMILARITY = "similarity"
DEFAULT_LEARNING_TAG = "teach-me-please"
BATCH_COUNTER_TAG = "batch-counter"

# ########### Anomaly Detection Tags ############
NORMAL = "normal"
ANOMALY = "anomaly"
UNKNOWN = "unknown"
BINARY_PREDICTION_TAG = "prediction"
BINARY_ACTUAL_TAG = "actual"

# ########### Default ZerOps Tags ############

# Default tag key, set by distributed anomaly experiment controller. Identifies the component where the anomaly
# was injected.
TARGET = "target"

# Split character used to separate target component and injected anomaly.
TARGET_SEP_CHAR = "|"

# cls tag
CLS = "cls"

# Default tag key, set by distributed anomaly experiment controller. Identifies the anomaly type that was injected.
ANOMALY_LABEL= "anomaly-label"

# Default tag key, set by distributed anomaly experiment controller. Identifies the anomaly type that was injected.
ANOMALY_COMPONENT = "anomaly-component"

# Default tag key, set by zerops bitflow collector. Identifies a system component. Follows a hierarchical
# structure: "physical-identifier"/"virtual-identifier"/"service-identifier". A physical node would have the
# form "physical-identifier". A virtual host would have the form "physical-identifier"/"virtual-identifier",
# whereby it is a combination of its identifier and the identifier of the physical node where it is deployed.
COMPONENT = "component"

# Default tag key, set by the zerops bitflow collector. Host name of the physical hypervisor where the collector
# agent was started at.
NODE = "node"

# Default tag key, set by the zerops bitflow collector. Libvirt ID of the VM the metric data belong to.
VM = "vm"

# Default tag key, set by the zerops bitflow collector. Service name of the service component the metric data belong to.
SERVICE = "service";

# Default tag key set by the zerops bitflow collector. Identifies the component, from which the metricsn within the
# sample were collected. Can be either the physical hypervisor itself (identified by its hostname) or one of the
# virtual machines running at the hypervisor (identified by their libvirt ID).
HOST = "host"


