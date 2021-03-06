import logging


class ProcessingStep:
    """Abstract interface class for implementing processing steps"""
    __name__ = "abstract-processing-step"
    __description__ = "No description provided"

    def __init__(self):
        """Subclasses should not use the constructor for setup tasks. Use the initialize() method instead."""
        self.context = None  # Will be initialized in initialize method

    def initialize(self, context):
        """Set up the processing step. This can include allocation of system-wide resources such as files or network connections.
        If required, spawn subprocesses or Threads. The stored context object can be used to output samples."""
        self.context = context

    def handle_sample(self, sample):
        """Handle a received sample"""
        pass

    def cleanup(self):
        """Clean up and prepare shutdown. The process will terminate shortly afterwards.
        Any parallel tasks or processes must be stopped before returning from this method."""
        pass

    def output(self, sample):
        self.context.output_sample(sample)

    @classmethod
    def get_step_name(cls):
        if hasattr(cls, "step_name"):
            return cls.step_name
        return cls.__name__


class BitflowContext:

    def __init__(self, channel):
        self.channel = channel

    def output_sample(self, sample):
        self.channel.output_sample(sample)


class BitflowRunner:

    def __init__(self):
        self.running = True

    def run(self, step, channel):
        logging.info("Initializing step {}".format(step))
        step.initialize(BitflowContext(channel))

        logging.info("Starting to receive samples...")
        while self.running:
            sample = channel.read_sample()
            if sample is None:  # Signifies end of the input stream
                break
            step.handle_sample(sample)

        # We are shutting down. Last thing to do: let the processing step clean up.
        step.cleanup()
        channel.close()

    def shutdown(self):
        self.running = False
