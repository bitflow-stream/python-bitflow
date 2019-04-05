from bitflow.processingstep import *


class DoNothing(ProcessingStep):

    # Description and name are mandatory for the classes to appear in the -capabilites command of python-bitflow
    __description__ = "This class does nothing except forwarding samples and providing in-code comments."
    __name__ = "DoNothing"

    # Init-function needs to be implemented: Is called once when the pipeline is created
    # Here we create an init function with one parameter which is also used within the example-script.bf
    def __init__(self, param1: str):
        # Call to Superclass(ProcessingStep) necessary
        super().__init__()

        # Save the given param in the object
        self.param1 = param1

    # This is an example of an optional function
    def do_nothing(self):
        print("My only parameter is: " + self.param1)

    # Execute-function needs to be implemented: Is called on every sample
    def execute(self, sample):
        # Do nothing (as the Class-Name suggests) / Only print the content of our given parameter
        self.do_nothing()

        # This writes the (in this case untouched) sample into the pipeline where the next step will receive it
        self.write(sample)

    # On_close-function needs to be implemented: Is called once when the pipeline is closed
    def on_close(self):
        # Do Nothing inside here as we have not created any objects which need to be closed in the end.
        # Examples for such objects would be files or plots which need to be closed before the pipeline can be closed.

        # Call to Superclass(ProcessingStep) necessary
        super().on_close()
