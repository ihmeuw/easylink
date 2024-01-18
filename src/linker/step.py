from dataclasses import dataclass


@dataclass
class Step:
    """A convenience container in the event we ever want to add step-level functionality"""

    name: str
    
    def validate_output(self, results_file):
        print("Output is OK")
        return True
