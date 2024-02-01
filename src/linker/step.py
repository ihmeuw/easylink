from typing import Callable, List, Dict
from dataclasses import dataclass
from pathlib import Path


@dataclass    
class InputSlot:
    env_name: str
    dir_name: str
    filepaths: List[str]
    prev_output: bool
    
    @property
    def bindings(self):
        return {path: Path(self.dir_name) / Path(path).name for path in self.filepaths}
    
    def add_bindings(self, paths: List[Path]):
        self.filepaths.extend(paths)
    
class Step:
    """Steps contain information about the purpose of the interoperable elements of the sequence called a *Pipeline* and how those elements relate to one another.
    In turn, steps are implemented by Implementations. In a sense, steps contain metadata about the implementations to which they relate."""
    def __init__(self, name: str, validate_output: Callable, input_slots: Dict = {}):
        self.name = name
        self.validate_output = validate_output
        self.input_slots = {input_slot_name: InputSlot(input_slot_name, **slot_params) for input_slot_name, slot_params in input_slots.items()}
    
    @property
    def input_data_bindings(self):
        all_bindings = {}
        for slot in self.input_slots.values():
            all_bindings.update(slot.bindings)
        return all_bindings
    
    def add_bindings(self, input_slot_name: str, paths: List[Path]):
        self.input_slots[input_slot_name].add_bindings(paths)
    
    def add_bindings_from_prev(self, paths: List[Path]):
        prev_slots = [slot for slot in self.input_slots.values() if slot.prev_output]
        for slot in prev_slots:
            slot.add_bindings(paths)
        