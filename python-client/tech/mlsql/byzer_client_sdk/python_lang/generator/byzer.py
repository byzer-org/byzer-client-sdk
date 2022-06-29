from tech.mlsql.byzer_client_sdk.python_lang.generator.builder import BaseNode, Load
from typing import List


class Byzer(object):
    def __init__(self):
        self.blocks: List[BaseNode] = []

    def load(self) -> Load:
        v = Load(self)
        self.blocks.append(v)
        return v

    def to_script(self) -> str:
        return "\n".join([block.to_block() for block in self.blocks])
