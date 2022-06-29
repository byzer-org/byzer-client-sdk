import dataclasses
import json
import uuid
from abc import ABCMeta, abstractmethod
from typing import Optional


class BaseNode(metaclass=ABCMeta):
    @abstractmethod
    def table_name(self) -> str:
        pass

    @abstractmethod
    def named_table_name(self, s: str) -> "BaseNode":
        pass

    @abstractmethod
    def tag(self, s: str) -> "BaseNode":
        pass

    @abstractmethod
    def end(self) -> "tech.mlsql.byzer_client_sdk.python_lang.generator.byzer.Byzer":
        pass

    @abstractmethod
    def options(self) -> "Options":
        pass

    @abstractmethod
    def to_block(self) -> str:
        pass

    @abstractmethod
    def get_tag(self) -> Optional[str]:
        pass

    @abstractmethod
    def to_json(self) -> str:
        pass

    @abstractmethod
    def from_json(self, s: str) -> "BaseNode":
        pass


class OptionValue(object):
    def __init__(self, value: str, quote_str: Optional[str]):
        self.value = value
        self.quote_str = quote_str


class Options(object):
    def __init__(self, parent: BaseNode):
        self.parent = parent
        self.o: dict[str, OptionValue] = {}

    def add(self, name: str, value: str) -> "Options":
        self.o[name] = OptionValue(value, None)
        return self

    def add_with_quoted_str(self, name: str, value: OptionValue) -> "Options":
        self.o[name] = value
        return self

    def end(self) -> BaseNode:
        return self.parent

    def items(self) -> dict[str, OptionValue]:
        return self.o

    def to_fragment(self) -> str:
        temp = []
        for k, v in self.o.items():
            if v.quote_str is None:
                if v.value is None:
                    temp.append(f"`{k}`=\"\"")
                else:
                    temp.append(f"`{k}`='''{v.value}'''")
            else:
                temp.append(f"`{k}`={v.quote_str}{v.value}{v.quote_str}")
        opts = " and ".join(temp)
        if len(self.o) != 0:
            return f"where {opts}"
        else:
            return ""


class MetaMeta(object):
    def __init__(self, name: str):
        self.name = name


class RawMeta:
    def __init__(self, __meta: MetaMeta, _tag: Optional[str], _is_ready: bool, _autogen_table_name: str, _code: str,
                 _table_name: str):
        self.__meta = __meta
        self._tag = _tag
        self._is_ready = _is_ready
        self._autogen_table_name = _autogen_table_name
        self._code = _code
        self._table_name = _table_name


@dataclasses.dataclass
class LoadMeta(object):
    _meta: MetaMeta
    _tag: Optional[str]
    _is_ready: bool
    _autogen_table_name: str
    _table_name: str
    _format: Optional[str]
    _path: Optional[str]
    _options: dict[str, OptionValue]


class Load(BaseNode):
    def __init__(self, parent: "tech.mlsql.byzer_client_sdk.python_lang.generator.byzer.Byzer"):
        self._is_ready = False
        self._autogen_table_name = str(uuid.uuid4())
        self._table_name = self._autogen_table_name
        self._format: Optional[str] = None
        self._path: Optional[str] = None
        self._options: Options = Options(self)
        self._tag: Optional[str] = None
        self._parent = parent

    def format(self, s: str) -> "Load":
        self._format = s
        return self

    def path(self, s: str) -> "Load":
        self._path = s
        return self

    def table_name(self) -> str:
        return self._table_name

    def named_table_name(self, s: str) -> BaseNode:
        self._table_name = s
        return self

    def tag(self, s: str) -> BaseNode:
        self._tag = s
        return self

    def end(self) -> "tech.mlsql.byzer_client_sdk.python_lang.generator.byzer.Byzer":
        self._is_ready = True
        return self._parent

    def options(self) -> Options:
        return self._options

    def to_block(self) -> str:
        return f"load {self._format}.`{self._path}` {self._options.to_fragment()} as {self._table_name};"

    def get_tag(self) -> Optional[str]:
        return self._tag

    def from_json(self, s: str) -> "BaseNode":
        v = json.loads(s)
        self._tag = v._tag
        self._is_ready = v._is_ready
        self._table_name = v._table_name
        self._format = v._format
        self._path = v._path
        self._options = Options(self)
        for key, value in self.options.items():
            self.options.add_with_quoted_str(key, value)
        return self

    def to_json(self) -> str:
        LoadMeta(
            _meta=MetaMeta(""),
            _tag=self._tag,
            _is_ready=self._is_ready,
            _autogen_table_name=self._autogen_table_name,
            _table_name=self._table_name,
            _format=self._format,
            _path=self._path,
            _options=self._options.items()
        )
        return self
