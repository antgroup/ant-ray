from abc import ABC, abstractmethod


class Record:
    """Data record in data stream"""

    def __init__(self, value, stream=None):
        self.value = value
        self.stream = stream
        self.trace_timestamp = -1

    def __repr__(self):
        return f"Record(value={self.value}, stream={self.stream})"

    def __eq__(self, other):
        if type(self) is type(other):
            return (self.stream, self.value) == (other.stream, other.value)
        return False

    def __hash__(self):
        return hash((self.stream, self.value))

    def get_trace_timestamp(self):
        return self.trace_timestamp

    def set_trace_timestamp(self, trace_timestamp):
        self.trace_timestamp = trace_timestamp


class KeyRecord(Record):
    """Data record in a keyed data stream"""

    def __init__(self, key, value, stream=None):
        super().__init__(value, stream)
        self.key = key

    def __eq__(self, other):
        if type(self) is type(other):
            return (self.stream, self.key, self.value) ==\
                   (other.stream, other.key, other.value)
        return False

    def __hash__(self):
        return hash((self.stream, self.key, self.value))


class Row(ABC):
    """Sql Row"""

    def __init__(self):
        self.pos = 0
        self.primary_keys = []
        self.props = {}

    @abstractmethod
    def get_field(self, position):
        raise NotImplementedError

    @abstractmethod
    def set_field(self, position, field):
        raise NotImplementedError

    @abstractmethod
    def add_field(self, field):
        raise NotImplementedError

    def get_primary_keys(self):
        return self.primary_keys

    def set_primary_keys(self, primary_keys):
        self.primary_keys = primary_keys

    def get_props(self):
        return self.props

    def set_props(self, props):
        self.props = props

    def set_pos(self, pos):
        self.pos = pos

    def get_pos(self):
        return self.pos

    @abstractmethod
    def get_arity(self):
        raise NotImplementedError


class ObjectRow(Row):
    """Sql Object Row"""

    def __init__(self, size):
        super().__init__()
        self.fields = [None] * size

    def __repr__(self):
        return f"Row(fields={self.fields}, pos={self.pos}, props={self.props})"

    def __eq__(self, other):
        if type(self) is type(other):
            return self.fields == other.fields
        return False

    def __hash__(self):
        return hash(self.fields)

    def get_field(self, position):
        return self.fields[position]

    def set_field(self, position, field):
        self.fields[position] = field

    def add_field(self, field):
        self.set_field(self.pos, field)
        self.pos += 1

    def get_arity(self):
        return len(self.fields)
