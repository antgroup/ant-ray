import enum


class ControlMessageType(enum.IntEnum):
    UPDATE_CONTEXT = 0
    BROADCAST_PARTIAL_BARRIER = 1
    CLEAR_PARTIAL_BARRIER = 2
    SUSPEND = 3
    RESUME = 4
    REINITIALIZE_OPERATOR = 5
    EXCEPTION_INJECTION = 6
    RESCALE_ROLLBACK = 7
    OPERATOR_COMMAND = 8


class ControlMessage:
    def __init__(self, message_type, message):
        self.message_type = message_type
        self.message = message
