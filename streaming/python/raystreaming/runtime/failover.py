class Barrier:
    """
    barrier
    """

    def __init__(self, id):
        self.id = id

    def __str__(self):
        return "Barrier [id:%s]" % self.id


class PartialBarrier:
    def __init__(self, global_checkpoint_id, partial_checkpoint_id):
        self.global_checkpoint_id = global_checkpoint_id
        self.partial_checkpoint_id = partial_checkpoint_id

    def __str__(self):
        return f"PartialBarrier [global_checkpoint_id:" \
               f"{self.global_checkpoint_id}, " \
               f"partial_checkpoint_id:{self.partial_checkpoint_id}]"


class OpCheckpointInfo:
    """
    operator checkpoint info
    """

    def __init__(self,
                 operator_point=None,
                 input_points=None,
                 output_points=None,
                 checkpoint_id=None):
        if input_points is None:
            input_points = {}
        if output_points is None:
            output_points = {}
        self.operator_point = operator_point
        self.input_points = input_points
        self.output_points = output_points
        self.checkpoint_id = checkpoint_id

    def __str__(self):
        return "Checkpoint id : {}, input points : {}, output points : {}." \
            .format(self.checkpoint_id, self.input_points, self.output_points)
