from enum import Enum
from typing import Union, Optional, List
from ray.util.annotations import PublicAPI
from ray.util.placement_group import PlacementGroup

# "DEFAULT": The default hybrid scheduling strategy
# based on config scheduler_spread_threshold.
# This disables any potential placement group capture.

# "SPREAD": Spread scheduling on a best effort basis.


@PublicAPI(stability="beta")
class PlacementGroupSchedulingStrategy:
    """Placement group based scheduling strategy.

    Attributes:
        placement_group: the placement group this actor belongs to,
            or None if it doesn't belong to any group.
        placement_group_bundle_index: the index of the bundle
            if the actor belongs to a placement group, which may be -1 to
            specify any available bundle.
        placement_group_capture_child_tasks: Whether or not children tasks
            of this actor should implicitly use the same placement group
            as its parent. It is False by default.
    """

    def __init__(self,
                 placement_group: PlacementGroup,
                 placement_group_bundle_index: int = -1,
                 placement_group_capture_child_tasks: Optional[bool] = None):
        if placement_group is None:
            raise ValueError("placement_group needs to be an instance "
                             "of PlacementGroup")

        self.placement_group = placement_group
        self.placement_group_bundle_index = placement_group_bundle_index
        self.placement_group_capture_child_tasks = \
            placement_group_capture_child_tasks


@PublicAPI(stability="beta")
class NodeAffinitySchedulingStrategy:
    """Static scheduling strategy used to run a task or actor
       on a particular node.
    Attributes:
        node_id: the hex id of the node where the task or actor should run.
        soft: whether the scheduler should run the task or actor somewhere else
            if the target node doesn't exist (e.g. the node dies) or is
            infeasible during scheduling.
            If the node exists and is feasible, the task or actor
            will only be scheduled there.
            This means if the node doesn't have the available resources,
            the task or actor will wait indefinitely until resources become
            available.
            If the node doesn't exist or is infeasible, the task or actor
            will fail if soft is False
            or be scheduled somewhere else if soft is True.
    """

    def __init__(self,
                 nodes: List[str],
                 soft: bool,
                 anti_affinity: bool = False):
        origin_node_list = nodes
        if not isinstance(nodes, List):
            origin_node_list = []
            origin_node_list.append(nodes)

        node_id_list = []
        for node_id in origin_node_list:
            # This will be removed once we standardize on
            # node id being hex string.
            if not isinstance(node_id, str):
                node_id_list.append(node_id.hex())
            else:
                node_id_list.append(node_id)

        self.nodes = node_id_list
        self.soft = soft
        self.anti_affinity = anti_affinity


class ActorAffinityOperator(Enum):
    IN = "IN"
    NOT_IN = "NOT_IN"
    EXISTS = "EXISTS"
    DOES_NOT_EXIST = "DOES_NOT_EXIST"


class ActorAffinityMatchExpression:
    """An expression used to represent an actor's affinity.
    Attributes:
        key: the key of label
        operator: IN、NOT_IN、EXISTS、DOES_NOT_EXIST,
                  if EXISTS、DOES_NOT_EXIST, values set []
        values: a list of label value
        soft: whether the scheduler should run the task or actor somewhere else
            if the target node doesn't exist (e.g. the node dies) or is
            infeasible during scheduling.
            If the node exists and is feasible, the task or actor
            will only be scheduled there.
            This means if the node doesn't have the available resources,
            the task or actor will wait indefinitely until resources become
            available.
            If the node doesn't exist or is infeasible, the task or actor
            will fail if soft is False
            or be scheduled somewhere else if soft is True.
    """

    def __init__(self, key: str, operator: ActorAffinityOperator,
                 values: List[str], soft: bool):
        self.key = key
        self.operator = operator
        self.values = values
        self.soft = soft


@PublicAPI(stability="beta")
class ActorAffinitySchedulingStrategy:
    """Static scheduling strategy used to run a task or actor
       on a node that meet a group of actor affinity expressions.
       eg:
            actor_1 = Actor.options(labels={
                "location": "dc-1",
                "version": "1"
            }).remote()
            ray.get(actor_1.value.remote())
            actor_2 = Actor.options(
                scheduling_strategy=ActorAffinitySchedulingStrategy([
                    ActorAffinityMatchExpression(
                        "location", ActorAffinityOperator.IN, ["dc-1"], False)
                ])).remote()
            ray.get(actor_2.value.remote())
    Attributes:
        match_expressions: a list of ActorAffinityMatchExpression
    """

    def __init__(self, match_expressions: List[ActorAffinityMatchExpression]):
        self.match_expressions = match_expressions


SchedulingStrategyT = Union[None, str,  # Literal["DEFAULT", "SPREAD"]
                            PlacementGroupSchedulingStrategy,
                            NodeAffinitySchedulingStrategy,
                            ActorAffinitySchedulingStrategy]
