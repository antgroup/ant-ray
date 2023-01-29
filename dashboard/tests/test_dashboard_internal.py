from random import randint
from ray.new_dashboard.utils import LimitedCapacityDict
import sys
import pytest


def test_create_dict_without_parameter():
    # initialize without paramter
    with pytest.raises(TypeError):
        LimitedCapacityDict()


def test_create_dict_with_negative_capacity():
    # initialize with incorrect paramter
    with pytest.raises(AssertionError):
        LimitedCapacityDict(-1)


def generate_radom_key_value_pairs(size):
    return [(10 * i + randint(0, 10), i) for i in range(size)]


def test_insert_and_read():
    capacity = 5
    limit_capacity_dict = LimitedCapacityDict(capacity)
    random_key_value_pairs = generate_radom_key_value_pairs(capacity)
    for key, value in random_key_value_pairs:
        limit_capacity_dict[key] = value
        assert limit_capacity_dict[key] == value


def test_update():
    limit_capacity_dict = LimitedCapacityDict(1)
    limit_capacity_dict["a"] = 1
    # Do update
    limit_capacity_dict["a"] = 2
    assert limit_capacity_dict["a"] == 2


def test_insert_more_than_capacity():
    capacity = 5
    limit_capacity_dict = LimitedCapacityDict(capacity)
    random_key_value_pairs = generate_radom_key_value_pairs(capacity + 2)
    for i in range(len(random_key_value_pairs)):
        key, value = random_key_value_pairs[i]
        limit_capacity_dict[key] = value
        if i + 1 <= capacity:
            # Size grows with insertion
            assert len(limit_capacity_dict) == i + 1
        else:
            # Size of the dict shouldn't exceed the capacity
            assert len(limit_capacity_dict) == capacity
            # FIFO pop out
            assert random_key_value_pairs[i - capacity][
                0] not in limit_capacity_dict
    for i in range(
            len(random_key_value_pairs) - capacity,
            len(random_key_value_pairs)):
        key, value = random_key_value_pairs[i]
        # The rest key value pairs should be in the dict
        assert limit_capacity_dict.pop(key) == value
    # Size of the dict should reduce to 0
    assert len(limit_capacity_dict) == 0


def test_reset_dict():
    capacity = 5
    limit_capacity_dict = LimitedCapacityDict(capacity)
    random_key_value_pairs = generate_radom_key_value_pairs(capacity + 2)
    limit_capacity_dict.reset(random_key_value_pairs[:3])
    for kv in random_key_value_pairs[:3]:
        # Items not beyond the capacity limit should all be in the dict
        assert kv[0] in limit_capacity_dict
    limit_capacity_dict.reset(random_key_value_pairs[:1] +
                              random_key_value_pairs[2:])
    # Key(s) not in the new list should be popped out
    assert random_key_value_pairs[1] not in limit_capacity_dict
    # Key(s) exceed capacity size in the new list should be skipped
    assert random_key_value_pairs[0] not in limit_capacity_dict
    # Keys left should be inserted
    for key, value in random_key_value_pairs[2:]:
        assert limit_capacity_dict[key] == value


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
