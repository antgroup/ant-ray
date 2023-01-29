import json

data = None
# 按照globalindex索引worker
workers = {}
names = locals()


def load_realtime_debug_infos(path):
    global data
    global workers
    global names
    print("Load {}", path)
    with open(path, "r") as f:
        data = json.load(f)

    for key in data.keys():
        worker = data[key]
        globalindex = key.split("|")[1]
        # 定义w+index的全局变量，如w1 w2
        names["w" + str(globalindex)] = worker
        # print("Add global {}", "w" + str(globalindex))

        if "producer" in worker:
            channels = worker["producer"]["channels"]
            for channel in channels:
                channel["peer_global_index"] = int(
                    channel["channel_id"].split("-")[1])
                worker[channel["channel_id"]] = channel

        if "consumer" in worker:
            channels = worker["consumer"]["channels"]
            for channel in channels:
                channel["peer_global_index"] = int(
                    channel["channel_id"].split("-")[0])
                worker[channel["channel_id"]] = channel
        workers[globalindex] = worker


# help函数
def help():
    pass


# 检查所有worker的debug info是不是都获取成功了，如果失败表明JobMaster无法call到worker
def check_health():
    # 无法获得profiling info的worker，一般是ray call不通
    unhealthy_workers = []
    # 尚未初始化的worker，它的producer和consumer都是空的，一般是JobMaster没有调用worker的rollback
    uninitialized_workers = []
    # 正在执行processor rollback的worker，此时在做一些初始化，比如State
    rolling_back_workers = []
    for key in data:
        worker = data[key]
        if worker["fetch_debug_info_success"] == "false":
            unhealthy_workers.append(key)
        elif "producer" not in worker and "consumer" not in worker:
            uninitialized_workers.append(key)
        elif "rollbacking" not in worker or worker["rollbacking"]:
            rolling_back_workers.append(key)
    if len(unhealthy_workers) > 0:
        print("The following workers are unhealthy: {}".format(
            unhealthy_workers))
    if len(uninitialized_workers) > 0:
        print("The following workers are not initialized: {}".format(
            uninitialized_workers))
    if len(rolling_back_workers) > 0:
        print("The following workers are rollbacking: {}".format(
            rolling_back_workers))
    else:
        print("All workers are healthy")


def find_blocked_by(back_pressure_workers, worker):
    for key, workers in back_pressure_workers.items():
        if worker in workers:
            return key

    return None


# 检查反压节点，并打印反压在哪个下游上
# 按照有向图顺序找到最下游引起反压的worker
def check_backpressure_worker():
    back_pressure_workers = {}
    for key in data:
        worker = data[key]
        if "producer" not in worker:
            continue

        last_write_channel = worker["producer"]["last_write_q_id"]
        channels = worker["producer"]["channels"]
        for channel in channels:
            if channel["back_pressure_ratio"] == 1 and \
                    channel["channel_id"] == last_write_channel:
                peer = channel["peer_global_index"]
                if peer not in back_pressure_workers:
                    back_pressure_workers[peer] = []
                back_pressure_workers[peer].append(worker["global_index"])

    if back_pressure_workers == {}:
        print("No back pressure detected.")
        return

    for key in back_pressure_workers:
        print("The worker {} block {}".format(key, back_pressure_workers[key]))

    # 从任意一个block worker出发，向下查找
    block_worker = list(back_pressure_workers.keys())[0]
    while block_worker is not None:
        next_block_worker = find_blocked_by(back_pressure_workers,
                                            block_worker)
        print("{} blocked by {}".format(block_worker, next_block_worker))
        block_worker = next_block_worker


# 对给定的某个checkpoint id，输出cp失败最上游的worker
# 对于producer，检查其last_global_barrier_id，如果小于checkpoint_id，则说明没有向下游发barrier
# 对于consumer，检查其每个channel的barrier_id（收到channel上游worker的barrier即更新），如果小于checkpoint_id，
#   则说明没有收到上游barrier
def check_cp(checkpoint_id):
    print("Check cp {}".format(checkpoint_id))
    # 检查Source
    source_workers = []
    for key in data:
        worker = data[key]
        if "consumer" in worker:
            continue
        if worker["producer"]["last_global_barrier_id"] < checkpoint_id:
            source_workers.append(key)
    if (len(source_workers) > 0):
        print("The following source workers does broadcast barrier: {}".format(
            source_workers))
        return
    else:
        print("Source workers OK.")

    # 检查非source
    for worker in workers:
        if "consumer" not in worker:
            continue
        channels = worker["consumer"]["channels"]
        unreay_channels = []
        for channel in channels:
            if channel["barrier_id"] < checkpoint_id:
                upstream_index = channel["channel_id"].split("-")[0]
                unreay_channels.append(upstream_index)
        downstream_index = worker["global_index"]
        if (len(unreay_channels) > 0):
            print(
                "worker {} does not receive barrier form upstreams: {}".format(
                    downstream_index, unreay_channels))
        else:
            print("Non source workers OK.")


#
def check_channel(channel_id):
    up_worker_index = channel_id.split("-")[0]
    down_worker_index = channel_id.split("-")[1]

    up_worker = workers[up_worker_index]
    down_worker = workers[down_worker_index]

    up_channel = next(x for x in up_worker["producer"]["channels"]
                      if x["channel_id"] == channel_id)
    down_channel = next(x for x in down_worker["consumer"]["channels"]
                        if x["channel_id"] == channel_id)

    # print("upstream channel:\n{}".format(up_channel))
    # print("downstream channel:\n{}".format(down_channel))

    last_sent_message_id = up_channel["queue_info.last_sent_message_id"]
    last_pop_msg_id = down_channel["queue_info.last_pop_msg_id"]
    if last_sent_message_id == last_pop_msg_id:
        pass
    else:
        print(
            "last_sent_message_id: {} not equal to last_pop_msg_id: {}".format(
                last_sent_message_id, last_pop_msg_id))

    last_sent_seq_id = up_channel["queue_info.last_sent_seq_id"]
    last_pop_seq_id = down_channel["queue_info.last_pop_seq_id"]
    if last_sent_seq_id == last_pop_seq_id:
        pass
    else:
        print("last_sent_seq_id: {} not equal to last_pop_seq_id: {}".format(
            last_sent_seq_id, last_pop_seq_id))

    pending_count = down_channel["queue_info.pending_count"]
    if (pending_count != 0):
        print("downstream channel pending bundle count: {}".format(
            pending_count))

    current_message_id = down_channel["current_message_id"]
    if current_message_id < last_pop_msg_id:
        print("Some messages are ignored, current_message_id: {}, \
            last_pop_msg_id: {}".format(current_message_id, last_pop_msg_id))

    if "queue_info.pull_status" in up_channel \
            and "queue_info.pull_status" in down_channel:
        up_channel_pull_status = up_channel["queue_info.pull_status"]
        down_channel_pull_status = down_channel["queue_info.pull_status"]
        if up_channel_pull_status != down_channel_pull_status:
            print("pull status does not match. up_channel_pull_status: {}, \
                down_channel_pull_status: {}".format(up_channel_pull_status,
                                                     down_channel_pull_status))
