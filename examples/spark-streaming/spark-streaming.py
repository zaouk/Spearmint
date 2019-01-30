import math
import numpy as np
import traceback

# TODO!
# First before starting to evaluate the different values of the parameters,
# we need to make sure that our GP model has been trained on all previously
# seen knobs.

# TODO!
# At some point we need to make sure that we don't re-evaluate a previously
# seen configuration...


def get_params(params):
    # 'inputRate': array([660000]),
    #  u'exec_properties': [u'2_2_4'],
    #  u'spark.default.parallelism': array([8]),
    #  u'spark.streaming.blockInterval': array([50]),
    #  u'batchInterval': array([ 1.]),
    #  u'spark.reducer.maxSizeInFlight': array([10]),
    #  u'spark.shuffle.sort.bypassMergeThreshold': array([5]),
    #  u'spark.shuffle.compress'

    input_rate = params['inputRate'][0]
    parallelism = params['spark.default.parallelism'][0]
    # Batch Interval in seconds
    batch_interval = params['batchInterval'][0]
    # Block Interval in milliseconds
    block_interval = params['spark.streaming.blockInterval'][0]
    max_size_in_flight = params['spark.reducer.maxSizeInFlight'][0]
    bypass_merge_threshold = params['spark.shuffle.sort.bypassMergeThreshold'][
        0]
    shuffle_compress = params['spark.shuffle.compress'][0]

    exec_properties = params['exec_properties'][0]

    return input_rate, parallelism, batch_interval, block_interval, \
        max_size_in_flight, bypass_merge_threshold, shuffle_compress


def get_knobs(params):
    knobs = {}

    exec_properties = params['exec_properties'][0]
    e_instances, e_cores, e_mem = exec_properties.split('_')

    knobs['spark.executor.instances'] = int(e_instances)
    knobs['spark.executor.cores'] = int(e_cores)
    # Exec memory in MB, leaving 20% for overhead and os memory.
    knobs['spark.executor.memory'] = str(int(int(e_mem)*1024*0.8)) + 'm'

    for knob in ['inputRate',
                 'spark.default.parallelism',
                 'batchInterval',
                 'spark.streaming.blockInterval',
                 'spark.reducer.maxSizeInFlight',
                 'spark.shuffle.sort.bypassMergeThreshold',
                 'spark.shuffle.compress']:
        knobs[knob] = params[knob][0]

    return knobs


def print_evaluation_point(params):
    pass


def get_latency(params):
    sys.stderr.write('[get_latency has been called]\n')
    return 1000 * (1 + np.random.rand())


def evaluate(job_id, params):
    print("-----------TRACEBACK------------")
    for line in traceback.format_stack():
        print(line.strip())
    print("--------------------------------")

    input_rate, parallelism, batch_interval, block_interval, \
        max_size_in_flight, bypass_merge_threshold, shuffle_compress \
        = get_params(params)
    # if block_interval > 1000*batch_interval:
    # return np.nan

    knobs = get_knobs(params)
    print(knobs)

    latency = float(get_latency(params))
    print(latency)

    # Batch Interval (ms) >= Block Interval
    con1 = float(batch_interval*1000 - block_interval)

    return {
        "latency": latency,
        "block_interval_less_than_batch_interval": con1
    }


def main(job_id, params):
    # try:
    return evaluate(job_id, params)
    # except Exception as ex:
    #     print ex
    #     print 'An error occurred in hello_world.py'
    #     return np.nan
