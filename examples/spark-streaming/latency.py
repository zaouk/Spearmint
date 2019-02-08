import sys
import math
import numpy as np
import traceback

# TODO!
# At some point we need to make sure that we don't re-evaluate a previously
# seen configuration...


def parse_knobs(params):
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

                 'spark.shuffle.sort.bypassMergeThreshold',
                 'spark.shuffle.compress']:
        knobs[knob] = params[knob][0]
    knobs['spark.reducer.maxSizeInFlight'] = \
        str(params['spark.reducer.maxSizeInFlight'][0]) + 'm'
    return knobs


def get_latency(params):
    sys.stderr.write('[get_latency has been called]\n')
    return 1000 * (1 + np.random.rand())


def evaluate(job_id, params):
    knobs = parse_knobs(params)

    print(knobs)

    if knobs['spark.streaming.blockInterval'] > 1000*knobs['batchInterval']:
        return np.nan

    latency = float(get_latency(params))

    # Batch Interval (ms) >= Block Interval
    con1 = float(knobs['batchInterval'] * 1000 -
                 knobs['spark.streaming.blockInterval'])

    return {
        "latency": latency,
        "block_interval_less_than_batch_interval": con1
    }


def main(job_id, params):
    try:
        return evaluate(job_id, params)
    except Exception as ex:
        print ex
        print 'An error occurred in latency.py'
        return np.nan
