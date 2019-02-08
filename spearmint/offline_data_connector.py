import numpy as np
import pandas as pd

# shortcuts
MSF = 'spark.reducer.maxSizeInFlight'
BMT = 'spark.shuffle.sort.bypassMergeThreshold'
EP = 'exec_properties'
SC = 'spark.shuffle.compress'
EM = 'spark.executor.memory'
EI = 'spark.executor.instances'
EC = 'spark.executor.cores'

OBJECTIVE = 'driver.StreamingMetrics.streaming.lastCompletedBatch_totalDelay.avg'
OBJECTIVE2 = 'driver.StreamingMetrics.streaming.lastCompletedBatch_processingDelay.avg'
OBJECTIVE3 = 'driver.FeatureEngineering.MeanThroughput'

# knobs that we're currently tuning (their original name)
ALL_KNOBS = ["batchInterval",
             "spark.streaming.blockInterval",
             "spark.default.parallelism",
             "inputRate",
             "spark.executor.instances",
             "spark.executor.cores",
             "spark.executor.memory",
             "spark.reducer.maxSizeInFlight",
             "spark.shuffle.sort.bypassMergeThreshold",
             "spark.shuffle.compress"]

# Knobs names after transforming spark.executor.instances'_'spark.executor.cores'_'spark.executor.memory'
TRANSFORMED_KNOBS = ["batchInterval",
                     "spark.streaming.blockInterval",
                     "spark.default.parallelism",
                     "inputRate",
                     "spark.reducer.maxSizeInFlight",
                     "spark.shuffle.sort.bypassMergeThreshold",
                     "spark.shuffle.compress",
                     "exec_properties"]

# Dict for Categorizing Executor properties choices
EXEC_PROP_TO_CAT = {"2_2_4": 0,
                    "3_2_4": 1,
                    "2_4_8": 2,
                    "4_2_4": 3,
                    "3_4_8": 4,
                    "4_3_6": 5,
                    "6_2_4": 6,
                    "4_4_8": 7,
                    "8_2_4": 8,
                    "6_4_8": 9,
                    "8_3_6": 10,
                    "12_2_4": 11,
                    "8_4_8": 12,
                    "16_2_4": 13,
                    "18_4_8": 14,
                    "24_3_6": 15,
                    "36_2_4": 16}
# Dict for Categorizing "spark.shuffle.compress"
SC_TO_CAT = {True: 0,
             False: 1}


class OfflineDataConnector:
    def __init__(self, trace_path, constraints=None):
        # TODO:
        # Add constraints that should be something like:
        # {'metric_name': {'min': ..., 'max': ...}}

        # Reading traces for the current Spark Streaming workload.
        df = pd.read_csv(trace_path)

        # Removing units in maxSizeInFlight and Exec's memory
        df[MSF] = list(map(lambda val: int(val[:-1]), df[MSF]))
        df[EM] = list(map(lambda val: int(val[:-1]), df[EM]))

        # TODO
        # filter the rows that violate some constraints

        # Transforming back the value of exec memory to GB (by adding the subtracted overhead)
        df[EM] = list(map(lambda val: int(round(val/(0.8*1024))), df[EM]))

        # Transforming the 3 columns: 'spark.executor.instances'_'spark.executor.cores'_'spark.executor.memory'
        # into a single column
        df[EP] = list(map(lambda inst, cor, mem: "{}_{}_{}".format(
            inst, cor, mem), df[EI], df[EC], df[EM]))

        # Vectorizing categorical variables the way Spearmint categorizes them
        df[EP] = list(map(lambda col: np.eye(len(EXEC_PROP_TO_CAT))
                          [EXEC_PROP_TO_CAT[col]], df[EP]))
        df[SC] = list(map(lambda col: np.eye(
            len(SC_TO_CAT))[SC_TO_CAT[col]], df[SC]))

        # inspect array size:
        first_row = df[TRANSFORMED_KNOBS].values[0, :]
        dim_input = len(first_row)
        for el in first_row:
            if type(el) != int:
                dim_input += len(el) - 1

        # Now let's flatten inside every single row:
        arr = np.zeros([len(df), dim_input])
        is_break = False
        for i, row in enumerate(df[TRANSFORMED_KNOBS].values):
            j = 0
            for ar_el in row:
                if type(ar_el) == list or type(ar_el) == np.ndarray:
                    for k in range(len(ar_el)):
                        try:
                            arr[i, j] = ar_el[k]
                            j += 1
                        except:
                            print(i, j, k)
                            print(ar_el)
                            print(row)
                            print(arr[i, :])
                            is_break = True
                            break
                else:
                    arr[i, j] = ar_el
                    j += 1
                if is_break:
                    break

        # Filtering the values where the objective is positive.
        obj_values = df[OBJECTIVE].values
        idxs_pos = np.where(obj_values > 0)[0]

        self.inputs_ = arr[idxs_pos]
        self.values_ = {'latency': obj_values[idxs_pos],
                        'block_interval_less_than_batch_interval':
                        arr[idxs_pos, 0] * 1000 - arr[idxs_pos, 1]}
