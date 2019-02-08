import os
import json
import time
import codecs
import subprocess
import numpy as np
from time import sleep

# CLUSTER_PATH = ""

# TODO
# Adjust the starting path where the commands are going to be run.
# (adding a variable that specifies the trace generation root folder
# should be enough..)


# TODO!
# Append a mapping from the current signature to the tuned knobs
# to some specific filestore...
# Maybe there should be a database service running on Ercilla to
# hold these information...


TGS_FOLDER = "~/Spearmint-Traces/"
MIN_DURATION = 180


class KnobTuner:
    def __init__(self, trace_duration=600, cluster_name="hsc0"):
        """
        Trace duration is given in seconds.
        """
        self.trace_duration = trace_duration
        self.cluster_name = cluster_name

        if trace_duration < MIN_DURATION:
            raise Exception("Trace duration must be less than 180 seconds")

    def launch_config(self, job_id, knobs):
        # Create a signature that corresponds to the current configuration.
        self.job_id = job_id
        self.signature = self.cluster_name + "_" + str(int(time.time()))
        input_rate = int(knobs['inputRate'])

        # Read the job related parameters for the spark submit command
        self.read_job_args()
        self.read_cluster_params()
        ss_job_args = self.job_args

        if 'jar:NumReceivers' in ss_job_args:
            num_receivers = int(ss_job_args['jar:NumReceivers'])
            sender_input_rate = int(input_rate / num_receivers)
        elif 'jar:NumReceiversTrain' in ss_job_args and 'jar:NumReceiversTest' in ss_job_args:
            # TODO: Add support for different values of input rate for train and test data
            # (for workloads with 2 streaming datasets...)
            num_receivers_train = int(ss_job_args['jar:NumReceiversTrain'])
            sender_input_rate = int(input_rate / num_receivers_train)
        else:
            sender_input_rate = int(input_rate)

        # 0) Tear down old running stuffs on cluster (if any already exist)
        kill_senders_cmd = self.get_cmd_kill_senders()

        # 1) Start senders
        start_senders_cmd = self.get_cmd_start_senders(sender_input_rate)

        # 2) Start nmon on worker nodes
        nmon_start_cmd = self.get_cmd_start_nmon()

        # 2') Start monitoring yarn
        monitor_yarn_cmd = self.get_cmd_monitor_yarn()

        # 3) Prepare and run the spark-submit program
        ss_command, ss_command_full = self.get_cmd_spark_submit(knobs)

        # 4) Timer
        timer_cmd = "sleep %d" % self.trace_duration

        # 5) Kill all java processes on driver + sender + workers, and kill nmon
        kill_driver_cmd = self.get_cmd_kill_driver(ss_job_args)

        # Kill senders:
        kill_senders_cmd = self.get_cmd_kill_senders()

        # -1) Clean output written to HDFS
        clean_hdfs_cmd = self.get_cmd_clean_hdfs()

        # Sleep more to make sure that nmon has stopped
        sleep_more_cmd = "sleep 20"

        # 7) Move traces to archive
        move_to_archive_cmds = self.get_cmd_move_logs_to_archive(
            ss_command_full)

        trace_cmds = [
            clean_hdfs_cmd, start_senders_cmd, nmon_start_cmd,
            monitor_yarn_cmd, ss_command, timer_cmd, kill_driver_cmd,
            kill_senders_cmd,
            sleep_more_cmd, move_to_archive_cmds]
        trace_cmds = "\n".join(trace_cmds)

        with open("commands.sh", "w") as f:
            f.write(trace_cmds)

        make_executable_cmd = "chmod +x commands.sh"
        subprocess.call(make_executable_cmd, shell=True)

        os.system("nohup bash commands.sh  1>log_%s.txt 2>&1 &" %
                  self.signature)
        sleep(self.trace_duration+60)  # sleep additional time to make sure
        # trace has been inserted

        while True:
            try:
                # TODO
                # ...
                # ...
                break
            except:
                pass

        # TODO parse latency and throughput of the current trace.

    def parse_latency_and_throughput(self):
        trace_dir = "%s/job_%s/%s" % (self.cluster_params['archiveDir'],
                                      self.job_id,
                                      self.signature)
        # TODO!
        pass

    def read_cluster_params(self):
        # Read the cluster related parameters
        cconfig_fname = os.path.join(
            TGS_FOLDER, "config/cluster/%s.json" % self.cluster_name)
        with codecs.open(cconfig_fname, "r", 'utf-8-sig') as f:
            cluster_params = json.load(f)
        self.cluster_params = cluster_params

    def read_job_args(self):
        # Read the job related parameters for the spark submit command
        wconfig_fname = os.path.join(
            TGS_FOLDER, "config/workloads/%s.json" % self.job_id)
        with codecs.open(wconfig_fname, "r", 'utf-8-sig') as f:
            self.job_args = json.load(f)

    def get_cmd_kill_senders(self):
        job_id = self.job_id
        job_args = self.job_args
        cluster_params = self.cluster_params

        num_senders = int(job_args['senders:numSenders'])
        java_bin_path = cluster_params['javaBin']

        sender_nodes_fp = cluster_params['senderNodestxtPath']
        sender_jar_prefix = cluster_params['sendersJarPrefix']

        kill_senders_cmds = []
        for i in range(1, num_senders + 1):
            sender_host = job_args['sender%d:host' % i]
            if sender_host[:5] == "hscx:":
                sender_host = cluster_params[sender_host[5:]]
            kill_sender = "jps | grep %s | cut -d ' ' -f 1| xargs -n1 kill -9" % sender_jar_prefix
            ssh_kill_sender = 'ssh %s "%s"' % (sender_host, kill_sender)
            kill_senders_cmds.append(ssh_kill_sender)
        kill_senders_cmds = "\n".join(kill_senders_cmds)
        return kill_senders_cmds

    def get_cmd_start_senders(self, senderInputRate):
        # senderInputRate is inputRate at which each sender source will pump in the data
        job_args = self.job_args
        cluster_params = self.cluster_params

        num_senders = int(job_args['senders:numSenders'])
        java_bin_path = cluster_params['javaBin']

        sender_nodes_fp = cluster_params['senderNodestxtPath']
        sender_jar_prefix = cluster_params['sendersJarPrefix']

        start_senders_cmds = []
        for i in range(1, num_senders + 1):
            jar_path = os.path.join(
                cluster_params['sendersJarDir'],
                sender_jar_prefix + job_args['sender%d:jarSuffix' % i])
            data_path = job_args['sender%d:dataPath' % i]
            sender_host = job_args['sender%d:host' % i]
            if sender_host[:5] == "hscx:":
                sender_host = cluster_params[sender_host[5:]]
            sender_port = job_args['sender%d:port' % i]
            ssh_sender = 'ssh %s "%s -jar %s %s %d %d" & 1>/dev/null 2>/dev/null'
            start_senders_cmds.append(ssh_sender % (sender_host, java_bin_path,
                                                    jar_path, data_path,
                                                    sender_port,
                                                    senderInputRate))
        start_senders_cmds = '\n'.join(start_senders_cmds)
        return start_senders_cmds

    def get_cmd_start_nmon(self):
        signature = self.signature
        cluster_params = self.cluster_params

        nmon_bin = cluster_params['nmonBin']
        nmon_log_dir = cluster_params['nmonLogDir']
        worker_nodes = cluster_params['computeNodes']
        nmon_cmds = []
        for hostname in worker_nodes:
            nmon_filename = "%s_%s_%s.nmon" % (hostname, self.job_id,
                                               signature)
            nmon_start_cmd = "%s -s1 -c%d -F %s  -m %s" % (
                nmon_bin, self.trace_duration, nmon_filename, nmon_log_dir)
            ssh_start_nmon = 'ssh %s "%s"' % (hostname, nmon_start_cmd)
            nmon_cmds.append(ssh_start_nmon)
        nmon_cmds = "\n".join(nmon_cmds)
        return nmon_cmds

    def get_cmd_monitor_yarn(self):
        yarnlog_fileprefix = "%s_%s" % (self.job_id, self.signature)
        cmd = "bash monitor-yarn.sh %s %d &" % (
            yarnlog_fileprefix, self.trace_duration)
        return cmd

    def get_cmd_spark_submit(self, knobs):
        job_id = self.job_id
        cluster_params = self.cluster_params
        signature = self.signature

        # Read job parameters
        ss_job_args = self.job_args

        # Expand the job related parameters read by using cluster related
        # parameters

        ss_args = {}
        for key, value in ss_job_args.items():
            if (type(value) == str or type(value) == unicode) and value[:5] == "hscx:":
                key_cluster = value[5:]
                val = cluster_params[key_cluster]
                ss_args[key] = val
            else:
                ss_args[key] = value

        # Prepare the spark-submit command
        if ss_args['master'] == 'yarn':
            ss_command = "spark-submit --class %s \\\n" % ss_args['className'] \
                + "--master %s --deploy-mode %s \\\n" % (
                    ss_args['master'], ss_args['deployMode'])
        else:
            ss_command = "spark-submit --class %s \\\n" % ss_args['className'] \
                + "--master %s \\ \n" % ss_args['master']

        confs = []
        for knob, value in knobs.items():
            if knob[:5] == "spark":
                confs.append("--conf %s=%s \\\n" % (knob, value))
        if 'spark.metrics.conf' in cluster_params:
            confs.append("--conf spark.metrics.conf=%s \\\n" %
                         cluster_params['spark.metrics.conf'])
        ss_command += "".join(confs) + ss_args['jarPath'] + " \\\n"
        jar_args = []
        for knob, value in knobs.items():
            if knob[:5] != 'spark':
                jar_args.append("%s=%s \\\n" % (knob, value))
        for key, value in ss_args.items():
            if key[:4] == 'jar:':
                jar_args.append("%s=%s \\\n" % (key[4:], value))
            elif key == 'job_id':
                jar_args.append("job_id=%s \\\n" % value)
        jar_args.append("signature=%s \\\n" % str(signature))
        ss_command += "".join(jar_args)
        ss_command = ss_command[:-3] + " \n"

        if not os.path.exists("ss_commands/logs"):
            os.makedirs("ss_commands/logs")

        with open("ss_commands/ss_%s.sh" % signature, "w") as f:
            f.writelines(ss_command)

        make_exec_cmd = 'chmod +x ss_commands/ss_%s.sh' % signature
        subprocess.call(make_exec_cmd, shell=True)

        cmd = "bash ss_commands/ss_%s.sh 1>ss_commands/logs/log_%s.txt 2>&1 &" % (
            signature, signature)

        return cmd, ss_command

    def get_cmd_kill_driver(self, ss_job_args):
        # Kill the driver
        if ss_job_args['master'] != 'yarn':
            # Kill sparkSubmit (if the deploy mode on YARN is client mode, or if Spark Standalone mode is used)
            kill_driver_cmd = "ps -ef | grep SparkSubmit | grep -v grep | awk '{print $2}' | xargs -n1 kill -9"
        elif ss_job_args['master'] == 'yarn' and ss_job_args['deployMode'] == 'client':
            kill_driver_cmd = "ps -ef | grep SparkSubmit | grep -v grep | awk '{print $2}' | xargs -n1 kill -9;"
            kill_driver_cmd += "yarn application -list | grep %s | tr -s ' ' | cut -d $'\\t' -f 1 | xargs yarn application -kill" % self.cluster_name
        else:
            # Kill from yarn:
            kill_driver_cmd = "yarn application -list | grep %s | tr -s ' ' | cut -d $'\\t' -f 1 | xargs yarn application -kill" % self.cluster_name
        return kill_driver_cmd

    def get_cmd_clean_hdfs(self):
        cluster_params = self.cluster_params
        clean_hdfs_output = "hadoop fs -rm -r -skipTrash %s*" % cluster_params['outputPath']
        clean_hdfs_checkpointing = "hadoop fs -rm -r -skipTrash %s" % cluster_params['checkpointDir']

        clean_hdfs_cmd = clean_hdfs_output + "\n" + clean_hdfs_checkpointing
        return clean_hdfs_cmd

    def get_cmd_move_logs_to_archive(self, ss_command_full):
        job_id = self.job_id
        cluster_params = self.cluster_params
        signature = self.signature
        # TODO! Undo these lines
        if not os.path.exists("%s/%s/%s" %
                              (cluster_params['archiveDir'],
                               job_id, signature)):
            os.makedirs("%s/job_%s/%s" %
                        (cluster_params['archiveDir'], job_id, signature))
        move_to_archive_cmds = []
        mkdir_archive_spark = "mkdir -p %s/job_%s/%s/spark_logs/driver" % (
            cluster_params['archiveDir'], job_id, signature)
        mkdir_archive_nmon = "mkdir -p %s/job_%s/%s/nmon_logs/" % (
            cluster_params['archiveDir'], job_id, signature)
        mv_sparklog_driver = "mv %s/Job_%s_%s* %s/job_%s/%s/spark_logs/driver" % (
            cluster_params['sparkLogDir'], job_id, signature,
            cluster_params['archiveDir'], job_id, signature)
        monitor_yarn_filepath = os.path.join(
            TGS_FOLDER, "yarnlogs_%s_%s.tar.gz" % (job_id, signature))
        mv_yarnlog_driver = "mv %s %s/job_%s/%s/" % (
            monitor_yarn_filepath, cluster_params['archiveDir'],
            job_id, signature)

        move_to_archive_cmds += [mkdir_archive_spark,
                                 mkdir_archive_nmon, mv_sparklog_driver,
                                 mv_yarnlog_driver]
        worker_nodes = cluster_params['computeNodes']
        for hostname in worker_nodes:
            mkdir_spark_worker = "mkdir -p %s/job_%s/%s/spark_logs/worker_%s" % (
                cluster_params['archiveDir'], job_id, signature, hostname)
            scp_sparklog_worker = "scp %s:%s/Job_%s_%s* %s/job_%s/%s/spark_logs/worker_%s/" % (
                hostname, cluster_params['sparkLogDir'], job_id, signature, cluster_params['archiveDir'], job_id, signature, hostname)
            rm_sparklog_worker = 'ssh %s "rm %s/Job_%s_%s*"' % (
                hostname, cluster_params['sparkLogDir'], job_id, signature)
            scp_nmonlog_worker = "scp %s:%s/%s_%s_%s.nmon %s/job_%s/%s/nmon_logs/" % (
                hostname, cluster_params['nmonLogDir'], hostname, job_id, signature, cluster_params['archiveDir'], job_id, signature)
            rm_nmonlog_worker = 'ssh %s "rm %s/%s_%s_%s.nmon"' % (
                hostname, cluster_params['nmonLogDir'], hostname, job_id, signature)
            move_to_archive_cmds += [mkdir_spark_worker, scp_sparklog_worker,
                                     rm_sparklog_worker, scp_nmonlog_worker, rm_nmonlog_worker]
        move_to_archive_cmds = "\n".join(move_to_archive_cmds)

        # Adds README file that describes the configuration that yielded these traces.
        readme_txt = "The traces under this repository have been obtained with the\
        following spark-submit command: \n %s" % ss_command_full
        with open("%s/job_%s/%s/README.txt" % (cluster_params['archiveDir'], job_id, signature), "w") as readme_f:
            readme_f.writelines(readme_txt)

        return move_to_archive_cmds
