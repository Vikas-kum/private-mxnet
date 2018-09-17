#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Launch a distributed job
"""
import argparse
import os, sys
import signal
import logging

curr_path = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.join(curr_path, "../3rdparty/dmlc-core/tracker"))

def dmlc_opts(opts):
    """convert from mxnet's opts to dmlc's opts
    """
    args = ['--num-workers', str(opts.num_workers),
            '--num-servers', str(opts.num_servers),
            '--cluster', opts.launcher,
            '--host-file', opts.hostfile,
            '--sync-dst-dir', opts.sync_dst_dir]
    if opts.launch_worker is True:
        args.append('--launch-worker')
        args.append(str(opts.launch_worker))
    if opts.elastic_training_enabled is True:
        args.append('--elastic-training-enabled')
        args.append(str(opts.elastic_training_enabled))

    # convert to dictionary
    dopts = vars(opts)
    for key in ['env_server', 'env_worker', 'env']:
        for v in dopts[key]:
            args.append('--' + key.replace("_","-"))
            args.append(v)
    if dopts['elastic_training_enabled'] is True:
        args.append('--mxnet-launch-script-path')
        args.append(os.path.abspath(__file__))
        args.append('--worker-host-file')
        args.append(dopts['hostfile'] + "_worker")
        args.append('--instance-pool')
        args.append(dopts['instance_pool'])
        args.append('--max-elastic-instances')
        args.append(str(dopts['max_elastic_instances']))

    if dopts['launch_worker'] is True:    
        #args.append('--launch-worker ' + True)
        args.append('--host')
        args.append(dopts['host'])
        args.append('--port')
        args.append(dopts['port'])
        
    
    args += opts.command
    logging.info("HAHAHAH 123333 %s", args)
    try:
        from dmlc_tracker import opts
    except ImportError:
        print("Can't load dmlc_tracker package.  Perhaps you need to run")
        print("    git submodule update --init --recursive")
        raise
    dmlc_opts = opts.get_opts(args)
    logging.info("HAHAHAH %s", dmlc_opts)

    return dmlc_opts


def main():
    parser = argparse.ArgumentParser(description='Launch a distributed job')
    parser.add_argument('-n', '--num-workers', required=True, type=int,
                        help = 'number of worker nodes to be launched')
    parser.add_argument('-s', '--num-servers', type=int,
                        help = 'number of server nodes to be launched, \
                        in default it is equal to NUM_WORKERS')
    parser.add_argument('-H', '--hostfile', type=str,
                        help = 'the hostfile of slave machines which will run \
                        the job. Required for ssh and mpi launcher')
    parser.add_argument('--sync-dst-dir', type=str,
                        help = 'if specificed, it will sync the current \
                        directory into slave machines\'s SYNC_DST_DIR if ssh \
                        launcher is used')
    parser.add_argument('--launcher', type=str, default='ssh',
                        choices = ['local', 'ssh', 'mpi', 'sge', 'yarn'],
                        help = 'the launcher to use')
    parser.add_argument('--env-server', action='append', default=[],
                        help = 'Given a pair of environment_variable:value, sets this value of \
                        environment variable for the server processes. This overrides values of \
                        those environment variable on the machine where this script is run from. \
                        Example OMP_NUM_THREADS:3')
    parser.add_argument('--env-worker', action='append', default=[],
                        help = 'Given a pair of environment_variable:value, sets this value of \
                        environment variable for the worker processes. This overrides values of \
                        those environment variable on the machine where this script is run from. \
                        Example OMP_NUM_THREADS:3')
    parser.add_argument('--env', action='append', default=[],
                        help = 'given a environment variable, passes their \
                        values from current system to all workers and servers. \
                        Not necessary when launcher is local as in that case \
                        all environment variables which are set are copied.')
    parser.add_argument('--elastic-training-enabled', type=bool, default=False,
                        help = ' if this option is set to true, elastic training is enabled. \
                        If True, you should specify which instance pool to use by using option \
                        --instance-pool')
    parser.add_argument('--instance-pool', type=str, default='DEFAULT', help=' You can use '
                        ' [reservedInstancePoolId | \'spotInstance\', | \'DEFAULT\']' \
                        'In case of DEFAULT a file will be created in same folder ' 
                        ' where --hostfile lives. The default worker filename will be \'default_worker_file\'')
    parser.add_argument('--max-elastic-instances', type=int, default=0,help = ' if instance pool is reserved' \
                        ' or spotInstance, up to max-elastic-instances can be added to existing cluster')
    parser.add_argument('--launch-worker', type=bool, default=False, help = 'whether this script should' \
                        'only launch worker instances')    
    parser.add_argument('--host', type=str, help='host name or ip of new worker host to launch')
    parser.add_argument('--port', type=str, default='22', help='port number of new worker for ssh command to run by')           
    parser.add_argument('command', nargs='+',
                        help = 'command for launching the program')
    # TODO verify if elastic training enabled is true
    # verify that --instance-pool is defined , 
    # if --instance-pool is [reserved|spot], verify that --max-elastic-instances is defined
    # if --instance-pool is DEFAULT then , max_elastic_instance is not defined
    # launch-worker is true, verify we have host

    args, unknown = parser.parse_known_args()
  #  if args.hostfile is not None: 

    args.command += unknown
    
    logging.info("BEGING %s", args)

    if args.num_servers is None:
        args.num_servers = args.num_workers

    args = dmlc_opts(args)
    
    logging.info("JAHHAHA%s", args)
    if args.host_file is None or args.host_file == 'None':
      if args.cluster == 'yarn':
          from dmlc_tracker import yarn
          yarn.submit(args)
      elif args.cluster == 'local':
          from dmlc_tracker import local
          local.submit(args)
      elif args.cluster == 'sge':
          from dmlc_tracker import sge
          sge.submit(args)
      elif args.cluster == 'ssh' and args.launch_worker is True:
          from dmlc_tracker import ssh
          logging.info("Vikas dmlc_tracker ssh %s", args)
          ssh.submit(args)
      else:
          raise RuntimeError('Unknown submission cluster type %s' % args.cluster)
    else:
      if args.cluster == 'ssh':
          from dmlc_tracker import ssh
          logging.info("Vikas dmlc_tracker ssh %s", args)
          ssh.submit(args)
      elif args.cluster == 'mpi':
          from dmlc_tracker import mpi
          mpi.submit(args)
      else:
          raise RuntimeError('Unknown submission cluster type %s' % args.cluster)

def signal_handler(signal, frame):
    logging.info('Stop launcher')
    sys.exit(0)

if __name__ == '__main__':
    fmt = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(format=fmt, level=logging.INFO)
    signal.signal(signal.SIGINT, signal_handler)
    main()
