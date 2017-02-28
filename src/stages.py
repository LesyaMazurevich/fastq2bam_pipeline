'''
Individual stages of the pipeline implemented as functions from
input files to output files.

The run_stage function knows everything about submitting jobs and, given
the state parameter, has full access to the state of the pipeline, such
as config, options, DRMAA and the logger.
'''

from pipeline_base.utils import safe_make_dir
from pipeline_base.runner import run_stage

import os
import sys
import util

class Stages(object):
    def __init__(self, state):
        self.state = state

    def get_stage_options(self, stage, *options):
        return self.state.config.get_stage_options(stage, *options)

    def get_options(self, *options):
        return self.state.config.get_options(*options)

    def original_files(self, output):
        '''Original files'''
        pass

    def fastq2bam(self, inputs, bam_out, sample):
        '''
          Convert fastq to bam. 
          stages:
          1 infer lanes and indexes
          2 split into lanes
          3 fastq2bam
          4 merge
          5 validate TODO
        '''

        # input filenames
        fastq_read1_in, fastq_read2_in = inputs
        output_dir = os.path.dirname(bam_out)
        log_out = os.path.join(output_dir, '{}.log.out'.format(bam_out))
        log_err = os.path.join(output_dir, '{}.log.err'.format(bam_out))

        command = "python /mnt/vicnode_nfs/code/fastq2bam.py --r1 {} --r2 {} --output_dir {} --bam {} 1>>{} 2>>{}".format(fastq_read1_in, fastq_read2_in, output_dir, bam_out, log_out, log_err)
        run_stage(self.state, 'fastq2bam', command)

    def align(self, inputs, bam_out):
        pass
