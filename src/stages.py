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
import re
import sys

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
          Convert fastq to a prealigned bam. 
          stages:
          1 infer lanes and indexes
          2 split into lanes
          3 fastq2bam
          4 merge
        '''

        # input filenames
        fastq_read1_in, fastq_read2_in = inputs
        output_dir = os.path.dirname(bam_out)

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        log_out = os.path.join(output_dir, '{}.log.out'.format(bam_out))
        log_err = os.path.join(output_dir, '{}.log.err'.format(bam_out))

        command = "python /mnt/vicnode_nfs/code/fastq2bam.py --r1 {} --r2 {} --output_dir {} --bam {} 1>>{} 2>>{}".format(fastq_read1_in, fastq_read2_in, output_dir, bam_out, log_out, log_err)
        run_stage(self.state, 'fastq2bam', command)

    def validate_prealigned_bam(self, input, validation_out):
        '''
            run validation script
            @input: the pre-aligned bam
            @validation_out: tsv file with validation details
        '''
        prefix = re.sub('.bam$', '', input)
        sample = re.sub('.bam$', '', os.path.basename(input))

        validation_in = '{}.validation_src'.format(prefix)
        # read in additional metadata
        found = False
        for line in open("/mnt/vicnode_nfs/code/sample-metadata.csv", 'r'):
            # Sample UUID,Patient UUID,Lab ID,tissue_id,is_normal
            fields = line.strip('\n').split(',')
            if fields[0] == sample:
                donor_id = fields[1]
                tissue_id = fields[3]
                is_normal = fields[4]
                found = True
                break

        if not found:
            raise Exception("Sample '{}' not found in metadata file".format(sample))

        # generate input to the validation script
        with open(validation_in, 'w') as validation_src:
            validation_src.write('#Donor_ID\tTissue_ID\tis_normal (Yes/No,Y/N)\tSample_ID\trelative_file_path\n')
            validation_src.write('{donor_id}\t{tissue_id}\t{is_normal}\t{sample_id}\t{sample}.bam\n'.format(
                donor_id=donor_id, 
                tissue_id=tissue_id, 
                is_normal=is_normal, 
                sample_id=sample, 
                sample=sample))

        # run the validation script and generate output
        command = ". /mnt/vicnode_nfs/code/profile; validate_sample_meta.pl -in {validation_in} -out {validation_out} -f tsv 1>>{prefix}.validation.out 2>>{prefix}.validation.err".format(validation_in=validation_in, validation_out=validation_out, prefix=prefix)
        run_stage(self.state, 'validate_prealigned_bam', command)

        # check that it worked - but run_stage doesn't block
        #lines = open(validation_out, 'r').readlines()
        #if len(lines) != 2:
        #    raise Exception('{} contained {} lines. Expected 2 lines.'.format(validation_out, len(lines)))
        #fields = lines[1].strip('\n').split('\t')
        #if len(fields) != 10:
        #    raise Exception('{} contained {} fields. Expected 10.'.format(validation_out, len(fields)))

    def align(self, inputs, bam_out):
        '''
          run the alignment dockstore image
          @input: the pre-aligned bam
          @bam_out: aligned bam
        '''
        # generate dockstore file as sample.dockstore
        validation, bam = inputs
        prefix = re.sub('.bam$', '', bam)
        dockstore_out = re.sub('.bam$', '.dockstore', bam)

        # determine sample from validation file
        for line in open(validation, 'r'):
            if line.startswith('#'):
                continue
            fields = line.strip('\n').split('\t')
            sample = fields[8]

        if input == dockstore_out:
            raise Exception("Unexpected input file {}".format(bam))

        log_out = '{}.log.out'.format(bam_out)
        log_err = '{}.log.err'.format(bam_out)

        # replace sample with our sample
        with open(dockstore_out, 'w') as dockstore_fh:
            for line in open('/mnt/vicnode_nfs/code/dockstore.template', 'r'):
                new_line = re.sub('PREFIX', prefix, line)
                new_line = re.sub('SAMPLE', sample, new_line)
                dockstore_fh.write(new_line)

        #command = '/mnt/vicnode_nfs/dockstore/dockstore tool launch --entry quay.io/wtsicgp/dockstore-cgpmap:1.0.6 --json {} 1>>{} 2>>{}'.format(dockstore_out, log_out, log_err)
        command = 'TMPDIR=/mnt/vicnode_nfs/dockstore-tmp /mnt/vicnode_nfs/dockstore/dockstore tool launch --entry quay.io/wtsicgp/dockstore-cgpmap:2.0.0 --json {} 1>>{} 2>>{}'.format(dockstore_out, log_out, log_err)
        run_stage(self.state, 'align', command)
