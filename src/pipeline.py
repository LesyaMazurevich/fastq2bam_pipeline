'''
Build the pipeline workflow by plumbing the stages together.
'''

from ruffus import Pipeline, suffix, formatter, add_inputs, output_from
from stages import Stages

def make_pipeline(state):
    '''Build the pipeline by constructing stages and connecting them together'''
    # Build an empty pipeline
    pipeline = Pipeline(name='fastq2bam')
    # Get a list of paths to all the FASTQ files
    input_files = state.config.get_option('files')
    # Stages are dependent on the state
    stages = Stages(state)

    # The original files
    # This is a dummy stage. It is useful because it makes a node in the
    # pipeline graph, and gives the pipeline an obvious starting point.
    pipeline.originate(
        task_func=stages.original_files,
        name='original_files',
        output=input_files)

    pipeline.transform(
        task_func=stages.fastq2bam,
        name='fastq2bam',
        input=output_from('original_files'),

        # format 1 looks like sample_sampletype_R1
        filter=formatter('(?P<path>.+)/(?P<sample>[a-zA-Z0-9_]+)_R1.fastq.gz'),
        add_inputs=add_inputs('{path[0]}/{sample[0]}_R2.fastq.gz'),
        extras=['{sample[0]}'],
        output='{path[0]}/{sample[0]}.bam')

        # format 2 looks like samplesampletype_flowcell_barcode_lane_R1
        #filter=formatter('.+/(?P<sample>[a-zA-Z0-9]+)_(?P<flowcell>[a-zA-Z0-9]+)_(?P<barcode>[a-zA-Z0-9]+)_L(?P<lane>[0-9]+)_R1.fastq.gz'),
        #add_inputs=add_inputs('{path[0]}/{sample[0]}_{flowcell[0]}_{barcode[0]}_L{lane[0]}_R2.fastq.gz'),
        #extras=['{sample[0]}_{flowcell[0]}_{barcode[0]}_{lane[0]}'], # key for reading rg from config

        #output='{path[0]}/{sample[0]}_{flowcell[0]}_{barcode[0]}_L{lane[0]}.bam')

    #pipeline.merge(
    #    task_func=stages.merge_bam,
    #    name='merge_bam',
    #    input=output_from('fastq2bam'),
    #    output='_merged')

    return pipeline
