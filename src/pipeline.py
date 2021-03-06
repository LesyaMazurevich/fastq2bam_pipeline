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
        filter=formatter('(?P<path>.+)/(?P<sample>[a-zA-Z0-9]+)_R1.fastq.gz'),
        add_inputs=add_inputs('{path[0]}/{sample[0]}_R2.fastq.gz'),
        extras=['{sample[0]}'],
        output='{path[0]}/out/{sample[0]}.bam')

    pipeline.transform(
        task_func=stages.validate_prealigned_bam,
        name='validate_prealigned_bam',
        input=output_from('fastq2bam'),
        filter=formatter('(?P<path>.+)/(?P<sample>[a-zA-Z0-9]+).bam'),
        output='{path[0]}/{sample[0]}.validation')

    pipeline.transform(
        task_func=stages.align,
        name='align',
        input=output_from('validate_prealigned_bam'),
        filter=formatter('(?P<path>.+)/(?P<sample>[a-zA-Z0-9]+).validation'),
        add_inputs=add_inputs('{path[0]}/{sample[0]}.bam'),
        output='{path[0]}/{sample[0]}.mapped.bam')

    return pipeline
