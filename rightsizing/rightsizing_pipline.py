import kfp
from kfp import dsl
from kfp import onprem
from kubernetes import client

@dsl.pipeline(
    name='Rightsizing pipeline',
    description='A pipeline to run vm/pod rightsizing.'
)

def rightsizing_pipeline():
    dsl.get_pipeline_conf().set_image_pull_secrets([client.V1LocalObjectReference(name='okestroaiops')])
    vop = dsl.PipelineVolume(pvc='kfp-pvc')
    rightsizing_op = dsl.ContainerOp(
        name='Rightsizing',
        image='okestroaiops/base:latest',
        command=['sh', '-c'],
        arguments=['echo "10.178.0.17 symphony.dev.okestro.cld\n10.178.0.17 symphony.api.dev.okestro.cld" >> /etc/hosts\necho "start"\npython3 /croffle/pipelines/rightsizing/rightsizing.py']
    ).set_cpu_limit("2000m").set_memory_limit("4000Mi").set_cpu_request("2000m").set_memory_request("4000Mi").apply(onprem.mount_pvc('kfp-pvc',
                             volume_name='pvc-cb316f81-4d8b-4cf6-997e-929988ace962',
                             volume_mount_path='/tmp'))
    # dsl.get_pipeline_conf().set_ttl_seconds_after_finished(20)