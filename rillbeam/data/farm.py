import os
import random
import attr


@attr.s
class FarmPayload(object):
    source = attr.ib()

    graphid = attr.ib()

    # {jobid: #tasks}
    jobtasks = attr.ib()

    jobid = attr.ib()
    taskid = attr.ib()

    output = attr.ib()


def gen_farm_messages(source=None, graphs=1, jobs=2, tasks=3, outputs=2):
    # example payload structure...
    # {
    #     'source': Any
    #     'graphid': 0,
    #     'jobtasks': {0: 3, 1: 3},
    #     'jobid': 0,
    #     'taskid': 2,
    #     'output': [
    #         '/tmp/graph-0/job-0_output-0.task-2.ext',
    #         '/tmp/graph-0/job-0_output-1.task-2.ext',
    #     ],
    # }
    data = []

    for graph in range(graphs):

        # get complete list of jobs and tasks
        jobtasks = {}
        for job in range(jobs):
            jobtasks[str(job)] = tasks

        for job in range(jobs):
            for task in range(tasks):
                # ['/tmp/0/job-0_output-0.task-0.ext',
                #  '/tmp/0/job-0_output-1.task-0.ext']
                output = []
                for i in range(outputs):
                    output.append(os.path.join(
                        os.sep,
                        'beam',
                        'graph-{}'.format(graph),
                        'job-{}_output-{}.task-{}.ext'.format(
                            job, i, task)
                    ))
                data.append(
                    FarmPayload(
                        source=source,
                        graphid=graph,
                        jobtasks=jobtasks,
                        jobid=job,
                        taskid=task,
                        output=output
                    ))

    # random yield order
    while data:
        # time.sleep(min(random.random() / random.random(), 2.0))
        index = random.randrange(len(data))
        payload = data[index]
        del data[index]
        yield attr.asdict(payload)
