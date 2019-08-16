from __future__ import absolute_import, print_function

from apache_beam.runners.portability.job_server import ExternalJobServer
from apache_beam.portability.api import beam_job_api_pb2

def get_jobs():
  client = ExternalJobServer('localhost:8099').start()

  response = client.GetJobs(beam_job_api_pb2.GetJobsRequest())
  for job in response.job_info:
    print(job)
    pipe_response = client.GetPipeline(
      beam_job_api_pb2.GetJobPipelineRequest(job_id=job.job_id))
    # print(pipe_response.pipeline)


if __name__ == '__main__':
  get_jobs()
