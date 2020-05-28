import visidata
from datetime import datetime
from visidata import date as vddate

__version__ = '2020.05.28'


def openurl_aws(url, filetype):
    assert url.given.startswith("aws://")
    path = url.given.replace("aws://", "", 1).rstrip("/")

    if path in {"batch", "batch/jobs"}:
        return AWSBatchJobsSheet("aws-batch-jobs", source = url)

    # XXX TODO support aws://batch/jobs/<job>
    # XXX TODO support aws://batch/queues
    # XXX TODO support aws://batch/queues/<queue>

    raise ValueError("Unsupported AWS resource: %s" % url.given)


class AWSBatchJobsSheet(visidata.Sheet):
    """
    Sheet with one row per AWS Batch job, across all statuses and queues.
    """
    rowtype = "jobs" # rowdef: Job
    columns = [
        visidata.ColumnAttr("id"),
        visidata.ColumnAttr("name"),
        visidata.ColumnAttr("queue"),
        visidata.ColumnAttr("status"),
        visidata.ColumnAttr("status_reason"),
        visidata.ColumnAttr("created", type=vddate),
        visidata.ColumnAttr("started", type=vddate),
        visidata.ColumnAttr("stopped", type=vddate),
        visidata.ColumnAttr("runtime"),
        visidata.SubColumnAttr("runtime", visidata.ColumnAttr("seconds", type=int), name="runtime_seconds"),
        visidata.ColumnAttr("image"),
        visidata.ColumnAttr("cmd"),
        visidata.ColumnAttr("definition"),
        visidata.ColumnAttr("cpus", type=int),
        visidata.ColumnAttr("memory_mib", type=int),
    ]

    nKeys = 1

    STATUSES = [
        "SUBMITTED",
        "PENDING",
        "RUNNABLE",
        "STARTING",
        "RUNNING",
        "SUCCEEDED",
        "FAILED",
    ]

    def __init__(self, name, source):
        super().__init__(name=name, source=source)

        # Late import to avoid requiring boto3 if this sheet/plugin is never used.
        import boto3
        self.client = boto3.client("batch")

    @visidata.asyncthread
    def reload(self):
        self.rows = []

        list_queues = self.client.get_paginator("describe_job_queues").paginate

        queues = sorted([
            queue["jobQueueName"]
                for page in list_queues()
                for queue in page["jobQueues"] ])

        loading_threads = [
            self._load_jobs(queue, status)
                for queue in queues
                for status in self.STATUSES ]

        # Wait for all jobs to load before sorting
        visidata.vd.sync(*loading_threads)

        # Sort by the existing order, if any, otherwise add an ordering.
        if self._ordering:
            self.sort()
        else:
            self.orderBy(
                self.column("created"),
                self.column("started"),
                reverse = True)

    @visidata.asyncthread
    def _load_jobs(self, queue, status):
        list_jobs = self.client.get_paginator("list_jobs").paginate

        # Page size of 100 because that's the limit of describe_jobs(),
        # which is called inside this loop.
        for page in list_jobs(jobQueue = queue, jobStatus = status, PaginationConfig = {"MaxItems": 100}):
            job_ids = [ job["jobId"] for job in page["jobSummaryList"] ]

            if job_ids:
                for job in self.client.describe_jobs(jobs = job_ids)["jobs"]:
                    self.rows.append(Job(job))


class Job:
    """
    A data class describing a single AWS Batch job, used for each row of an
    :py:class:`AWSBatchJobsSheet`.
    """
    def __init__(self, source):
        self.id = source["jobId"]
        self.name = source["jobName"]
        self.queue = arn_name(source["jobQueue"])
        self.status = source["status"]
        self.created = timestamp(source.get("createdAt"))
        self.started = timestamp(source.get("startedAt"))
        self.stopped = timestamp(source.get("stoppedAt"))
        self.image = source["container"]["image"]
        self.cmd = " ".join(source["container"]["command"])
        self.definition = arn_name(source["jobDefinition"])
        self.cpus = source["container"]["vcpus"]
        self.memory_mib = source["container"]["memory"]
        self.__source = source

    @property
    def status_reason(self):
        reason = self.__source.get("statusReason")
        container_reason = self.__source.get("container", {}).get("reason")
        exit_code = self.__source.get("container", {}).get("exitCode")

        # Make the default/normal reason more informative
        if reason == "Essential container in task exited":
            if exit_code is not None:
                reason = "exited %d" % exit_code
            else:
                reason = "exited"

        if reason and container_reason:
            return "%s, %s" % (container_reason, reason)
        else:
            return reason or container_reason

    @property
    def runtime(self):
        if self.started:
            return (self.stopped or datetime.now().replace(microsecond=0)) - self.started
        else:
            return None


def timestamp(value):
    return vddate(value // 1000) if value is not None else None


def arn_name(arn):
    return arn.split(":", 5)[-1].split("/", 1)[-1]


visidata.addGlobals({
    "openurl_aws": openurl_aws,
    "AWSBatchJobsSheet": AWSBatchJobsSheet,
})
