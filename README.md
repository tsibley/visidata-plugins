# trs' VisiData plugins

I clone this repo into _~/.visidata/plugins/_ and then put the following line
in my _~/.visidatarc_:

    import plugins

This loads the following:

  - vdaws — Exposes sheets for AWS services accessed via `aws://` URLs.
    Currently only supports `aws://batch` to provide a listing of AWS Batch
    jobs across queues and statuses (which is more than the AWS management
    console provides!).

  - vds3 — [AJ Kerrigan's vds3 plugin](https://github.com/ajkerrigan/visidata-plugins)
    which provides excellent support for s3:// URLs.

The vds3 plugin is vendored using [git subrepo](https://github.com/ingydotnet/git-subrepo)
so that I can easily bring in updates or make my own tweaks.
