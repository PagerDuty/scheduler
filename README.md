# Scheduler [![CircleCI](https://circleci.com/gh/PagerDuty/scheduler.svg?style=svg)](https://circleci.com/gh/PagerDuty/scheduler)
## Introduction

This library schedules arbitrary Scala code (tasks) to run at an arbitrary time. It tries to do this despite changing infrastructure, and it tries to maintain some sense of task ordering.

Scheduler is in production use at PagerDuty. It's meant to be included in Scala services deployed on many servers for redundancy and scaling. It's designed from the ground up to be running in a multi-datacenter environment. We take fault tolerance and reliability very seriously, so using this library is a little more complicated then just including it via SBT.

Scheduler relies on Kafka for task buffering and Cassandra for task persistence. Most documentation is
in the specific guides in doc/.

## Additional Documentation
- [Design](doc/design.md)
- [User Guide](doc/user.md)
- [Operations Guide](doc/operations.md)
- [Contributing Guide](doc/contributing.md)

## Contact

This library is primarily maintained by the Core Team at PagerDuty. The best way to reach us is by opening a GitHub issue.
