![image](images/confluent-logo-300-2.png)

# Overview

This repo contains the bits used for a search engineering workshop based on Apache Kafka, Kafka Connect and Kafka Streams (and might be KSQL?)

image::images/SearchEngineeringArchitecture.png[ArchitectureOfASearchEngineer]

## Prerequisites

Before embarking on the workshop you should follow the [pre requisites](pre-requisites.adoc). This document contains all necessary dependencies
and pieces of infrastructure necessary to successfully run or deliver this workshop.

## Getting use of this repository

### Interested to follow the workshop self paced

You had been part of a session of this workshop, or just discovered over the internet, yes, you can take this exercise alone, you will find a guide, including answers to the different scenarios presented here. With this guide you will be able to follow it alone without much problem.

----

**You can follow the [step-by-step guide](workshop-explain.adoc) workshop instructions here. These instructions are based on Docker**

----

This repository include as well the [code samples](app/SearchEngApp/) containing a proposing solution for the Kafka Streams part.


### Interested to teach the workshop

This workshop is usually delivered in sessions of 3h to 4h, but more time can be used so more in depth content and edge cases could be covered. A [timesheet](workshop-timesheet.md) proposal can be found as well within this repository.

The workshop setup is based on [Docker](pre-requisites.adoc) as it helps delivering uniform infrastructure for the students, you can adapt this to your needs.

Included in this repository there is as well an app [scaffold](app/SearchEngAppScaffold/) that could be used to let the workshop attendees time to develop their own Kafka Streams solutions.

## Questions

If you have questions, issues, contributions or a follow-up on this workshop materials, feel free contribute them back, all your thoughts are very welcome.

All contributions are welcome: ideas, patches, documentation, bug reports, complaints, etc!

Programming is not a required skill, and there are many ways to help out! It is more important to us that you are able to contribute.

## Thanks

This workshop could not has been develop without the help and contributions  by my coworkers at Confluent Inc, many thanks. Special mention to Robin Moffatt <[@rmoff](https://twitter.com/rmoff)> for providing a great baseline for this lab.
