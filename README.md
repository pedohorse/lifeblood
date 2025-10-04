[Documentation](https://pedohorse.github.io/lifeblood)

| dev                                                                                                                                                                         |
|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [![Tests](https://github.com/pedohorse/lifeblood/actions/workflows/tests.yml/badge.svg?branch=dev)](https://github.com/pedohorse/lifeblood/actions/workflows/python-app.yml)|

![](icon/lifeblood.svg)

1. [Overview](#lifeblood)
2. [Features](#features)
3. [Installation](#installation)

## Lifeblood
Lifeblood is a generic execution task management and workflow creation software.

In Lifeblood you define with a node graph how tasks need to be processed.

In terms of CG industry Lifeblood can be simplified to be a render farm manager, but actually it's much more than that,  
It is a universal task automation tool.

Rendering, simulating, caching, generating previews, publishing, reporting, converting, 
copying, processing - all those are tasks that can be automated and connected using visual and somewhat intuitive node graph.

This [series of posts](https://www.patreon.com/posts/different-4-82326359) show 
how Lifeblood can be used not just like a renderfarm manager, but can run all of studio's pipeline.

Currently, it aims at smaller teams, farms or even individual setups, but with scaling always in mind.

watch the teaser (youtube)

[<img src="https://img.youtube.com/vi/ZYDpVIwP4Og/maxresdefault.jpg" width="50%">](https://youtu.be/ZYDpVIwP4Og)

Lifeblood consists of **scheduler**, **workers** and **viewers**

a scheduler manages multiple workers, giving them tasks to complete.  
GUI tool is used to check on the state of scheduler and tasks and modify task
processing graph.

This system overview should be very familiar to anyone who interacted with at least one renderfarm.

check [documentation here](https://pedohorse.github.io/lifeblood)  
and also [video tutorials](https://pedohorse.github.io/lifeblood/tutorials.html)

### Features:
- instantly and easily deployable
- easy scaling in mind
- dynamic worker resources management:
  - multiple tasks may run on the same machine as long as it has free resources to run them
  - dynamic GPU management: different tasks may run on different GPUs of the same machine
  - arbitrary resources: want to define your own resources? it's easy!
- support for environment wrappers (allow you to integrate with existing packaging systems like rez)

### Features To Be Done:
- arbitrary token counting (for license management for example)
- rez environment resolver implementation
  
### Even Further Future Features
- easy cloud deployment
- be integrateable (to some extent) with existing farm managers

## Installation

There are multiple ways to get Lifeblood to try it out, refer to [installation section in the docs](https://pedohorse.github.io/lifeblood/installation.html#simplest-lifeblood-manager)

In short - easiest way is to use [Lifeblood-Manager](https://github.com/pedohorse/lifeblood-manager/releases), as described in the docs

### DCC tools and submitters

Lifeblood-Manager can install houdini tools automatically.

For manual installation - sumbitters for Houdini and Blender are available in [releases](https://github.com/pedohorse/lifeblood/releases).

## Links

- [blog + support](https://www.patreon.com/xapkohheh)
- [telegram support/discussion group](https://t.me/+v9klFGZcKVY2MjYy)
- [documentation](https://pedohorse.github.io/lifeblood)
