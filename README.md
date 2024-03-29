[Documentation](https://pedohorse.github.io/lifeblood)

| dev                                                                                                                                                                         |
|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [![Tests](https://github.com/pedohorse/lifeblood/actions/workflows/tests.yml/badge.svg?branch=dev)](https://github.com/pedohorse/lifeblood/actions/workflows/python-app.yml)|

![](icon/lifeblood.svg)

## Lifeblood
Lifeblood is a task/job management software package  
currently it aims for smaller teams, farms or even individual setups, but with scaling  always in mind.

In terms of CG industry it's a mini render farm manager, but actually it's more than that,  
It is a universal task automation tool.  
Rendering, simulating, caching - are all just tasks that can be automated.

it consists of **scheduler**, **workers** and **viewers**

a scheduler manages multiple workers, giving them tasks to complete.  
GUI tool is used to check on the state of scheduler and tasks and modify task
processing graph.

This system overview should be very familiar to anyone who saw at least one renderfarm.

check [documentation here](https://pedohorse.github.io/lifeblood)  
and also [video tutorials](https://pedohorse.github.io/lifeblood/tutorials.html)

## UNDER DEVELOPMENT

### Features:
- instantly and easily deployable
- easy scaling in mind
- dynamic slots, worker resources management
- support for environment wrappers (allow you to integrate with existing packaging systems like rez)

### Features To Be Done:
- arbitrary resource requirements
- arbitrary token counting (for license management for example)
- worker capabilities (automatic versioned worker "groups")
- rez environment resolver
- cgroups environment resolver
  
### Even Further Future Features
- easy cloud deployment
- be integrateable (to some extent) with existing farm managers

## Installation

There are multiple ways to get Lifeblood to try it out, refer to [installation section in the docs](https://pedohorse.github.io/lifeblood/installation.html#simplest-lifeblood-manager)

In short - easiest way is to use [Lifeblood-Manager](https://github.com/pedohorse/lifeblood-manager/releases), as described in the docs

## Links

- [blog + support](https://www.patreon.com/xapkohheh)
- [telegram support/discussion group](https://t.me/+v9klFGZcKVY2MjYy)
- [documentation](https://pedohorse.github.io/lifeblood)
