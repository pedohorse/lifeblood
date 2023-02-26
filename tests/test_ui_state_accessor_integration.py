import asyncio
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
import logging
from lifeblood.logging import set_default_loglevel
from lifeblood.enums import TaskState
from lifeblood.exceptions import NotSubscribedError
from lifeblood.scheduler.scheduler import Scheduler
from lifeblood.taskspawn import NewTask
from lifeblood.ui_events import TaskFullState, TaskUpdated, TasksUpdated, TasksRemoved
from lifeblood.environment_resolver import EnvironmentResolverArguments


def purge_db(testdbpath):
    testdbpath = Path(testdbpath)
    if testdbpath.exists():
        testdbpath.unlink()


class EventQueueTest(IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        set_default_loglevel(logging.DEBUG)
        purge_db('test_uilog.db')

    @classmethod
    def tearDownClass(cls) -> None:
        purge_db('test_uilog.db')

    async def test_event_log_basics(self):
        purge_db('test_uilog.db')

        sched = Scheduler('test_uilog.db', do_broadcasting=False, helpers_minimal_idle_to_ensure=0)
        await sched.start()
        ui_state_access = sched.ui_state_access

        node_id = await sched.add_node('null', 'test_null')
        _, task1_id = await sched.spawn_tasks(NewTask('testtask1', node_id, task_attributes={'ass': 1}, extra_groups=['test_group1']))
        await asyncio.sleep(0.5)  # all task processing should be done

        self.assertEqual(0, ui_state_access.subscriptions_count())
        events = await ui_state_access.subscribe_to_task_events_for_groups(('test_group1',), True, 5.0)
        self.assertEqual(1, ui_state_access.subscriptions_count())
        self.assertEqual(1, len(events))
        full_state_event = events[0]
        self.assertIsInstance(full_state_event, TaskFullState)

        print(full_state_event)
        last_known_id = full_state_event.event_id

        # now change something
        await sched.set_task_name(task1_id, 'testtask1_newname')
        await asyncio.sleep(0.5)  # all task processing should be done

        events = await ui_state_access.get_events_for_groups_since_event_id(('test_group1',), True, last_known_id)
        self.assertEqual(1, len(events))
        self.assertIsInstance(events[0], TasksUpdated)
        event = events[0]
        last_known_id = event.event_id
        print(event)

        await asyncio.sleep(1.0)  # random slee
        # next change
        await sched.set_task_environment_resolver_arguments(task1_id, EnvironmentResolverArguments('blee', {'foo': 'bar'}))
        await sched.set_task_groups(task1_id, ('borkers',))
        await asyncio.sleep(0.5)  # all task processing should be done

        events = await ui_state_access.get_events_for_groups_since_event_id(('test_group1',), True, last_known_id)
        self.assertEqual(2, len(events))
        event0, event1 = events
        self.assertIsInstance(event0, TasksUpdated)
        self.assertIsInstance(event1, TasksRemoved)
        print(event0)
        print(event1)
        self.assertEqual(1, len(event0.task_data.tasks))
        self.assertIn(task1_id, event0.task_data.tasks)
        self.assertEqual(1, len(event1.task_ids))
        self.assertIn(task1_id, event1.task_ids)
        self.assertSetEqual({'test_group1', f'testtask1#{task1_id}'}, event0.task_data.tasks[task1_id].groups)
        last_known_id = event1.event_id

        await sched.set_task_name(task1_id, 'testtask1_newname11')
        await asyncio.sleep(0.5)  # all task processing should be done
        events = await ui_state_access.get_events_for_groups_since_event_id(('test_group1',), True, last_known_id)
        print(events)
        self.assertEqual(0, len(events))  # cuz we changed group name, so that change did not go to the log

        await sched.set_task_groups(task1_id, ('borkers', 'test_group1'))  # return group back
        await asyncio.sleep(0.5)  # all task processing should be done
        events = await ui_state_access.get_events_for_groups_since_event_id(('test_group1',), True, last_known_id)
        self.assertEqual(1, len(events))
        event0 = events[0]
        self.assertIsInstance(event0, TasksUpdated)
        self.assertEqual(1, len(event0.task_data.tasks))
        self.assertEqual('testtask1_newname11', event0.task_data.tasks[task1_id].name)
        self.assertSetEqual({'borkers', 'test_group1'}, event0.task_data.tasks[task1_id].groups)
        last_known_id = event0.event_id

        await asyncio.sleep(1.0)  # random slee
        events = await ui_state_access.get_events_for_groups_since_event_id(('test_group1',), True, last_known_id)
        self.assertEqual(0, len(events))

        self.assertEqual(1, ui_state_access.subscriptions_count())
        await asyncio.sleep(3)  # ensure subsctiption timeout is reached
        self.assertEqual(0, ui_state_access.subscriptions_count())
        was_raised = False
        try:
            await ui_state_access.get_events_for_groups_since_event_id(('test_group1',), True, last_known_id)
        except NotSubscribedError:
            was_raised = True
        self.assertTrue(was_raised)

        print('stage 2')
        #
        # next steps
        _, task2_id = await sched.spawn_tasks(NewTask('testtask2', node_id, task_attributes={'ass': 2.2}, extra_groups=['test_group1']))
        await asyncio.sleep(0.5)  # all task processing should be done

        _, task3_id = await sched.spawn_tasks(NewTask('testtask3', node_id, task_attributes={'ass': 3.33}, extra_groups=['test_group1']))
        await asyncio.sleep(0.5)  # all task processing should be done

        events = await ui_state_access.subscribe_to_task_events_for_groups(('test_group1',), True, 5.0)
        await asyncio.sleep(0.5)  # all task processing should be done
        self.assertEqual(1, ui_state_access.subscriptions_count())
        self.assertEqual(1, len(events))
        full_state_event = events[0]
        self.assertIsInstance(full_state_event, TaskFullState)

        print(full_state_event)
        self.assertSetEqual({task1_id, task2_id, task3_id}, set(x for x in full_state_event.task_data.tasks))
        last_known_id = full_state_event.event_id

        await sched.force_change_task_state([task1_id, task2_id, task3_id], TaskState.WAITING)
        await asyncio.sleep(0.5)  # all task processing should be done

        events = await ui_state_access.get_events_for_groups_since_event_id(('test_group1',), True, last_known_id)
        for event in events:
            print('!', event)
        self.assertEqual(3, len(events))
        _ids = set()
        for event in events:
            self.assertIsInstance(event, TasksUpdated)
            self.assertEqual(TaskState.WAITING, list(event.task_data.tasks.values())[0].state)
            _ids.update(event.task_data.tasks.keys())
        self.assertIn(task1_id, _ids)
        self.assertIn(task2_id, _ids)
        self.assertIn(task3_id, _ids)
        last_known_id = events[-1].event_id

        # unpause and ensure the whole processing iteration generated correct events
        print('unpaused')
        await sched.set_task_paused([task1_id, task2_id, task3_id], False)
        await asyncio.sleep(0.5)  # all task processing should be done

        events = await ui_state_access.get_events_for_groups_since_event_id(('test_group1',), True, last_known_id)
        self.assertEqual(4 + 3*2, len(events))  # note - bad check: this count is implementation-specific and may change without affecting anything
        # TODO: instead of event count - check that in total there are all tasks going over all 5 states
        check_states = {task1_id: (TaskState.WAITING, True),
                        task2_id: (TaskState.WAITING, True),
                        task3_id: (TaskState.WAITING, True)}
        # NOTE: currently event implementation is lazy:
        #  that means event is reported, but no data is queried at this point
        #  as data query is considered to be expensive operation
        #  instead, data query will happen later asynchronously when events are processed
        #  This is cuz we don't care THAT much about UI, as we care about fast processing
        #  Therefore tasks may jump states in the test below, and several update events may contain the same data
        #   (as they were queried together after task's state actually changed)
        for event in events:
            print(event.event_id)
            self.assertIsInstance(event, TasksUpdated)
            for task_id, task_data in event.task_data.tasks.items():
                print(task_data)
                self.assertIn(task_id, check_states)
                if check_states[task_id] == (TaskState.WAITING, True):
                    self.assertIn(task_data.state,
                                  (TaskState.WAITING, TaskState.GENERATING, TaskState.POST_WAITING, TaskState.POST_GENERATING, TaskState.DONE))
                elif check_states[task_id] == (TaskState.WAITING, False):
                    self.assertIn(task_data.state,
                                  (TaskState.WAITING, TaskState.GENERATING, TaskState.POST_WAITING, TaskState.POST_GENERATING, TaskState.DONE))
                elif check_states[task_id] == (TaskState.GENERATING, False):
                    self.assertIn(task_data.state,
                                  (TaskState.GENERATING, TaskState.POST_WAITING, TaskState.POST_GENERATING, TaskState.DONE))
                elif check_states[task_id] == (TaskState.POST_WAITING, False):
                    self.assertIn(task_data.state, (TaskState.POST_WAITING, TaskState.POST_GENERATING, TaskState.DONE))
                elif check_states[task_id] == (TaskState.POST_GENERATING, False):
                    self.assertIn(task_data.state, (TaskState.POST_GENERATING, TaskState.DONE))
                elif check_states[task_id] == (TaskState.DONE, False):
                    self.assertIn(task_data.state, (TaskState.DONE,))
                elif check_states[task_id] == (TaskState.DONE, True):
                    self.assertIn(task_data.state, (TaskState.DONE,))
                else:
                    print(check_states[task_id])
                    print(event)
                    self.fail('something wrong with events')

                if task_data.state == TaskState.DONE:
                    if check_states[task_id][1]:  # if already paused - cannot get unpaused
                        self.assertTrue(task_data.paused)
                else:
                    self.assertFalse(task_data.paused)
                check_states[task_id] = task_data.state, task_data.paused

        for state in check_states.values():
            self.assertEqual((TaskState.DONE, True), state)

        sched.stop()
        await sched.wait_till_stops()
