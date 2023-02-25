import asyncio
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
import logging
from lifeblood.logging import set_default_loglevel
from lifeblood.exceptions import NotSubscribedError
from lifeblood.scheduler.scheduler import Scheduler
from lifeblood.taskspawn import NewTask
from lifeblood.ui_events import TaskFullState, TaskUpdated, TasksUpdated, TasksDeleted
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

        events = await ui_state_access.subscribe_to_task_events_for_groups(('test_group1',), True, 5.0)
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
        self.assertIsInstance(event1, TasksUpdated)
        print(event0)
        print(event1)
        self.assertEqual(1, len(event0.task_data.tasks))
        self.assertIn(task1_id, event0.task_data.tasks)
        self.assertEqual(1, len(event1.task_data.tasks))
        self.assertIn(task1_id, event1.task_data.tasks)
        self.assertSetEqual({'test_group1', f'testtask1#{task1_id}'}, event0.task_data.tasks[task1_id].groups)
        self.assertSetEqual({'borkers'}, event1.task_data.tasks[task1_id].groups)
        last_known_id = event1.event_id

        await asyncio.sleep(1.0)  # random slee
        events = await ui_state_access.get_events_for_groups_since_event_id(('test_group1',), True, last_known_id)
        self.assertEqual(0, len(events))

        await asyncio.sleep(3)  # ensure subsctiption timeout is reached
        was_raised = False
        try:
            await ui_state_access.get_events_for_groups_since_event_id(('test_group1',), True, last_known_id)
        except NotSubscribedError:
            was_raised = True
        self.assertTrue(was_raised)

        sched.stop()
        await sched.wait_till_stops()
