import os
import re
import shutil
import tempfile
from pathlib import Path
from lifeblood_testing_common.nodes_common import TestCaseBase


class FFMPEGTestCase(TestCaseBase):
    async def test_movie_to_sequence(self):
        await self._helper_test_movie_to_sequence(
            'movie7.mov',  # that number in the end is interpreted as num of frames by ffmpeg mockers
            expected_frames=[100, 101, 102, 103, 104, 105, 106]
        )

    async def test_movie_to_sequence_several_streams(self):
        await self._helper_test_movie_to_sequence(
            'movie_several_streams9-11.mov',  # 9-11 means 2 streams, first 9, second 11 frames
            expected_frames=[100, 101, 102, 103, 104, 105, 106, 107, 108]
        )

    async def _helper_test_movie_to_sequence(self, fake_filename: str, *, expected_frames: list):
        temp_dir = tempfile.mkdtemp("lifeblood_tests")
        try:
            updated_attrs = await self._helper_test_simple_invocation(
                'ffmpeg',
                [
                    {'operation_mode': 1},  # movie_to_sequence mode
                    {
                        'in_movie': fake_filename,
                        'out_sequence': os.path.join(temp_dir, 'seq', 'file.%04d.png'),
                        'start_frame': 100,
                        'do_out_attr_name': True,
                        'out_attr_name': 'images',
                    }
                ],
                {},
                add_relative_to_PATH=Path(__file__).parent / 'data' / 'mock_ffmpeg',
                commands_to_replace_with_py_mock=['ffmpeg', 'ffprobe'],
            )

            # we expect sequence
            files = list(os.listdir(os.path.join(temp_dir, 'seq')))
            frames = [int(re.match(r'file.(\d+).png', f).group(1)) for f in files]
            ff = sorted(zip(frames, files), key=lambda x: x[0])
            frames, files = zip(*ff)

            self.assertEqual(len(expected_frames), len(updated_attrs['images']))
            self.assertEqual(len(expected_frames), len(files))
            self.assertEqual([os.path.join(temp_dir, 'seq', x) for x in files], updated_attrs['images'])
            self.assertListEqual(expected_frames, updated_attrs['frames'])
            self.assertListEqual(expected_frames, sorted(frames))

        finally:
            shutil.rmtree(temp_dir)
