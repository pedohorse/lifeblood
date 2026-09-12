
def gpu_device_env_common_code():
    return (
           'import os\n'
           'from lifeblood_connection import get_provided_devices\n'
           '\n'
           'def _do_gpu_related_env_setup():\n'
           '    provided_devices = get_provided_devices()\n'
           '    if "{gpu_dev_type}" not in provided_devices:\n'
           '        return\n'
           '    enabled_karma_devices_raw = {{dev_tags["karma_dev"] for _, dev_tags in get_provided_devices().get("{gpu_dev_type}", {{}}).items() if "karma_dev" in dev_tags}}\n'
           '    opencl_devices_raw = [dev_tags["houdini_ocl"] for _, dev_tags in get_provided_devices().get("{gpu_dev_type}", {{}}).items() if "houdini_ocl" in dev_tags]\n'
           '    enabled_karma_devices = set()\n'
           '    optix_device_count = 0\n'
           '    try:\n'
           '        for x in enabled_karma_devices_raw:\n'
           '            if "/" in x:\n'
           '                x, e = x.split("/")\n'
           '                optix_device_count = max(int(x)+1, optix_device_count, int(e))\n'
           '            enabled_karma_devices.add(int(x))\n'
           '    except ValueError:\n'
           '        print("karma_dev tag of gpu device must be an integer! disabling all gpu")\n'
           '\n'
           '    if len(opencl_devices_raw) == 0 or ":" not in opencl_devices_raw[0]:\n'
           '        os.environ["HOUDINI_OCL_DEVICETYPE"] = "CPU"\n'
           '        print("no GPU opencl devices enabled")\n'
           '    else:\n'
           '        ocl_dev_type, ocl_dev_vendor = opencl_devices_raw[0].split(":", 1)\n'
           '        ocl_dev_num = None\n'
           '        if ":" in ocl_dev_vendor:\n'
           '            ocl_dev_vendor, ocl_dev_num = ocl_dev_vendor.rsplit(":", 1)\n'
           '        print(f"ocl device type \'{{ocl_dev_type}}\' num \'{{ocl_dev_num}}\' vendor \'{{ocl_dev_vendor}}\' is enabled")\n'
           '        os.environ["HOUDINI_OCL_DEVICETYPE"] = ocl_dev_type\n'
           '        if ocl_dev_vendor:\n'
           '            os.environ["HOUDINI_OCL_VENDOR"] = ocl_dev_vendor\n'
           '        if ocl_dev_num:\n'
           '            os.environ["HOUDINI_OCL_DEVICENUMBER"] = ocl_dev_num\n'
           '\n'
           '    if len(enabled_karma_devices) == 0:\n'
           '        os.environ["KARMA_XPU_DISABLE_OPTIX_DEVICE"] = "1"\n'
           '        print("no karma xpu optix devices enabled")\n'
           '    for i in range(optix_device_count):\n'
           '        if i in enabled_karma_devices:\n'
           '            print(f"karma xpu optix device {{i}} enabled")\n'
           '            continue\n'
           '        os.environ[f"KARMA_XPU_DISABLE_DEVICE_{{i}}"] = "1"\n'
           '\n'
           '_do_gpu_related_env_setup()\n'
           '\n').format(
                gpu_dev_type="gpu",
           )

def checkpoint_init_functions_code(do_checkpoint: bool):
    if do_checkpoint:
        return (
            '__checkpoint_frames = set()\n'
            'def _checkpoint_frame(frame):\n'
            "    print('task checkpointing frame', frame)\n"
            '    __checkpoint_frames.add(frame)\n'
            '    try:\n'
            "        with open(checkpoint_path, 'w') as f:\n"
            "            json.dump({'frames': list(__checkpoint_frames), 'checksum': __checkpoint_checksum}, f)\n"
            "    except OSError as e:\n"
            "        print('!!WARNING!! Task checkpointing failed!', e)\n"

            'def _is_frame_checkpointed(frame):\n'
            '    return frame in __checkpoint_frames\n'
        )
    else:
        return (
            'def _checkpoint_frame(frame):\n'
            '    pass\n'
            'def _is_frame_checkpointed(frame):\n'
            '    return False\n'
        )

def checkpoint_init_code(checkpoint_file_path: str):
    return (
        f'checkpoint_path = {checkpoint_file_path}\n'

    # and init __checkpoint_frames
        'if os.path.exists(checkpoint_path):\n'
        "    print('task checkpoint file found', checkpoint_path)\n"
        '    try:\n'
        "        with open(checkpoint_path, 'r') as f:\n"
        "            _d = json.load(f)\n"
        "        if __checkpoint_checksum != _d['checksum']:\n"
        "            print('task checkpoint has different checksum, discarding')\n"
        "            os.unlink(checkpoint_path)\n"
        "        else:\n"
        "            __checkpoint_frames = set(_d['frames'])\n"
        "            print('task checkpoint: frames already done:', sorted(__checkpoint_frames))\n"
        '    except OSError as e:\n'
        "        print('!!WARNING!! Task checkpoint read error!', e)\n"
        "    except json.JSONDecodeError as e:\n"
        "        print('!!WARNING!! Task checkpoint integrity error!', e)\n"
        "    except KeyError as e:\n"
        "        print('!!WARNING!! Task checkpoint unexpected data error', e)\n"
        "    except TypeError as e:\n"
        "        print('!!WARNING!! Task checkpoint unexpected data error', e)\n"
    )

def checkpoint_cleanup_code(checkpoint_file_path: str):
    """
    TODO: pass just one path, not two parts when path mapping between OSes is fixed
    """
    return (
        f'checkpoint_path = {checkpoint_file_path}\n'
        f'if exit_code == 0:\n'
        f'    try:\n'
        f"        print('deleting task checkpoint', checkpoint_path)\n"
        f'        os.unlink(checkpoint_path)\n'
        f'    except Exception as e:\n'
        f'        print("unexpected error deleting checkpoint file", e)\n'
        f'else:\n'
        f"    print('keeping checkpoint file', checkpoint_path)\n"
    )
