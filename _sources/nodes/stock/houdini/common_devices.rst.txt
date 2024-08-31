:gpu:

    Treated as GPU OpenCL device

    tags used:

    - ``opencl_dev`` - has format ``<device type>:<device vendor>:<device number>``

      - ``<device type>`` - value identifying this device through houdini's ``HOUDINI_OCL_DEVICETYPE`` variable
      - ``<device vendor>`` - value identifying this device through houdini's ``HOUDINI_OCL_VENDOR`` variable
      - ``<device number>`` - value identifying this device through houdini's ``HOUDINI_OCL_DEVICENUMBER`` variable

      Each value can be empty, in that case corresponding houdini's environment variable is not set, it's up to the user to
      configure this value correctly.

      Example values: ``"GPU::0"``, ``"GPU:Intel(R) Corporation"``, ``GPU:NVIDIA Corporation:0``
