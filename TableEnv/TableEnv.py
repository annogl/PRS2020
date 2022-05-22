from pyflink.table import EnvironmentSettings, TableEnvironment

# Simple new env
batch_settings = EnvironmentSettings.in_batch_mode()
batch_tbl_env = TableEnvironment.create(batch_settings)

# With builder
stream_settings = EnvironmentSettings.new_instance()\
                                      .in_streaming_mode()\
                                      .build()
stream_tbl_env = TableEnvironment.create(stream_settings)


